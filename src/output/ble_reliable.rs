// /src/output/ble_reliable.rs
// Module: output.ble_reliable
// Purpose: BLE GATT server output using reliable stream protocol (binary frames)
//          with sliding window acknowledgment.
//
// Uses our custom GattServer (ble_gatt.rs) which supports Write callbacks,
// replacing the ble-windows-server crate that only supports Read + Notify.
//
// Characteristics (per PDF spec):
// - Catalog     (0x90ae): Read   - Available signal catalog
// - Data_IN     (0x90ac): Write  - ACK frames from client
// - Data_OUT    (0x90ad): Notify - Data frames to client
// - Subscribe   (0x90af): Write  - Subscribe to signal
// - Control     (0x90b0): Notify - Session control / subscribe confirmations
// - Unsubscribe (0x90b1): Write  - Unsubscribe from signal

use crate::domain::ble_protocol::{AckFrame, Catalog, SignalId};
use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use crate::output::ble_gatt::{CharProperty, GattServer, WriteEvent};
use crate::output::ble_session::BleSessionState;
use std::sync::Arc;
use tokio::sync::RwLock;

/// ID SRS: SRS-MOD-BLERELIABLE-001
/// Title: ReliableBleOutput
///
/// Description: VRConnect shall provide BLE GATT server output using reliable
/// binary protocol with sliding window acknowledgment.
/// Write callbacks are handled via async mpsc channel from our custom GattServer.
///
/// Version: V5.0
pub struct ReliableBleOutput {
    server: Arc<RwLock<GattServer>>,
    state: Arc<RwLock<BleSessionState>>,
    catalog: Catalog,
}

/// Characteristic UUID suffixes from PDF specification
const CATALOG_UUID_SUFFIX: &str = "90ae";
const DATA_IN_UUID_SUFFIX: &str = "90ac";
const DATA_OUT_UUID_SUFFIX: &str = "90ad";
const SUBSCRIBE_UUID_SUFFIX: &str = "90af";
const CONTROL_UUID_SUFFIX: &str = "90b0";
const UNSUBSCRIBE_UUID_SUFFIX: &str = "90b1";

impl ReliableBleOutput {
    /// ID SRS: SRS-FN-BLERELIABLE-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a ReliableBleOutput instance with device
    /// name, service UUID, and setup the 6 standard characteristics from PDF spec.
    ///
    /// Version: V5.0
    pub async fn new(
        device_name: String,
        service_uuid_str: String,
        _update_interval_ms: u64,
    ) -> Result<Self> {
        // Parse service UUID
        let service_uuid = uuid::Uuid::parse_str(&service_uuid_str)
            .map_err(|e| VitalError::Config(format!("Invalid BLE service UUID: {}", e)))?;

        // Extract base for building characteristic UUIDs
        let base_uuid = service_uuid_str.trim().replace('-', "").to_lowercase();

        // Build the 6 characteristic UUIDs from base + suffix
        let catalog_uuid = Self::build_char_uuid(&base_uuid, CATALOG_UUID_SUFFIX)?;
        let data_in_uuid = Self::build_char_uuid(&base_uuid, DATA_IN_UUID_SUFFIX)?;
        let data_out_uuid = Self::build_char_uuid(&base_uuid, DATA_OUT_UUID_SUFFIX)?;
        let subscribe_uuid = Self::build_char_uuid(&base_uuid, SUBSCRIBE_UUID_SUFFIX)?;
        let control_uuid = Self::build_char_uuid(&base_uuid, CONTROL_UUID_SUFFIX)?;
        let unsubscribe_uuid = Self::build_char_uuid(&base_uuid, UNSUBSCRIBE_UUID_SUFFIX)?;

        log::info!("Reliable BLE Output Configuration:");
        log::info!("  Device Name: {}", device_name);
        log::info!("  Service UUID: {}", service_uuid);
        log::info!("  Update Interval: {}ms", _update_interval_ms);

        // Create our custom GATT server (with Write support)
        let mut server = GattServer::new(device_name, service_uuid);

        // Catalog: Read-only, contains available signal streams
        server.add_characteristic("Catalog", catalog_uuid, &[CharProperty::Read]);
        log::info!("  Characteristic: Catalog (Read)      -> {}", catalog_uuid);

        // Data_IN: Write, client sends ACK frames here
        server.add_characteristic(
            "Data_IN",
            data_in_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );
        log::info!("  Characteristic: Data_IN (Write)     -> {}", data_in_uuid);

        // Data_OUT: Notify, server sends data frames
        server.add_characteristic("Data_OUT", data_out_uuid, &[CharProperty::Notify]);
        log::info!(
            "  Characteristic: Data_OUT (Notify)   -> {}",
            data_out_uuid
        );

        // Subscribe: Write, client subscribes to signals
        server.add_characteristic(
            "Subscribe",
            subscribe_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );
        log::info!(
            "  Characteristic: Subscribe (Write)   -> {}",
            subscribe_uuid
        );

        // Control: Notify, session control events / subscribe confirmations
        server.add_characteristic("Control", control_uuid, &[CharProperty::Notify]);
        log::info!(
            "  Characteristic: Control (Notify)    -> {}",
            control_uuid
        );

        // Unsubscribe: Write, client unsubscribes from signals
        server.add_characteristic(
            "Unsubscribe",
            unsubscribe_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );
        log::info!(
            "  Characteristic: Unsubscribe (Write) -> {}",
            unsubscribe_uuid
        );

        // Create default medical catalog with HR, SpO2, Temperature
        let catalog = Catalog::default_medical_catalog();

        // Initialize session state with session ID 1
        let state = BleSessionState::new(1);

        Ok(Self {
            server: Arc::new(RwLock::new(server)),
            state: Arc::new(RwLock::new(state)),
            catalog,
        })
    }

    /// Build a characteristic UUID from base UUID (32 hex chars) and suffix (4 hex chars).
    fn build_char_uuid(base_uuid: &str, suffix: &str) -> Result<uuid::Uuid> {
        let uuid_str = if base_uuid.len() >= 32 {
            // Replace last 4 hex chars with the suffix
            let uuid_without_suffix = &base_uuid[..base_uuid.len() - 4];
            format!("{}{}", uuid_without_suffix, suffix)
        } else {
            format!("{}{}", base_uuid, suffix)
        };

        // Reformat to standard UUID string with dashes
        let formatted = if uuid_str.len() == 32 {
            format!(
                "{}-{}-{}-{}-{}",
                &uuid_str[0..8],
                &uuid_str[8..12],
                &uuid_str[12..16],
                &uuid_str[16..20],
                &uuid_str[20..32]
            )
        } else {
            uuid_str
        };

        uuid::Uuid::parse_str(&formatted)
            .map_err(|e| VitalError::Config(format!("Invalid UUID: {}", e)))
    }

    /// Serialize catalog to binary format per PDF spec.
    /// Format: [ID(2b) | NameLen(1b) | Name | Type(1b) | Period(4b)] per entry.
    fn serialize_catalog(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        for entry in &self.catalog.entries {
            // Signal ID (2 bytes, little-endian)
            bytes.extend_from_slice(&entry.id.to_le_bytes());
            // Name length (1 byte) + name bytes
            let name_bytes = entry.name.as_bytes();
            bytes.push(name_bytes.len() as u8);
            bytes.extend_from_slice(name_bytes);
            // Stream type (1 byte): 0=Num, 1=Wav
            let type_byte = match entry.stream_type {
                crate::domain::ble_protocol::StreamType::Num => 0u8,
                crate::domain::ble_protocol::StreamType::Wav => 1u8,
            };
            bytes.push(type_byte);
            // Period in ms (4 bytes, little-endian)
            bytes.extend_from_slice(&entry.period_ms.to_le_bytes());
        }

        bytes
    }

    /// ID SRS: SRS-FN-BLERELIABLE-002
    /// Title: start
    ///
    /// Description: VRConnect shall start BLE GATT server with reliable protocol:
    /// 1. Set Catalog read value
    /// 2. Take write receiver and spawn the write-handler task
    /// 3. Start the BLE server (creates Windows GATT service + advertises)
    ///
    /// Version: V5.0
    pub async fn start(&self) -> Result<()> {
        log::info!("Starting Reliable BLE GATT server...");

        // 1. Prepare Catalog payload and set it as static read value
        let catalog_bytes = self.serialize_catalog();
        log::info!("Catalog prepared ({} bytes, {} signals)", catalog_bytes.len(), self.catalog.entries.len());

        {
            let mut server = self.server.write().await;
            server.set_read_value("Catalog", catalog_bytes);
        }

        // 2. Take write receiver and spawn the write-handler task
        let write_rx = {
            let mut server = self.server.write().await;
            server.take_write_receiver()
        };

        if let Some(rx) = write_rx {
            let state = self.state.clone();
            let server = self.server.clone();

            tokio::spawn(async move {
                Self::write_handler_loop(rx, state, server).await;
            });

            log::info!("Write handler task started (Data_IN / Subscribe / Unsubscribe)");
        } else {
            log::warn!("Write receiver already taken - write handlers won't work");
        }

        // 3. Start the GATT server (creates service, registers handlers, advertises)
        {
            let mut server = self.server.write().await;
            server.start().await?;
        }

        log::info!("Reliable BLE GATT server started successfully");
        log::info!("Waiting for BLE client connections...");

        Ok(())
    }

    /// Background task: reads write events from the GATT server and dispatches them.
    async fn write_handler_loop(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WriteEvent>,
        state: Arc<RwLock<BleSessionState>>,
        server: Arc<RwLock<GattServer>>,
    ) {
        log::info!("Write handler loop running");

        while let Some(event) = rx.recv().await {
            match event.characteristic_name.as_str() {
                // ----- ACK frame from client -----
                "Data_IN" => {
                    match AckFrame::from_bytes(&event.data) {
                        Ok(ack) => {
                            log::debug!(
                                "ACK received: session={}, stream={}, ack_base={}",
                                ack.session_id,
                                ack.stream_id,
                                ack.ack_base
                            );

                            let retransmits = {
                                let mut st = state.write().await;
                                st.handle_ack(&ack)
                            };

                            // Retransmit missing frames immediately
                            if !retransmits.is_empty() {
                                log::info!(
                                    "Retransmitting {} missing frame(s)",
                                    retransmits.len()
                                );
                                let srv = server.read().await;
                                for frame in retransmits {
                                    match frame.to_bytes() {
                                        Ok(bytes) => {
                                            if let Err(e) = srv.notify("Data_OUT", &bytes).await {
                                                log::warn!(
                                                    "Retransmit failed for seq {}: {}",
                                                    frame.seq_num,
                                                    e
                                                );
                                            } else {
                                                log::debug!("Retransmitted seq {}", frame.seq_num);
                                            }
                                        }
                                        Err(e) => {
                                            log::error!("Failed to serialize retransmit: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("Invalid ACK frame ({} bytes): {}", event.data.len(), e);
                        }
                    }
                }

                // ----- Subscribe request from client -----
                "Subscribe" => {
                    if event.data.len() >= 2 {
                        let signal_id = u16::from_le_bytes([event.data[0], event.data[1]]);
                        let signal_name = SignalId::from_u16(signal_id)
                            .map(|s| s.name())
                            .unwrap_or("Unknown");

                        {
                            let mut st = state.write().await;
                            st.subscribe(signal_id);
                        }

                        log::info!(
                            "Client subscribed to signal {} ({})",
                            signal_id,
                            signal_name
                        );

                        // Send confirmation via Control characteristic
                        // Format: [signal_id(2b) | status(1b)] where 1 = subscribed
                        let mut confirm = Vec::new();
                        confirm.extend_from_slice(&signal_id.to_le_bytes());
                        confirm.push(0x01); // 1 = subscribed

                        let srv = server.read().await;
                        if let Err(e) = srv.notify("Control", &confirm).await {
                            log::warn!("Failed to send subscribe confirmation: {}", e);
                        }
                    } else {
                        log::warn!(
                            "Invalid Subscribe payload: expected >= 2 bytes, got {}",
                            event.data.len()
                        );
                    }
                }

                // ----- Unsubscribe request from client -----
                "Unsubscribe" => {
                    if event.data.len() >= 2 {
                        let signal_id = u16::from_le_bytes([event.data[0], event.data[1]]);
                        let signal_name = SignalId::from_u16(signal_id)
                            .map(|s| s.name())
                            .unwrap_or("Unknown");

                        {
                            let mut st = state.write().await;
                            st.unsubscribe(signal_id);
                        }

                        log::info!(
                            "Client unsubscribed from signal {} ({})",
                            signal_id,
                            signal_name
                        );

                        // Send confirmation via Control characteristic
                        // Format: [signal_id(2b) | status(1b)] where 0 = unsubscribed
                        let mut confirm = Vec::new();
                        confirm.extend_from_slice(&signal_id.to_le_bytes());
                        confirm.push(0x00); // 0 = unsubscribed

                        let srv = server.read().await;
                        if let Err(e) = srv.notify("Control", &confirm).await {
                            log::warn!("Failed to send unsubscribe confirmation: {}", e);
                        }
                    } else {
                        log::warn!(
                            "Invalid Unsubscribe payload: expected >= 2 bytes, got {}",
                            event.data.len()
                        );
                    }
                }

                other => {
                    log::warn!("Unexpected write on characteristic '{}'", other);
                }
            }
        }

        log::info!("Write handler loop ended");
    }

    /// Extract signal values (HR, SpO2, Temperature) from processed data.
    /// Returns Vec of (signal_id, value) for room index 0 only.
    #[cfg(test)]
    fn extract_signal_values(data: &ProcessedData) -> Vec<(u16, f32)> {
        use std::collections::HashMap;
        let mut values = Vec::new();

        let signal_map: HashMap<&str, u16> = [
            ("HR", SignalId::HR.as_u16()),
            ("SPO2", SignalId::SpO2.as_u16()),
            ("TEMP", SignalId::Temperature.as_u16()),
            ("TEMPERATURE", SignalId::Temperature.as_u16()),
        ]
        .into_iter()
        .collect();

        // Only process room 0 (first room / BED_01)
        if let Some(room) = data.rooms.iter().find(|r| r.room_index == 0) {
            for track in &room.tracks {
                let track_name_upper = track.name.to_uppercase();
                if let Some(&signal_id) = signal_map.get(track_name_upper.as_str()) {
                    if let Some(raw) = track.raw_value {
                        values.push((signal_id, raw as f32));
                    } else if let Ok(parsed) = track.display_value.parse::<f32>() {
                        values.push((signal_id, parsed));
                    }
                }
            }
        }

        values
    }

    /// ID SRS: SRS-FN-BLERELIABLE-003
    /// Title: output
    ///
    /// Description: VRConnect shall transmit live vital sign data via BLE.
    /// When vrconnect gets a new vital sign from Socket.IO: map it, frame it, send it.
    ///
    /// Version: V5.0
    pub async fn output(&self, data: &ProcessedData) -> Result<()> {
        let mut state = self.state.write().await;
        let server = self.server.read().await;

        for track in &data.all_tracks {
            // Only process BED_01 (room_index 0)
            if track.room_index != 0 {
                continue;
            }

            // Map string track name to PDF Catalog signal IDs
            let signal_id = match track.name.to_uppercase().as_str() {
                "HR" => 1,
                "SPO2" => 2,
                "TEMP" | "TEMPERATURE" => 3,
                _ => continue,
            };

            // Get value as f32
            if let Some(raw_val) = track.raw_value {
                let val_f32 = raw_val as f32;

                // Add to sliding window state (returns Some if subscribed)
                if let Some(frame) = state.add_data(signal_id, val_f32) {
                    // Send via Data_OUT Notify characteristic
                    match frame.to_bytes() {
                        Ok(bytes) => {
                            if let Err(e) = server.notify("Data_OUT", &bytes).await {
                                log::warn!("BLE notify failed for signal {}: {}", signal_id, e);
                            } else {
                                log::debug!(
                                    "Sent Data_OUT: signal={}, seq={}, session={}",
                                    signal_id,
                                    frame.seq_num,
                                    frame.session_id
                                );
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to serialize DataFrame: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-BLERELIABLE-004
    /// Title: handle_ack
    ///
    /// Description: VRConnect shall process an acknowledgment frame and retransmit
    /// any missing packets. (Also called internally by write_handler_loop)
    ///
    /// Version: V5.0
    pub async fn handle_ack(&self, ack: &AckFrame) -> Result<()> {
        let mut state = self.state.write().await;
        let retransmits = state.handle_ack(ack);

        let server = self.server.read().await;
        for frame in retransmits {
            match frame.to_bytes() {
                Ok(bytes) => {
                    if let Err(e) = server.notify("Data_OUT", &bytes).await {
                        log::warn!("Retransmit failed for seq {}: {}", frame.seq_num, e);
                    } else {
                        log::debug!("Retransmitted seq {}", frame.seq_num);
                    }
                }
                Err(e) => {
                    log::error!("Failed to serialize retransmit frame: {}", e);
                }
            }
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-BLERELIABLE-005
    /// Title: subscribe
    ///
    /// Description: VRConnect shall subscribe a client to a signal.
    /// (Also called internally by write_handler_loop when client writes to Subscribe char)
    ///
    /// Version: V5.0
    pub async fn subscribe(&self, signal_id: u16) {
        let mut state = self.state.write().await;
        state.subscribe(signal_id);
        log::info!("Subscribed to signal {}", signal_id);
    }

    /// ID SRS: SRS-FN-BLERELIABLE-006
    /// Title: unsubscribe
    ///
    /// Description: VRConnect shall unsubscribe a client from a signal.
    /// (Also called internally by write_handler_loop when client writes to Unsubscribe char)
    ///
    /// Version: V5.0
    pub async fn unsubscribe(&self, signal_id: u16) {
        let mut state = self.state.write().await;
        state.unsubscribe(signal_id);
        log::info!("Unsubscribed from signal {}", signal_id);
    }

    /// ID SRS: SRS-FN-BLERELIABLE-007
    /// Title: get_session_stats
    ///
    /// Description: VRConnect shall return current session statistics.
    ///
    /// Version: V5.0
    pub async fn get_session_stats(&self) -> (u16, u32, usize) {
        let state = self.state.read().await;
        (
            state.current_session_id,
            state.last_seq,
            state.get_pending_count(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ProcessedRoom, ProcessedTrack, TrackType};
    use chrono::Utc;

    /// Helper to create test track
    fn create_test_track(
        name: &str,
        value: f64,
        room_index: i32,
        room_name: &str,
    ) -> ProcessedTrack {
        ProcessedTrack {
            name: name.to_string(),
            display_value: format!("{:.1}", value),
            raw_value: Some(value),
            unit: "unit".to_string(),
            timestamp: Utc::now(),
            room_index,
            room_name: room_name.to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-001
    /// Title: Test UUID building
    #[test]
    fn test_build_char_uuid() {
        let base = "12345678123412341234123456789012";
        let uuid = ReliableBleOutput::build_char_uuid(base, "90ae").unwrap();
        let uuid_str = uuid.to_string();
        assert!(uuid_str.contains("90ae"));
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-002
    /// Title: Test catalog serialization
    #[test]
    fn test_serialize_catalog() {
        use crate::domain::ble_protocol::StreamType;

        let catalog = Catalog::default_medical_catalog();

        let mut bytes = Vec::new();
        for entry in &catalog.entries {
            bytes.extend_from_slice(&entry.id.to_le_bytes());
            let name_bytes = entry.name.as_bytes();
            bytes.push(name_bytes.len() as u8);
            bytes.extend_from_slice(name_bytes);
            let type_byte = match entry.stream_type {
                StreamType::Num => 0u8,
                StreamType::Wav => 1u8,
            };
            bytes.push(type_byte);
            bytes.extend_from_slice(&entry.period_ms.to_le_bytes());
        }

        assert!(!bytes.is_empty());
        assert!(bytes.len() >= 30); // 3 entries * ~10 bytes each
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-003
    /// Title: Test signal value extraction (room 0 only)
    #[test]
    fn test_extract_signal_values() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                create_test_track("HR", 75.0, 0, "BED_01"),
                create_test_track("SPO2", 98.0, 0, "BED_01"),
            ],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);

        assert_eq!(values.len(), 2);
        assert!(values.iter().any(|(id, _)| *id == 1)); // HR
        assert!(values.iter().any(|(id, _)| *id == 2)); // SpO2
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-004
    /// Title: Test room filtering (only room 0)
    #[test]
    fn test_room_filtering() {
        let data = ProcessedData::new(
            "VR-TEST".to_string(),
            vec![
                ProcessedRoom {
                    room_index: 0,
                    room_name: "BED_01".to_string(),
                    tracks: vec![create_test_track("HR", 75.0, 0, "BED_01")],
                },
                ProcessedRoom {
                    room_index: 1,
                    room_name: "BED_02".to_string(),
                    tracks: vec![create_test_track("HR", 80.0, 1, "BED_02")],
                },
            ],
        );

        let values = ReliableBleOutput::extract_signal_values(&data);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].1, 75.0);
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-005
    /// Title: Test case-insensitive signal matching
    #[test]
    fn test_case_insensitive_matching() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                create_test_track("hr", 75.0, 0, "BED_01"),
                create_test_track("SpO2", 98.0, 0, "BED_01"),
                create_test_track("TEMPERATURE", 37.0, 0, "BED_01"),
            ],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);

        assert_eq!(values.len(), 3);
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-006
    /// Title: Test write handler dispatches ACK correctly
    #[tokio::test]
    async fn test_write_handler_ack_dispatch() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        // Subscribe and add some data
        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16());
            st.add_data(SignalId::HR.as_u16(), 75.0);
            st.add_data(SignalId::HR.as_u16(), 76.0);
            st.add_data(SignalId::HR.as_u16(), 77.0);
            assert_eq!(st.get_pending_count(), 3);
        }

        // Simulate ACK for seq 1-2
        let ack = AckFrame::new(1, 1, 2, vec![]);
        {
            let mut st = state.write().await;
            let retransmits = st.handle_ack(&ack);
            assert!(retransmits.is_empty());
            // Only seq 3 should remain
            assert_eq!(st.get_pending_count(), 1);
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-007
    /// Title: Test subscribe/unsubscribe via write handler
    #[tokio::test]
    async fn test_subscribe_unsubscribe_via_signal_id() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        // Subscribe to HR (signal_id = 1)
        {
            let mut st = state.write().await;
            st.subscribe(1);
            assert!(st.is_subscribed(1));
        }

        // Unsubscribe from HR
        {
            let mut st = state.write().await;
            st.unsubscribe(1);
            assert!(!st.is_subscribed(1));
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-008
    /// Title: Test subscribe payload parsing (2 bytes LE)
    #[test]
    fn test_subscribe_payload_parsing() {
        // Signal ID 2 (SpO2) as little-endian u16
        let data = 2u16.to_le_bytes();
        let signal_id = u16::from_le_bytes([data[0], data[1]]);
        assert_eq!(signal_id, 2);
        assert_eq!(SignalId::from_u16(signal_id), Some(SignalId::SpO2));
    }
}
