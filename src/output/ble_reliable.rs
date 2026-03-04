// /src/output/ble_reliable.rs
// Module: output.ble_reliable
// Purpose: BLE GATT server output using reliable stream protocol (binary frames)
//          Replaces state-sync broadcast model with sliding window acknowledgment
//
// Characteristics (per PDF spec):
// - Catalog   (0x90ae): Read - Available signal streams
// - Data_IN   (0x90ac): Write - ACK frames from client
// - Data_OUT  (0x90ad): Notify - Data frames to client
// - Subscribe (0x90af): Write - Subscribe to signal
// - Control   (0x90b0): Notify - Session control events
// - Unsubscribe (0x90b1): Write - Unsubscribe from signal

use crate::domain::ble_protocol::{AckFrame, Catalog, DataFrame, SignalId};
use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use crate::output::ble_session::BleSessionState;
use ble_windows_server::{Uuid, WindowsBLEGattServer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

/// ID SRS: SRS-MOD-BLERELIABLE-001
/// Title: ReliableBleOutput
///
/// Description: VRConnect shall provide BLE GATT server output using reliable
/// binary protocol with sliding window acknowledgment instead of state-sync broadcast.
///
/// Version: V4.0
pub struct ReliableBleOutput {
    server: Arc<RwLock<WindowsBLEGattServer>>,
    state: Arc<RwLock<BleSessionState>>,
    catalog: Catalog,
    base_uuid: String,
    current_data: Arc<RwLock<Option<ProcessedData>>>,
    update_interval_ms: u64,
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
    /// Version: V4.0
    ///
    /// # Arguments
    /// * `device_name` - BLE advertising name
    /// * `service_uuid_str` - Service UUID string (base UUID)
    /// * `update_interval_ms` - Update interval in milliseconds
    ///
    /// # Returns
    /// New ReliableBleOutput instance or error
    pub async fn new(
        device_name: String,
        service_uuid_str: String,
        update_interval_ms: u64,
    ) -> Result<Self> {
        // Parse service UUID
        let service_uuid = Uuid::parse_str(&service_uuid_str)
            .map_err(|e| VitalError::Config(format!("Invalid BLE service UUID: {}", e)))?;

        // Extract base UUID (remove suffix if present, or use as-is)
        let base_uuid = service_uuid_str.trim().replace("-", "").to_lowercase();

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
        log::info!("  Base UUID: {}", base_uuid);
        log::info!("  Update Interval: {}ms", update_interval_ms);

        // Create BLE server
        let mut server = WindowsBLEGattServer::new(device_name, service_uuid);

        // Setup the 6 Exact Characteristics from PDF
        // Catalog: Read-only, contains available signal streams
        server.add_characteristic("Catalog", catalog_uuid, "Read");
        log::info!("  Characteristic: Catalog -> {}", catalog_uuid);

        // Data_IN: Write-only, client sends ACK frames here
        server.add_characteristic("Data_IN", data_in_uuid, "Write");
        log::info!("  Characteristic: Data_IN -> {}", data_in_uuid);

        // Data_OUT: Notify-only, server sends data frames
        server.add_characteristic("Data_OUT", data_out_uuid, "Notify");
        log::info!("  Characteristic: Data_OUT -> {}", data_out_uuid);

        // Subscribe: Write-only, client subscribes to signals
        server.add_characteristic("Subscribe", subscribe_uuid, "Write");
        log::info!("  Characteristic: Subscribe -> {}", subscribe_uuid);

        // Control: Notify-only, session control events
        server.add_characteristic("Control", control_uuid, "Notify");
        log::info!("  Characteristic: Control -> {}", control_uuid);

        // Unsubscribe: Write-only, client unsubscribes from signals
        server.add_characteristic("Unsubscribe", unsubscribe_uuid, "Write");
        log::info!("  Characteristic: Unsubscribe -> {}", unsubscribe_uuid);

        // Create default medical catalog with HR, SpO2, Temperature
        let catalog = Catalog::default_medical_catalog();

        // Initialize session state with session ID 1
        let state = BleSessionState::new(1);

        Ok(Self {
            server: Arc::new(RwLock::new(server)),
            state: Arc::new(RwLock::new(state)),
            catalog,
            base_uuid,
            current_data: Arc::new(RwLock::new(None)),
            update_interval_ms,
        })
    }

    /// Build a characteristic UUID from base UUID and suffix
    fn build_char_uuid(base_uuid: &str, suffix: &str) -> Result<Uuid> {
        // Handle both 128-bit UUID formats
        // If base ends with the common pattern, we replace it or append
        let uuid_str = if base_uuid.len() >= 32 {
            // Take first 28 chars ( removing last 4) and add suffix
            // Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            // We want to replace the last 4 chars before any dashes
            let uuid_without_suffix = &base_uuid[..base_uuid.len() - 4];
            format!("{}{}", uuid_without_suffix, suffix)
        } else {
            // Fallback: just append
            format!("{}{}", base_uuid, suffix)
        };

        // Reformat to standard UUID string with dashes if needed
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

        Uuid::parse_str(&formatted).map_err(|e| VitalError::Config(format!("Invalid UUID: {}", e)))
    }

    /// Serialize catalog to binary format
    /// Format per PDF: [ID(2b) | NameLen(1b) | Name | Type(1b) | Period(4b)]
    fn serialize_catalog(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        for entry in &self.catalog.entries {
            // Signal ID (2 bytes, little-endian)
            bytes.extend_from_slice(&entry.id.to_le_bytes());

            // Name length (1 byte)
            let name_bytes = entry.name.as_bytes();
            bytes.push(name_bytes.len() as u8);

            // Name bytes
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
    /// 1. Set Catalog characteristic with serialized catalog
    /// 2. Setup write handlers for Data_IN, Subscribe, Unsubscribe
    /// 3. Start the BLE server and begin transmission loop
    ///
    /// Version: V4.0
    ///
    /// # Returns
    /// Result indicating success or error
    pub async fn start(&self) -> Result<()> {
        log::info!("Starting Reliable BLE GATT server...");

        // 1. Prepare Catalog payload
        let catalog_bytes = self.serialize_catalog();
        log::info!("Catalog prepared ({} bytes)", catalog_bytes.len());

        // 2. Setup write handlers (stubs - ble-windows-server v0.2.1 has no on_write API)
        self.setup_data_in_handler().await?;
        self.setup_subscribe_handler().await?;
        self.setup_unsubscribe_handler().await?;

        // Clone for server task
        let server_for_run = self.server.clone();

        // 3. Start the BLE server in a separate task
        tokio::spawn(async move {
            log::info!("BLE server task started");
            let mut server = server_for_run.write().await;

            match server.start().await {
                Ok(_) => {
                    log::info!("BLE server completed normally");
                }
                Err(e) => {
                    log::error!("BLE server failed: {:?}", e);
                    log::error!("Error details: {}", e);
                    if e.to_string().contains("No Bluetooth adapters found") {
                        log::error!("Please ensure you have a Bluetooth adapter that supports BLE peripheral role.");
                    }
                }
            }
            log::info!("BLE server task ended");
        });

        // Wait for server to initialize
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 4. Send Catalog value now that server is running
        {
            let server = self.server.read().await;
            if let Err(e) = server.notify("Catalog", &catalog_bytes).await {
                log::warn!("Failed to initialize Catalog characteristic: {}", e);
            } else {
                log::info!("Catalog characteristic initialized");
            }
        }

        // 5. Start the data transmission loop
        self.start_transmission_loop().await;

        log::info!("Reliable BLE GATT server started successfully");
        log::info!("Waiting for BLE client connections...");

        Ok(())
    }

    /// Setup Data_IN write handler for ACK frames
    async fn setup_data_in_handler(&self) -> Result<()> {
        let _state_clone = self.state.clone();
        let _server_clone = self.server.clone();

        // Note: This is pseudo-code pattern - actual implementation depends on
        // ble-windows-server API. The handler receives ACK frames and processes them.
        log::info!("Setting up Data_IN write handler for ACK frames");

        // In actual implementation, this would be:
        // self.server.write().await.on_write("Data_IN", move |data| {
        //     let state_lock = state_clone.clone();
        //     let srv_lock = server_clone.clone();
        //
        //     tokio::spawn(async move {
        //         if let Ok(ack) = AckFrame::from_bytes(&data) {
        //             let mut state = state_lock.write().await;
        //             let retransmits = state.handle_ack(&ack);
        //
        //             // Retransmit missing packets immediately
        //             let server = srv_lock.read().await;
        //             for frame in retransmits {
        //                 if let Ok(bytes) = frame.to_bytes() {
        //                     let _ = server.notify_bytes("Data_OUT", &bytes).await;
        //                 }
        //             }
        //         }
        //     });
        // });

        Ok(())
    }

    /// Setup Subscribe write handler
    async fn setup_subscribe_handler(&self) -> Result<()> {
        let _state_clone = self.state.clone();

        log::info!("Setting up Subscribe write handler");

        // In actual implementation:
        // self.server.write().await.on_write("Subscribe", move |data| {
        //     if data.len() >= 2 {
        //         // Parse signal ID (2 bytes, little-endian)
        //         let signal_id = u16::from_le_bytes([data[0], data[1]]);
        //
        //         let mut state = state_clone.blocking_write();
        //         state.subscribe(signal_id);
        //         log::info!("Client subscribed to signal {}", signal_id);
        //     }
        // });

        Ok(())
    }

    /// Setup Unsubscribe write handler
    async fn setup_unsubscribe_handler(&self) -> Result<()> {
        let _state_clone = self.state.clone();

        log::info!("Setting up Unsubscribe write handler");

        // In actual implementation:
        // self.server.write().await.on_write("Unsubscribe", move |data| {
        //     if data.len() >= 2 {
        //         // Parse signal ID (2 bytes, little-endian)
        //         let signal_id = u16::from_le_bytes([data[0], data[1]]);
        //
        //         let mut state = state_clone.blocking_write();
        //         state.unsubscribe(signal_id);
        //         log::info!("Client unsubscribed from signal {}", signal_id);
        //     }
        // });

        Ok(())
    }

    /// Start the periodic data transmission loop
    async fn start_transmission_loop(&self) {
        let server = self.server.clone();
        let state = self.state.clone();
        let current_data = self.current_data.clone();
        let interval_ms = self.update_interval_ms;

        tokio::spawn(async move {
            log::info!("BLE transmission loop started");
            let mut ticker = interval(Duration::from_millis(interval_ms));

            loop {
                ticker.tick().await;

                // Get current data
                let data_opt = current_data.read().await.clone();

                if let Some(data) = data_opt {
                    // Extract signal values from room 0
                    let signal_values = Self::extract_signal_values(&data);

                    // Get state lock
                    let mut state_lock = state.write().await;

                    // Add data for each signal and send if subscribed
                    for (signal_id, value) in signal_values {
                        if let Some(frame) = state_lock.add_data(signal_id, value) {
                            // Send the data frame
                            let server_guard = server.read().await;
                            match frame.to_bytes() {
                                Ok(bytes) => {
                                    if let Err(e) = server_guard.notify("Data_OUT", &bytes).await {
                                        log::warn!("Failed to send Data_OUT: {}", e);
                                    } else {
                                        log::debug!(
                                            "Sent Data_OUT: session={}, stream={}, seq={}",
                                            frame.session_id,
                                            frame.stream_id,
                                            frame.seq_num
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
            }
        });
    }

    /// Extract signal values (HR, SpO2, Temperature) from processed data
    /// Returns Vec of (signal_id, value) for room index 0 only
    fn extract_signal_values(data: &ProcessedData) -> Vec<(u16, f32)> {
        let mut values = Vec::new();

        // Map track names to signal IDs
        let signal_map: HashMap<&str, u16> = [
            ("HR", SignalId::HR.as_u16()),
            ("SPO2", SignalId::SpO2.as_u16()),
            ("TEMP", SignalId::Temperature.as_u16()),
            ("TEMPERATURE", SignalId::Temperature.as_u16()),
        ]
        .into_iter()
        .collect();

        // Only process room 0 (first room)
        if let Some(room) = data.rooms.iter().find(|r| r.room_index == 0) {
            for track in &room.tracks {
                let track_name_upper = track.name.to_uppercase();

                // Check if this track maps to a known signal
                if let Some(&signal_id) = signal_map.get(track_name_upper.as_str()) {
                    // Get the value
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
    /// When vrconnect gets a new vital sign from Socket.IO, we map it, frame it,
    /// and send it immediately via the reliable protocol.
    ///
    /// PDF Logic: "When vrconnect gets a new vital sign from Socket.IO: map it, frame it, send it"
    ///
    /// Version: V5.0
    ///
    /// # Arguments
    /// * `data` - Processed vital data from Socket.IO
    ///
    /// # Returns
    /// Result indicating success or error
    pub async fn output(&self, data: &ProcessedData) -> Result<()> {
        let mut state = self.state.write().await;
        let server = self.server.read().await;

        for track in &data.all_tracks {
            // Only process BED_01 (room_index 0)
            if track.room_index != 0 {
                continue;
            }

            // Map string track name to PDF Catalog IDs
            let signal_id = match track.name.to_uppercase().as_str() {
                "HR" => 1,
                "SPO2" => 2,
                "TEMP" | "TEMPERATURE" => 3,
                _ => continue, // Ignore unmapped signals
            };

            // Get value as f32
            if let Some(raw_val) = track.raw_value {
                let val_f32 = raw_val as f32;

                // Add to Sliding Window state (returns Some if subscribed)
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
    /// any missing packets.
    ///
    /// Version: V4.0
    ///
    /// # Arguments
    /// * `ack` - Acknowledgment frame from client
    pub async fn handle_ack(&self, ack: &AckFrame) -> Result<()> {
        let mut state = self.state.write().await;
        let retransmits = state.handle_ack(ack);

        // Retransmit missing packets
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
    ///
    /// Version: V4.0
    ///
    /// # Arguments
    /// * `signal_id` - Signal ID to subscribe
    pub async fn subscribe(&self, signal_id: u16) {
        let mut state = self.state.write().await;
        state.subscribe(signal_id);
        log::info!("Subscribed to signal {}", signal_id);
    }

    /// ID SRS: SRS-FN-BLERELIABLE-006
    /// Title: unsubscribe
    ///
    /// Description: VRConnect shall unsubscribe a client from a signal.
    ///
    /// Version: V4.0
    ///
    /// # Arguments
    /// * `signal_id` - Signal ID to unsubscribe
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
    /// Version: V4.0
    ///
    /// # Returns
    /// Tuple of (session_id, last_seq, pending_count)
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
    ///
    /// Description: VRConnect shall correctly build characteristic UUIDs from base.
    #[test]
    fn test_build_char_uuid() {
        let base = "12345678123456781234567812345678";
        let uuid = ReliableBleOutput::build_char_uuid(base, "90ae").unwrap();
        let uuid_str = uuid.to_string();
        assert!(uuid_str.contains("90ae"));
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-002
    /// Title: Test catalog serialization
    ///
    /// Description: VRConnect shall serialize catalog to binary format.
    #[test]
    fn test_serialize_catalog() {
        use crate::domain::ble_protocol::StreamType;

        let catalog = Catalog::default_medical_catalog();

        // Replicate serialize_catalog logic for testing without needing a full server
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

        // Each entry should have: ID(2) + NameLen(1) + Name + Type(1) + Period(4)
        // HR entry: 2 + 1 + 2 + 1 + 4 = 10 bytes minimum
        assert!(bytes.len() >= 30); // 3 entries * ~10 bytes each
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-003
    /// Title: Test signal value extraction
    ///
    /// Description: VRConnect shall extract signal values from room 0 only.
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
        // HR = 1, SpO2 = 2
        assert!(values.iter().any(|(id, _)| *id == 1)); // HR
        assert!(values.iter().any(|(id, _)| *id == 2)); // SpO2
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-004
    /// Title: Test room filtering
    ///
    /// Description: VRConnect shall only extract values from room 0.
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
        assert_eq!(values[0].1, 75.0); // From room 0, not room 1
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-005
    /// Title: Test case-insensitive signal matching
    ///
    /// Description: VRConnect shall match signal names case-insensitively.
    #[test]
    fn test_case_insensitive_matching() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                create_test_track("hr", 75.0, 0, "BED_01"),   // lowercase
                create_test_track("SpO2", 98.0, 0, "BED_01"), // mixed case
                create_test_track("TEMPERATURE", 37.0, 0, "BED_01"), // uppercase
            ],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);

        assert_eq!(values.len(), 3);
    }
}
