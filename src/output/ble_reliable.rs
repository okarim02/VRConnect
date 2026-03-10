// /src/output/ble_reliable.rs
// Module: output.ble_reliable
// Purpose: BLE GATT server output using the IDT ("ICU Data Transport") reliable protocol.
//          Full IDT implementation: DATA_FRAME (35b), ACK_FRAME (24b), NACK_FRAME,
//          SUBSCRIBE_REQ / SUBSCRIBE_RSP, per-stream sequence + retransmit buffers.
//
// Uses our custom GattServer (ble_gatt.rs) which supports Write callbacks,
// replacing the ble-windows-server crate that only supports Read + Notify.
//
// Characteristics (per PDF "Proposition de protocole BLE"):
// - Catalog     (0x90ae): Read   - Available signal catalog (TLV binary)
// - Data_IN     (0x90ac): Write  - ACK_FRAME / NACK_FRAME from client
// - Data_OUT    (0x90ad): Notify - DATA_FRAME + SUBSCRIBE_RSP to client
// - Subscribe   (0x90af): Write  - SUBSCRIBE_REQ (IDT) from client
// - Control     (0x90b0): Notify - (legacy / reserved, no longer used for SUBSCRIBE_RSP)
// - Unsubscribe (0x90b1): Write  - SUBSCRIBE_REQ with op=UNSUBSCRIBE, or legacy 2b fallback

use crate::domain::ble_protocol::{
    parse_tlv_subscribe_req, AckFrame, Catalog, InboundFrame, SignalId, SubscribeReq, SubscribeRsp,
    SubscribeRspItem, SUB_OP_SUBSCRIBE, SUB_OP_UNSUBSCRIBE,
};
use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use crate::output::ble_gatt::{CharProperty, GattServer, WriteEvent};
use crate::output::ble_session::BleSessionState;
use std::sync::Arc;
use tokio::sync::RwLock;

/// ID SRS: SRS-MOD-BLERELIABLE-001
/// Title: ReliableBleOutput
///
/// Description: VRConnect shall provide BLE GATT server output using the IDT reliable
///              protocol with per-signal streams, cumulative ACK, and explicit NACK retransmit.
///
/// Version: V6.0
pub struct ReliableBleOutput {
    server: Arc<RwLock<GattServer>>,
    state: Arc<RwLock<BleSessionState>>,
    catalog: Catalog,
}

/// Characteristic UUID suffixes (PDF spec)
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
    /// Description: VRConnect shall construct a ReliableBleOutput instance, register
    ///              the 6 standard GATT characteristics, and initialize session state.
    ///
    /// Version: V6.0
    pub async fn new(
        device_name: String,
        service_uuid_str: String,
        _update_interval_ms: u64,
    ) -> Result<Self> {
        let service_uuid = uuid::Uuid::parse_str(&service_uuid_str)
            .map_err(|e| VitalError::Config(format!("Invalid BLE service UUID: {}", e)))?;

        let base_uuid = service_uuid_str.trim().replace('-', "").to_lowercase();

        let catalog_uuid = Self::build_char_uuid(&base_uuid, CATALOG_UUID_SUFFIX)?;
        let data_in_uuid = Self::build_char_uuid(&base_uuid, DATA_IN_UUID_SUFFIX)?;
        let data_out_uuid = Self::build_char_uuid(&base_uuid, DATA_OUT_UUID_SUFFIX)?;
        let subscribe_uuid = Self::build_char_uuid(&base_uuid, SUBSCRIBE_UUID_SUFFIX)?;
        let control_uuid = Self::build_char_uuid(&base_uuid, CONTROL_UUID_SUFFIX)?;
        let unsubscribe_uuid = Self::build_char_uuid(&base_uuid, UNSUBSCRIBE_UUID_SUFFIX)?;

        log::info!("Reliable BLE Output Configuration:");
        log::info!("  Device Name: {}", device_name);
        log::info!("  Service UUID: {}", service_uuid);

        let mut server = GattServer::new(device_name, service_uuid);

        server.add_characteristic("Catalog", catalog_uuid, &[CharProperty::Read]);
        log::info!("  Characteristic: Catalog (Read)      -> {}", catalog_uuid);

        server.add_characteristic(
            "Data_IN",
            data_in_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );
        log::info!("  Characteristic: Data_IN (Write)     -> {}", data_in_uuid);

        server.add_characteristic("Data_OUT", data_out_uuid, &[CharProperty::Notify]);
        log::info!("  Characteristic: Data_OUT (Notify)   -> {}", data_out_uuid);

        server.add_characteristic(
            "Subscribe",
            subscribe_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );
        log::info!("  Characteristic: Subscribe (Write)   -> {}", subscribe_uuid);

        server.add_characteristic("Control", control_uuid, &[CharProperty::Notify]);
        log::info!("  Characteristic: Control (Notify)    -> {}", control_uuid);

        server.add_characteristic(
            "Unsubscribe",
            unsubscribe_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );
        log::info!("  Characteristic: Unsubscribe (Write) -> {}", unsubscribe_uuid);

        let catalog = Catalog::default_medical_catalog();
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
            let uuid_without_suffix = &base_uuid[..base_uuid.len() - 4];
            format!("{}{}", uuid_without_suffix, suffix)
        } else {
            format!("{}{}", base_uuid, suffix)
        };

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

    /// ID SRS: SRS-FN-BLERELIABLE-002
    /// Title: start
    ///
    /// Description: VRConnect shall start the BLE GATT server:
    ///   1. Set Catalog read value (IDT TLV binary via Catalog::to_ble_bytes)
    ///   2. Spawn the write-handler task (Data_IN / Subscribe / Unsubscribe)
    ///   3. Start the GATT server (creates Windows GATT service + advertises)
    ///
    /// Version: V6.0
    pub async fn start(&self) -> Result<()> {
        log::info!("Starting Reliable BLE GATT server (IDT protocol)...");

        // 1. Serialize catalog using new IDT TLV binary format
        let catalog_bytes = self.catalog.to_ble_bytes();
        log::info!(
            "Catalog prepared ({} bytes, {} signals)",
            catalog_bytes.len(),
            self.catalog.entries.len()
        );
        {
            let mut server = self.server.write().await;
            server.set_read_value("Catalog", catalog_bytes);
        }

        // 2. Spawn write-handler task
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
            log::warn!("Write receiver already taken — write handlers won't work");
        }

        // 3. Start GATT server
        {
            let mut server = self.server.write().await;
            server.start().await?;
        }
        log::info!("Reliable BLE GATT server started successfully (IDT v1)");
        log::info!("Waiting for BLE client connections...");
        Ok(())
    }

    /// Background task: reads write events from the GATT server and dispatches them.
    async fn write_handler_loop(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WriteEvent>,
        state: Arc<RwLock<BleSessionState>>,
        server: Arc<RwLock<GattServer>>,
    ) {
        log::info!("Write handler loop running (IDT dispatcher)");

        while let Some(event) = rx.recv().await {
            match event.characteristic_name.as_str() {
                // ── Data_IN: ACK_FRAME or NACK_FRAME from client ──────────────
                "Data_IN" => {
                    match InboundFrame::from_ble_bytes(&event.data) {
                        Some(InboundFrame::Ack(ack)) => {
                            log::debug!(
                                "IDT ACK: session={}, stream={}, ack_upto={}",
                                ack.header.session_id,
                                ack.header.stream_id,
                                ack.ack_upto
                            );
                            let mut st = state.write().await;
                            st.handle_ack(
                                ack.header.session_id,
                                ack.header.stream_id,
                                ack.ack_upto,
                            );
                        }
                        Some(InboundFrame::Nack(nack)) => {
                            log::info!(
                                "IDT NACK: stream={}, reason={}, {} seq(s) to retransmit",
                                nack.header.stream_id,
                                nack.reason,
                                nack.seq_list.len()
                            );
                            let retransmits = {
                                let st = state.read().await;
                                st.handle_nack(nack.header.stream_id, &nack.seq_list)
                            };
                            if !retransmits.is_empty() {
                                let srv = server.read().await;
                                for frame in retransmits {
                                    let bytes = frame.to_ble_bytes();
                                    if let Err(e) = srv.notify("Data_OUT", &bytes).await {
                                        log::warn!(
                                            "Retransmit failed for seq {}: {}",
                                            frame.header.seq,
                                            e
                                        );
                                    } else {
                                        log::debug!("Retransmitted seq {}", frame.header.seq);
                                    }
                                }
                            }
                        }
                        _ => {
                            log::warn!(
                                "Invalid IDT frame on Data_IN ({} bytes): bad magic or msg_type",
                                event.data.len()
                            );
                        }
                    }
                }

                // ── Subscribe: SUBSCRIBE_REQ (IDT) from client ────────────────
                "Subscribe" => {
                    // Always dump raw bytes at INFO level — essential for protocol debugging
                    let hex: String = event.data.iter()
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    log::info!(
                        "Subscribe raw ({} bytes): {}",
                        event.data.len(),
                        hex
                    );

                    if let Some(InboundFrame::SubscribeReq(req)) =
                        InboundFrame::from_ble_bytes(&event.data)
                    {
                        Self::handle_subscribe_req(req, &state, &server).await;
                    } else if let Some((req_id, signal_ids)) =
                        parse_tlv_subscribe_req(&event.data)
                    {
                        log::info!(
                            "TLV subscribe: req_id={}, signals={:?}",
                            req_id,
                            signal_ids
                        );
                        Self::handle_tlv_subscribe(req_id, signal_ids, &state, &server).await;
                    } else {
                        // Log why it failed for diagnosis
                        Self::log_subscribe_parse_failure(&event.data);
                    }
                }

                // ── Unsubscribe: IDT SUBSCRIBE_REQ (op=2) or legacy 2-byte ───
                "Unsubscribe" => {
                    if let Some(InboundFrame::SubscribeReq(req)) =
                        InboundFrame::from_ble_bytes(&event.data)
                    {
                        // IDT: full SUBSCRIBE_REQ with op=UNSUBSCRIBE
                        Self::handle_subscribe_req(req, &state, &server).await;
                    } else if event.data.len() >= 2 {
                        // Legacy fallback: 2-byte signal_id LE (old protocol)
                        let signal_id = u16::from_le_bytes([event.data[0], event.data[1]]);
                        let mut st = state.write().await;
                        st.unsubscribe(signal_id);
                        log::info!("Unsubscribed (legacy 2b) signal 0x{:04X}", signal_id);
                    } else {
                        log::warn!(
                            "Invalid Unsubscribe payload ({} bytes): too short",
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

    /// Handle a SUBSCRIBE_REQ IDT frame:
    ///   - op=1 (SUBSCRIBE):   allocate stream → send SUBSCRIBE_RSP on Data_OUT
    ///   - op=2 (UNSUBSCRIBE): remove stream, no RSP sent
    async fn handle_subscribe_req(
        req: SubscribeReq,
        state: &Arc<RwLock<BleSessionState>>,
        server: &Arc<RwLock<GattServer>>,
    ) {
        let session_id = req.header.session_id;
        let req_id = req.req_id;
        let mut rsp_items: Vec<SubscribeRspItem> = Vec::new();

        {
            let mut st = state.write().await;
            for item in &req.items {
                match req.op {
                    SUB_OP_SUBSCRIBE => {
                        // Normalize legacy simple IDs (1,2,3 per I.pdf) to IDT compound IDs
                        // (0x0101,0x0102,0x0103 per "Proposition de protocole BLE")
                        let canonical_id = SignalId::from_u16(item.signal_id)
                            .map(|s| s.as_u16())
                            .unwrap_or(item.signal_id);
                        if canonical_id != item.signal_id {
                            log::info!(
                                "Signal ID: app sent 0x{:04X} → normalized to 0x{:04X} (legacy→IDT)",
                                item.signal_id,
                                canonical_id
                            );
                        }
                        let stream_id = st.subscribe(canonical_id);
                        let (period_ms, source_id) =
                            if let Some(sig) = SignalId::from_u16(item.signal_id) {
                                (sig.nominal_period_ms(), sig.source_id())
                            } else {
                                (item.period_ms, item.source_id)
                            };
                        rsp_items.push(SubscribeRspItem {
                            source_id,
                            signal_id: canonical_id,
                            stream_id,
                            effective_period_ms: period_ms,
                            effective_batch_max: 1,
                        });
                        log::info!(
                            "SUBSCRIBE: signal 0x{:04X} → stream {}",
                            canonical_id,
                            stream_id
                        );
                    }
                    SUB_OP_UNSUBSCRIBE => {
                        // Also normalize on unsubscribe
                        let canonical_id = SignalId::from_u16(item.signal_id)
                            .map(|s| s.as_u16())
                            .unwrap_or(item.signal_id);
                        st.unsubscribe(canonical_id);
                        log::info!("UNSUBSCRIBE: signal 0x{:04X}", canonical_id);
                    }
                    _ => {
                        log::warn!("Unknown subscribe op: 0x{:02X}", req.op);
                    }
                }
            }
        }

        // Send SUBSCRIBE_RSP for SUBSCRIBE op with allocated streams.
        // Notify on both Data_OUT (per IDT spec) and Control (per I.pdf / legacy clients).
        if req.op == SUB_OP_SUBSCRIBE && !rsp_items.is_empty() {
            let rsp = SubscribeRsp {
                session_id,
                req_id,
                status: 0, // 0 = OK
                results: rsp_items,
            };
            let bytes = rsp.to_ble_bytes();
            let srv = server.read().await;
            if let Err(e) = srv.notify("Data_OUT", &bytes).await {
                log::warn!("Failed to send SUBSCRIBE_RSP on Data_OUT: {}", e);
            } else {
                log::info!(
                    "SUBSCRIBE_RSP sent on Data_OUT ({} bytes, req_id={})",
                    bytes.len(),
                    req_id
                );
            }
            // Also send on Control char — some Flutter implementations listen here (I.pdf)
            if let Err(e) = srv.notify("Control", &bytes).await {
                log::debug!("SUBSCRIBE_RSP on Control: {} (client may not be subscribed)", e);
            } else {
                log::info!("SUBSCRIBE_RSP also sent on Control ({} bytes)", bytes.len());
            }
        }
    }

    /// Handle a TLV-format SUBSCRIBE_REQ from the Flutter app (non-IDT wire format).
    /// Normalizes legacy signal IDs (1,2,3) to IDT compound IDs (0x0101-0x0103),
    /// subscribes each signal, and sends SUBSCRIBE_RSP on Data_OUT and Control.
    ///
    /// Stream ID assignment: use `raw_id.swap_bytes()` so that when the Flutter Dart
    /// code reads stream_id as big-endian (Dart ByteData default), it recovers the
    /// original legacy signal ID (1, 2, 3).
    ///   raw_id=1  → stream_id=0x0100=256 → LE bytes [0x00,0x01] → Dart BE-read → 1
    ///   raw_id=2  → stream_id=0x0200=512 → LE bytes [0x00,0x02] → Dart BE-read → 2
    ///   raw_id=3  → stream_id=0x0300=768 → LE bytes [0x00,0x03] → Dart BE-read → 3
    ///
    /// RSP is delayed 300 ms so the client has time to enable CCCD notifications
    /// before the notification arrives.
    async fn handle_tlv_subscribe(
        req_id: u16,
        signal_ids: Vec<u16>,
        state: &Arc<RwLock<BleSessionState>>,
        server: &Arc<RwLock<GattServer>>,
    ) {
        let session_id = state.read().await.current_session_id;
        let mut rsp_items: Vec<SubscribeRspItem> = Vec::new();

        {
            let mut st = state.write().await;
            for raw_id in &signal_ids {
                let canonical_id = SignalId::from_u16(*raw_id)
                    .map(|s| s.as_u16())
                    .unwrap_or(*raw_id);
                if canonical_id != *raw_id {
                    log::info!(
                        "TLV Signal ID: app sent 0x{:04X} → normalized to 0x{:04X} (legacy→IDT)",
                        raw_id,
                        canonical_id
                    );
                }
                // Byteswap the legacy raw_id so Dart's big-endian stream_id read gives raw_id back
                let preferred_stream_id = raw_id.swap_bytes();
                let stream_id = st.subscribe_with_stream_id(canonical_id, preferred_stream_id);
                let (period_ms, source_id) = if let Some(sig) = SignalId::from_u16(*raw_id) {
                    (sig.nominal_period_ms(), sig.source_id())
                } else {
                    (1000, 1)
                };
                rsp_items.push(SubscribeRspItem {
                    source_id,
                    signal_id: canonical_id,
                    stream_id,
                    effective_period_ms: period_ms,
                    effective_batch_max: 1,
                });
                log::info!(
                    "TLV SUBSCRIBE: signal 0x{:04X} → stream {} (Dart BE-reads as {})",
                    canonical_id,
                    stream_id,
                    raw_id
                );
            }
        }

        if !rsp_items.is_empty() {
            // Delay before sending RSP to ensure the client has enabled CCCD notifications.
            // The Flutter app enables CCCD *after* writing to the Subscribe characteristic,
            // so without a delay the notify would be silently dropped.
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

            let rsp = SubscribeRsp {
                session_id,
                req_id,
                status: 0,
                results: rsp_items,
            };
            let bytes = rsp.to_ble_bytes();
            let srv = server.read().await;
            if let Err(e) = srv.notify("Data_OUT", &bytes).await {
                log::warn!("Failed to send TLV SUBSCRIBE_RSP on Data_OUT: {}", e);
            } else {
                log::info!(
                    "TLV SUBSCRIBE_RSP sent on Data_OUT ({} bytes, req_id={})",
                    bytes.len(),
                    req_id
                );
            }
            if let Err(e) = srv.notify("Control", &bytes).await {
                log::debug!(
                    "TLV SUBSCRIBE_RSP on Control: {} (client may not be subscribed)",
                    e
                );
            } else {
                log::info!("TLV SUBSCRIBE_RSP also sent on Control ({} bytes)", bytes.len());
            }
        }
    }

    /// Extract (signal_id, f32) pairs from ProcessedData for room_index=0.
    /// Signal name mapping uses VitalRecorder names ("HR", "SPO2", "TEMP"/"TEMPERATURE").
    #[cfg(test)]
    fn extract_signal_values(data: &ProcessedData) -> Vec<(u16, f32)> {
        use std::collections::HashMap;
        let signal_map: HashMap<&str, u16> = [
            ("HR", SignalId::HR.as_u16()),
            ("SPO2", SignalId::SpO2.as_u16()),
            ("TEMP", SignalId::Temperature.as_u16()),
            ("TEMPERATURE", SignalId::Temperature.as_u16()),
        ]
        .into_iter()
        .collect();

        let mut values = Vec::new();
        if let Some(room) = data.rooms.iter().find(|r| r.room_index == 0) {
            for track in &room.tracks {
                let name_upper = track.name.to_uppercase();
                if let Some(&sid) = signal_map.get(name_upper.as_str()) {
                    if let Some(raw) = track.raw_value {
                        values.push((sid, raw as f32));
                    } else if let Ok(parsed) = track.display_value.parse::<f32>() {
                        values.push((sid, parsed));
                    }
                }
            }
        }
        values
    }

    /// ID SRS: SRS-FN-BLERELIABLE-003
    /// Title: output
    ///
    /// Description: VRConnect shall transmit live vital sign data via IDT DATA_FRAME.
    ///              For each track in room_index=0 that matches a subscribed signal,
    ///              a 35-byte IDT DATA_FRAME (with t0_ms timestamp) is notified on Data_OUT.
    ///
    /// Version: V6.0
    pub async fn output(&self, data: &ProcessedData) -> Result<()> {
        let mut state = self.state.write().await;
        let server = self.server.read().await;

        for track in &data.all_tracks {
            // Only BED_01 (room_index = 0)
            if track.room_index != 0 {
                continue;
            }

            // Map VitalRecorder signal name → IDT signal_id
            let signal_id = match track.name.to_uppercase().as_str() {
                "HR" => SignalId::HR.as_u16(),
                "SPO2" => SignalId::SpO2.as_u16(),
                "TEMP" | "TEMPERATURE" => SignalId::Temperature.as_u16(),
                _ => continue,
            };

            let val_f32 = if let Some(raw) = track.raw_value {
                raw as f32
            } else if let Ok(parsed) = track.display_value.parse::<f32>() {
                parsed
            } else {
                continue;
            };

            // Sample timestamp (milliseconds since Unix epoch)
            let t0_ms = track.timestamp.timestamp_millis() as u64;

            // add_data returns Some(frame) only if signal is subscribed
            if let Some(frame) = state.add_data(signal_id, val_f32, t0_ms) {
                let bytes = frame.to_ble_bytes();
                if let Err(e) = server.notify("Data_OUT", &bytes).await {
                    log::warn!("BLE notify failed for signal 0x{:04X}: {}", signal_id, e);
                } else {
                    log::debug!(
                        "Data_OUT: signal=0x{:04X}, stream={}, seq={}, {} bytes",
                        signal_id,
                        frame.header.stream_id,
                        frame.header.seq,
                        bytes.len()
                    );
                }
            }
        }
        Ok(())
    }

    /// ID SRS: SRS-FN-BLERELIABLE-004
    /// Title: handle_ack_idt
    ///
    /// Description: VRConnect shall process a parsed IDT AckFrame (external callers).
    ///              Delegates to BleSessionState::handle_ack with the IDT header fields.
    ///
    /// Version: V6.0
    pub async fn handle_ack_idt(&self, ack: &AckFrame) -> Result<()> {
        let mut state = self.state.write().await;
        state.handle_ack(ack.header.session_id, ack.header.stream_id, ack.ack_upto);
        Ok(())
    }

    /// ID SRS: SRS-FN-BLERELIABLE-005
    /// Title: subscribe
    ///
    /// Description: VRConnect shall subscribe a client to a signal (external callers).
    ///
    /// Version: V6.0
    pub async fn subscribe(&self, signal_id: u16) {
        let mut state = self.state.write().await;
        let stream_id = state.subscribe(signal_id);
        log::info!("Subscribed to signal 0x{:04X} → stream {}", signal_id, stream_id);
    }

    /// ID SRS: SRS-FN-BLERELIABLE-006
    /// Title: unsubscribe
    ///
    /// Description: VRConnect shall unsubscribe a client from a signal (external callers).
    ///
    /// Version: V6.0
    pub async fn unsubscribe(&self, signal_id: u16) {
        let mut state = self.state.write().await;
        state.unsubscribe(signal_id);
        log::info!("Unsubscribed from signal 0x{:04X}", signal_id);
    }

    /// ID SRS: SRS-FN-BLERELIABLE-007
    /// Title: get_session_stats
    ///
    /// Description: VRConnect shall return current IDT session statistics
    ///              (session_id, total pending frames across all streams).
    ///
    /// Version: V6.0
    pub async fn get_session_stats(&self) -> (u16, usize) {
        let state = self.state.read().await;
        (state.current_session_id, state.total_pending())
    }

    /// Diagnostic helper: logs why a SUBSCRIBE_REQ frame failed to parse.
    /// Tries all candidate SubscribeItem sizes (17..=30) to identify the correct one,
    /// and dumps the first item's raw bytes + signal_id for protocol mismatch detection.
    fn log_subscribe_parse_failure(data: &[u8]) {
        use crate::domain::ble_protocol::{IDT_MAGIC, MSG_SUBSCRIBE_REQ};

        if data.len() < 16 {
            log::warn!(
                "Subscribe PARSE FAIL: too short ({} bytes, need ≥ 16 for IDT header)",
                data.len()
            );
            return;
        }
        let magic = u16::from_le_bytes([data[0], data[1]]);
        if magic != IDT_MAGIC {
            log::warn!(
                "Subscribe PARSE FAIL: bad magic 0x{:04X} (expected 0x{:04X}=IDT_MAGIC)",
                magic,
                IDT_MAGIC
            );
            return;
        }
        let msg_type = data[3];
        if msg_type != MSG_SUBSCRIBE_REQ {
            log::warn!(
                "Subscribe PARSE FAIL: msg_type=0x{:02X} (expected 0x01=SUBSCRIBE_REQ)",
                msg_type
            );
            return;
        }
        if data.len() < 20 {
            log::warn!(
                "Subscribe PARSE FAIL: too short for fixed payload fields ({} bytes)",
                data.len()
            );
            return;
        }

        let req_id = u16::from_le_bytes([data[16], data[17]]);
        let op = data[18];
        let n = data[19] as usize;

        log::warn!(
            "Subscribe PARSE FAIL: req_id={}, op=0x{:02X}, n={} items, {} bytes total",
            req_id,
            op,
            n,
            data.len()
        );

        // Brute-force candidate item sizes to find CRC match
        if n > 0 {
            // fixed overhead: header(16) + req_id(2)+op(1)+n(1) + CRC(4)
            let fixed = 16 + 4 + 4;
            let payload_bytes = data.len().saturating_sub(fixed);
            log::warn!(
                "  item payload bytes = {} ÷ {} items = {} bytes/item  (server expects 17)",
                payload_bytes,
                n,
                payload_bytes / n
            );

            for candidate in 17usize..=30 {
                let expected_total = 16 + 4 + n * candidate + 4;
                if expected_total == data.len() {
                    let crc_off = expected_total - 4;
                    let expected_crc = crc32c::crc32c(&data[..crc_off]);
                    let actual_crc = u32::from_le_bytes([
                        data[crc_off],
                        data[crc_off + 1],
                        data[crc_off + 2],
                        data[crc_off + 3],
                    ]);
                    if expected_crc == actual_crc {
                        log::warn!(
                            "  → item_size={}: CRC MATCH ← fix SubscribeItem::SIZE to {}",
                            candidate,
                            candidate
                        );
                    } else {
                        log::warn!(
                            "  → item_size={}: total matches but CRC fails (exp=0x{:08X} got=0x{:08X})",
                            candidate,
                            expected_crc,
                            actual_crc
                        );
                    }
                }
            }
        }

        // Dump first item bytes for signal_id inspection
        if n > 0 && data.len() > 20 {
            let end = data.len().min(20 + 30);
            let raw: String = data[20..end]
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ");
            log::warn!("  item[0] raw: {}", raw);
            if data.len() >= 23 {
                let src = data[20];
                let sig = u16::from_le_bytes([data[21], data[22]]);
                let sig_label = match sig {
                    0x0101 => "HR (IDT compound ID)",
                    0x0102 => "SpO2 (IDT compound ID)",
                    0x0103 => "Temp (IDT compound ID)",
                    1 => "HR? (legacy simple ID — mismatch!)",
                    2 => "SpO2? (legacy simple ID — mismatch!)",
                    3 => "Temp? (legacy simple ID — mismatch!)",
                    _ => "unknown",
                };
                log::warn!(
                    "  item[0] source_id={}, signal_id=0x{:04X} ({})",
                    src,
                    sig,
                    sig_label
                );
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ProcessedRoom, ProcessedTrack, TrackType};
    use crate::domain::ble_protocol::{
        AckFrame, FLAG_RETRANSMIT, IDT_MAGIC, IDT_HEADER_LEN, IDT_VERSION,
        IdtHeader, InboundFrame, MSG_ACK_FRAME, MSG_SUBSCRIBE_REQ, MSG_SUBSCRIBE_RSP,
        SubscribeItem, SubscribeRsp, SubscribeRspItem, SUB_OP_SUBSCRIBE,
    };
    use chrono::Utc;

    /// Helper: create a test ProcessedTrack
    fn create_test_track(name: &str, value: f64, room_index: i32, room_name: &str) -> ProcessedTrack {
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

    /// Helper: build a valid 24-byte IDT ACK_FRAME buffer
    fn make_ack_bytes(session_id: u16, stream_id: u16, ack_upto: u32) -> Vec<u8> {
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_ACK_FRAME,
            flags: 0,
            header_len: IDT_HEADER_LEN,
            session_id,
            stream_id,
            seq: 0,
            payload_len: 4,
        };
        let mut buf: Vec<u8> = header.to_bytes().to_vec();
        buf.extend_from_slice(&ack_upto.to_le_bytes());
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Helper: build a valid IDT SUBSCRIBE_REQ buffer with one item
    fn make_subscribe_req_bytes(
        session_id: u16,
        req_id: u16,
        op: u8,
        signal_id: u16,
    ) -> Vec<u8> {
        let n = 1usize;
        let payload_len = 4 + n * SubscribeItem::SIZE;
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_SUBSCRIBE_REQ,
            flags: 0,
            header_len: IDT_HEADER_LEN,
            session_id,
            stream_id: 0,
            seq: 0,
            payload_len: payload_len as u16,
        };
        let mut buf: Vec<u8> = header.to_bytes().to_vec();
        buf.extend_from_slice(&req_id.to_le_bytes());
        buf.push(op);
        buf.push(1u8); // n = 1 item
        buf.push(1u8); // source_id
        buf.extend_from_slice(&signal_id.to_le_bytes());
        buf.push(0u8); // mode = LIVE
        buf.extend_from_slice(&1000u32.to_le_bytes()); // period_ms
        buf.push(1u8); // batch_max
        buf.extend_from_slice(&0u64.to_le_bytes()); // start_time_ms
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    // ── UUID / build ──────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-001
    /// Title: Test UUID building
    #[test]
    fn test_build_char_uuid() {
        let base = "12345678123412341234123456789012";
        let uuid = ReliableBleOutput::build_char_uuid(base, "90ae").unwrap();
        let uuid_str = uuid.to_string();
        assert!(uuid_str.contains("90ae"), "UUID should contain suffix 90ae");
    }

    // ── Catalog ───────────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-002
    /// Title: Test catalog serialization (IDT TLV format)
    #[test]
    fn test_serialize_catalog() {
        let catalog = Catalog::default_medical_catalog();
        let bytes = catalog.to_ble_bytes();
        assert!(!bytes.is_empty());
        // First entry: source_id=1, then signal_id=0x0101 LE → [0x01, 0x01]
        assert_eq!(bytes[0], 1u8, "source_id should be 1");
        assert_eq!(bytes[1], 0x01, "signal_id LE low byte = 0x01 (HR = 0x0101)");
        assert_eq!(bytes[2], 0x01, "signal_id LE high byte = 0x01 (HR = 0x0101)");
    }

    // ── extract_signal_values ─────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-003
    /// Title: Test signal value extraction uses IDT signal IDs
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
        assert!(values.iter().any(|(id, _)| *id == SignalId::HR.as_u16()),
            "HR should have IDT signal_id 0x0101 = {}", SignalId::HR.as_u16());
        assert!(values.iter().any(|(id, _)| *id == SignalId::SpO2.as_u16()),
            "SpO2 should have IDT signal_id 0x0102 = {}", SignalId::SpO2.as_u16());
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-004
    /// Title: Test room filtering (only room_index=0 is processed)
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
        assert!((values[0].1 - 75.0f32).abs() < f32::EPSILON);
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
                create_test_track("SpO2", 98.0, 0, "BED_01"), // note: SPO2 after toUpper
                create_test_track("TEMPERATURE", 37.0, 0, "BED_01"),
            ],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);
        // "SpO2".to_uppercase() = "SPO2" which matches the map key "SPO2"
        assert_eq!(values.len(), 3);
    }

    // ── Session state helpers ─────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-006
    /// Title: Test ACK dispatch purges retransmit buffer
    #[tokio::test]
    async fn test_write_handler_ack_dispatch() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        let stream_id;
        {
            let mut st = state.write().await;
            stream_id = st.subscribe(SignalId::HR.as_u16());
            st.add_data(SignalId::HR.as_u16(), 75.0, 0);
            st.add_data(SignalId::HR.as_u16(), 76.0, 1000);
            st.add_data(SignalId::HR.as_u16(), 77.0, 2000);
            assert_eq!(st.get_pending_count(SignalId::HR.as_u16()), 3);
        }

        // Build a valid 24-byte IDT ACK_FRAME acknowledging seq 1 and 2
        let ack_bytes = make_ack_bytes(1, stream_id, 2);
        let ack = AckFrame::from_ble_bytes(&ack_bytes).unwrap();

        {
            let mut st = state.write().await;
            st.handle_ack(ack.header.session_id, ack.header.stream_id, ack.ack_upto);
            // Only seq 3 should remain
            assert_eq!(st.get_pending_count(SignalId::HR.as_u16()), 1);
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-007
    /// Title: Test subscribe/unsubscribe via IDT signal IDs
    #[tokio::test]
    async fn test_subscribe_unsubscribe_via_signal_id() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16()); // 0x0101
            assert!(st.is_subscribed(SignalId::HR.as_u16()));
        }
        {
            let mut st = state.write().await;
            st.unsubscribe(SignalId::HR.as_u16());
            assert!(!st.is_subscribed(SignalId::HR.as_u16()));
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-008
    /// Title: Test SUBSCRIBE_REQ IDT payload parsing
    ///
    /// Description: A valid IDT SUBSCRIBE_REQ for SpO2 (0x0102) shall be parsed
    ///              correctly by InboundFrame dispatcher.
    #[test]
    fn test_subscribe_payload_parsing() {
        let buf = make_subscribe_req_bytes(1, 1, SUB_OP_SUBSCRIBE, SignalId::SpO2.as_u16());
        match InboundFrame::from_ble_bytes(&buf) {
            Some(InboundFrame::SubscribeReq(req)) => {
                assert_eq!(req.items[0].signal_id, SignalId::SpO2.as_u16()); // 0x0102
                assert_eq!(req.op, SUB_OP_SUBSCRIBE);
            }
            _ => panic!("Expected InboundFrame::SubscribeReq"),
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-009
    /// Title: Test NACK triggers retransmit with FLAG_RETRANSMIT
    #[tokio::test]
    async fn test_nack_triggers_retransmit() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        let stream_id;
        {
            let mut st = state.write().await;
            stream_id = st.subscribe(SignalId::HR.as_u16());
            st.add_data(SignalId::HR.as_u16(), 70.0, 0);
            st.add_data(SignalId::HR.as_u16(), 71.0, 1000);
            st.add_data(SignalId::HR.as_u16(), 72.0, 2000);
        }

        let retransmits = {
            let st = state.read().await;
            st.handle_nack(stream_id, &[2])
        };

        assert_eq!(retransmits.len(), 1);
        assert_eq!(retransmits[0].header.seq, 2);
        assert_ne!(
            retransmits[0].header.flags & FLAG_RETRANSMIT,
            0,
            "FLAG_RETRANSMIT must be set on retransmitted frame"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-010
    /// Title: Test SUBSCRIBE_RSP is built correctly for Data_OUT
    ///
    /// Description: SubscribeRsp::to_ble_bytes() shall produce a valid IDT frame
    ///              with MSG_SUBSCRIBE_RSP (0x02) and correct IDT magic.
    #[test]
    fn test_subscribe_rsp_sent_on_data_out() {
        let rsp = SubscribeRsp {
            session_id: 1,
            req_id: 42,
            status: 0,
            results: vec![SubscribeRspItem {
                source_id: 1,
                signal_id: SignalId::HR.as_u16(), // 0x0101
                stream_id: 1,
                effective_period_ms: 1000,
                effective_batch_max: 1,
            }],
        };
        let bytes = rsp.to_ble_bytes();
        assert_eq!(
            u16::from_le_bytes([bytes[0], bytes[1]]),
            IDT_MAGIC,
            "SUBSCRIBE_RSP must start with IDT_MAGIC"
        );
        assert_eq!(bytes[3], MSG_SUBSCRIBE_RSP, "msg_type must be 0x02 (SUBSCRIBE_RSP)");
        // Size: header(16) + req_id(2)+status(1)+n(1) + result(10) + crc(4) = 34
        assert_eq!(bytes.len(), 34);
    }
}
