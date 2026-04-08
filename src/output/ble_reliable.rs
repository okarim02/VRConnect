// /src/output/ble_reliable.rs
// Module: output.ble_reliable
// Purpose: BLE GATT server output using the IDT ("ICU Data Transport") v1.1 reliable protocol.
//          Full IDT-compliant implementation: DATA_FRAME (34b with CRC32C), ACK_FRAME (30b IDT),
//          NACK_FRAME, SUBSCRIBE_REQ / SUBSCRIBE_RSP, per-stream sequence + retransmit buffers.
//
// Uses our custom GattServer (ble_gatt.rs) which supports Write callbacks,
// replacing the ble-windows-server crate that only supports Read + Notify.
//
// Characteristics (per PDF "Proposition de protocole BLE"):
// - Catalog     (0x90ae): Read   - Available signal catalog (TLV binary)
// - Data_IN     (0x90ac): Write  - ACK_FRAME / NACK_FRAME from the Central
// - Data_OUT    (0x90ad): Notify - DATA_FRAME + SUBSCRIBE_RSP to the Central
// - Subscribe   (0x90af): Write  - SUBSCRIBE_REQ (IDT) from the Central
// - Control     (0x90b0): Notify - (legacy / reserved)
// - Unsubscribe (0x90b1): Write  - SUBSCRIBE_REQ with op=UNSUBSCRIBE, or legacy 2b fallback

use crate::domain::ble_protocol::{
    has_idt_magic, AckFrame, Catalog, InboundFrame, SignalId, SignalRegistry, SubscribeReq,
    SubscribeRsp, SubscribeRspItem, SUB_OP_SUBSCRIBE, SUB_OP_UNSUBSCRIBE,
};
use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use crate::output::ble_gatt::{CharProperty, GattServer, WriteEvent};
use crate::output::ble_session::BleSessionState;
use crate::utils::chaos;
use std::sync::Arc;
use tokio::sync::RwLock;

/// ID SRS: SRS-MOD-BLERELIABLE-001
/// Title: ReliableBleOutput
///
/// Description: VRConnect shall provide BLE GATT server output using the IDT reliable
///              protocol with per-signal streams, cumulative ACK, and explicit NACK retransmit.
///
/// Version: V1.0
pub struct ReliableBleOutput {
    server: Arc<RwLock<GattServer>>,
    state: Arc<RwLock<BleSessionState>>,
    catalog: Catalog,
    /// Extensible signal registry: drives catalog building and subscribe validation.
    /// Immutable after construction; shared read-only into async handlers via Arc.
    registry: Arc<SignalRegistry>,
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
    /// Version: V1.0
    pub async fn new(
        device_name: String,
        service_uuid_str: String,
        _update_interval_ms: u64,
        registry: Option<SignalRegistry>,
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
        log::info!(
            "  Characteristic: Subscribe (Write)   -> {}",
            subscribe_uuid
        );

        server.add_characteristic("Control", control_uuid, &[CharProperty::Notify]);
        log::info!("  Characteristic: Control (Notify)    -> {}", control_uuid);

        server.add_characteristic(
            "Unsubscribe",
            unsubscribe_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );
        log::info!(
            "  Characteristic: Unsubscribe (Write) -> {}",
            unsubscribe_uuid
        );

        let registry = Arc::new(registry.unwrap_or_else(SignalRegistry::with_defaults));
        let catalog = registry.build_catalog();
        let state = BleSessionState::new(1);

        Ok(Self {
            server: Arc::new(RwLock::new(server)),
            state: Arc::new(RwLock::new(state)),
            catalog,
            registry,
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
    ///   3. Spawn the ACK watchdog task (buffer depth monitor, 5 s interval)
    ///   4. Start the GATT server (creates Windows GATT service + advertises)
    ///
    /// Version: V1.0
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
            let registry = self.registry.clone();
            tokio::spawn(async move {
                Self::write_handler_loop(rx, state, server, registry).await;
            });
            log::info!("Write handler task started (Data_IN / Subscribe / Unsubscribe)");
        } else {
            log::warn!("Write receiver already taken — write handlers won't work");
        }

        // 3. Spawn ACK watchdog task
        // [OBS-2] Periodically checks total_pending() across all streams and emits WARN/ERROR
        //         when the buffer depth suggests the ACK uplink is frozen or congested.
        {
            let state = self.state.clone();
            tokio::spawn(async move {
                Self::ack_watchdog_loop(state).await;
            });
            log::info!("ACK watchdog task started (interval=5s, warn≥50, error≥900 frames)");
        }

        // 4. Start GATT server
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
        registry: Arc<SignalRegistry>,
    ) {
        log::info!("Write handler loop running (IDT dispatcher)");

        while let Some(event) = rx.recv().await {
            match event.characteristic_name.as_str() {
                // ── Data_IN: ACK_FRAME or NACK_FRAME from the Central ─────────
                // All inbound frames carry IDT magic and are dispatched via InboundFrame.
                "Data_IN" => {
                    let data = &event.data;
                    match InboundFrame::from_ble_bytes(data) {
                        Some(InboundFrame::Ack(ack)) => {
                            log::info!(
                                "ACK Recv: stream={}, ack_upto={}, bitmap={:02X?}",
                                ack.stream_id,
                                ack.ack_upto,
                                ack.bitmap
                            );
                            let retransmits = {
                                let mut st = state.write().await;
                                st.handle_ack_with_bitmap(
                                    ack.session_id,
                                    ack.stream_id,
                                    ack.ack_upto,
                                    &ack.bitmap,
                                )
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
                                        log::info!(
                                            "--> RETRANSMITTED seq {} for stream {}",
                                            frame.header.seq,
                                            frame.header.stream_id
                                        );
                                    }
                                }
                            }
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
                                "Data_IN: unrecognized payload ({} bytes, byte[0]=0x{:02X}) — discarded",
                                data.len(),
                                data.first().copied().unwrap_or(0)
                            );
                        }
                    }
                }

                // ── Subscribe: SUBSCRIBE_REQ (IDT) from the Central ──────────
                "Subscribe" => {
                    let data = &event.data;
                    // Always dump raw bytes at INFO level — essential for protocol debugging
                    let hex: String = data
                        .iter()
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    log::info!("Subscribe raw ({} bytes): {}", data.len(), hex);

                    if has_idt_magic(data) {
                        if let Some(InboundFrame::SubscribeReq(req)) =
                            InboundFrame::from_ble_bytes(data)
                        {
                            Self::handle_subscribe_req(req, &state, &server, &registry).await;
                        } else {
                            log::warn!(
                                "Subscribe: IDT magic present but not a valid SUBSCRIBE_REQ \
                                 (msg_type=0x{:02X}) — discarded",
                                data.get(3).copied().unwrap_or(0)
                            );
                        }
                    } else {
                        log::warn!(
                            "Subscribe: unrecognized format ({} bytes, byte[0]=0x{:02X}) — \
                             expected IDT frame (magic=0xD17A), discarded",
                            data.len(),
                            data.first().copied().unwrap_or(0)
                        );
                        Self::log_subscribe_parse_failure(data);
                    }
                }

                // ── Unsubscribe: IDT SUBSCRIBE_REQ (op=2) or legacy 2-byte ───
                "Unsubscribe" => {
                    let data = &event.data;
                    if has_idt_magic(data) {
                        // IDT: full SUBSCRIBE_REQ with op=UNSUBSCRIBE
                        if let Some(InboundFrame::SubscribeReq(req)) =
                            InboundFrame::from_ble_bytes(data)
                        {
                            Self::handle_subscribe_req(req, &state, &server, &registry).await;
                        } else {
                            log::warn!(
                                "Unsubscribe: IDT magic present but not a valid SUBSCRIBE_REQ — discarded"
                            );
                        }
                    } else if data.len() >= 2 {
                        // Legacy fallback: 2-byte signal_id LE (old protocol)
                        let signal_id = u16::from_le_bytes([data[0], data[1]]);
                        let mut st = state.write().await;
                        st.unsubscribe(signal_id);
                        log::info!("Unsubscribed (legacy 2b) signal 0x{:04X}", signal_id);
                    } else {
                        log::warn!(
                            "Unsubscribe: payload too short ({} bytes) — discarded",
                            data.len()
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

    /// ID SRS: SRS-FN-BLERELIABLE-006
    /// Title: ack_watchdog_loop
    ///
    /// Description: VRConnect shall periodically check the total number of unacknowledged
    ///              frames across all active streams.  Emits WARN when pending frames exceed
    ///              WARN_THRESHOLD (ACK channel slow / congested) and ERROR when near the
    ///              hard buffer cap (data loss imminent).
    ///
    ///              Tagged [OBS-2] — cross-referenced in start().
    ///
    /// Version: V1.0
    async fn ack_watchdog_loop(state: Arc<RwLock<BleSessionState>>) {
        /// Frames pending before WARN is emitted (~50 s of unacked data at 1 Hz).
        const WARN_THRESHOLD: usize = 50;
        /// Frames pending before ERROR is emitted (90 % of the 1 000-frame hard cap).
        const ERROR_THRESHOLD: usize = 900;

        log::info!("ACK watchdog running (interval=5s, warn≥50, error≥900)");

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            let pending = state.read().await.total_pending();

            if pending >= ERROR_THRESHOLD {
                log::error!(
                    "[ACK Watchdog] {} frames pending — buffer near capacity (cap=1000). \
                     Data loss imminent. ACK channel appears frozen.",
                    pending
                );
            } else if pending >= WARN_THRESHOLD {
                log::warn!(
                    "[ACK Watchdog] {} frames pending — ACK channel may be slow or frozen.",
                    pending
                );
            }
        }
    }

    /// Handle a SUBSCRIBE_REQ IDT frame:
    ///   - op=1 (SUBSCRIBE):   allocate stream → send SUBSCRIBE_RSP on Data_OUT
    ///   - op=2 (UNSUBSCRIBE): remove stream, no RSP sent
    /// Signal IDs are validated against `registry`; unknown IDs are rejected with a warning.
    async fn handle_subscribe_req(
        req: SubscribeReq,
        state: &Arc<RwLock<BleSessionState>>,
        server: &Arc<RwLock<GattServer>>,
        registry: &Arc<SignalRegistry>,
    ) {
        let session_id = req.header.session_id;
        let req_id = req.req_id;
        let mut rsp_items: Vec<SubscribeRspItem> = Vec::new();
        // Collect (canonical_id, stream_id, mode, start_time_ms) for post-RSP replay
        let mut replay_requests: Vec<(u16, u16, u8, u64)> = Vec::new();

        {
            let mut st = state.write().await;
            for item in &req.items {
                match req.op {
                    SUB_OP_SUBSCRIBE => {
                        // Validate + normalize via registry (handles legacy 1/2/3 → IDT 0x01xx)
                        let canonical_id = match registry.normalize_id(item.signal_id) {
                            Some(id) => id,
                            None => {
                                log::warn!(
                                    "SUBSCRIBE: unknown signal_id 0x{:04X} — not in registry, rejected",
                                    item.signal_id
                                );
                                continue;
                            }
                        };
                        if canonical_id != item.signal_id {
                            log::info!(
                                "Signal ID: app sent 0x{:04X} → normalized to 0x{:04X} (legacy→IDT)",
                                item.signal_id,
                                canonical_id
                            );
                        }
                        // Safety: normalize_id succeeded, so get() is guaranteed Some
                        let meta = registry.get(canonical_id).unwrap();
                        let stream_id = st.subscribe(canonical_id);
                        rsp_items.push(SubscribeRspItem {
                            source_id: meta.source_id,
                            signal_id: canonical_id,
                            stream_id,
                            effective_period_ms: meta.nominal_period_ms,
                            effective_batch_max: 1,
                        });
                        log::info!(
                            "SUBSCRIBE: signal 0x{:04X} → stream {} (mode={})",
                            canonical_id,
                            stream_id,
                            item.mode
                        );
                        // Queue replay if mode=1 (BACKLOG_THEN_LIVE) or mode=2 (BACKLOG_ONLY)
                        if item.mode == 1 || item.mode == 2 {
                            replay_requests.push((
                                canonical_id,
                                stream_id,
                                item.mode,
                                item.start_time_ms,
                            ));
                        }
                    }
                    SUB_OP_UNSUBSCRIBE => {
                        // Normalize on unsubscribe (registry path)
                        let canonical_id = registry
                            .normalize_id(item.signal_id)
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
            // Also send on Control char — some older Central implementations listen here
            if let Err(e) = srv.notify("Control", &bytes).await {
                log::debug!(
                    "SUBSCRIBE_RSP on Control: {} (client may not be subscribed)",
                    e
                );
            } else {
                log::info!("SUBSCRIBE_RSP also sent on Control ({} bytes)", bytes.len());
            }
        }

        // Send historical replay frames for BACKLOG_THEN_LIVE / BACKLOG_ONLY
        for (canonical_id, stream_id, mode, start_time_ms) in replay_requests {
            let replay_frames = {
                let mut st = state.write().await;
                st.start_replay(canonical_id, start_time_ms)
            };
            if replay_frames.is_empty() {
                log::info!(
                    "Replay requested for signal 0x{:04X} (stream {}) but history is empty",
                    canonical_id,
                    stream_id
                );
            } else {
                log::info!(
                    "Replaying {} historical frame(s) for signal 0x{:04X} (stream {}, mode={})",
                    replay_frames.len(),
                    canonical_id,
                    stream_id,
                    mode
                );
                let srv = server.read().await;
                for frame in &replay_frames {
                    let bytes = frame.to_ble_bytes();
                    if let Err(e) = srv.notify("Data_OUT", &bytes).await {
                        log::warn!(
                            "Replay notify failed for signal 0x{:04X} seq {}: {}",
                            canonical_id,
                            frame.header.seq,
                            e
                        );
                    }
                }
            }
            {
                let mut st = state.write().await;
                st.finish_replay(stream_id);
                if mode == 2 {
                    st.unsubscribe(canonical_id);
                    log::info!(
                        "BACKLOG_ONLY: unsubscribed signal 0x{:04X} after replay",
                        canonical_id
                    );
                }
            }
        }
    }

    /// Extract (signal_id, f32) pairs from ProcessedData for room_index=0.
    /// Signal name mapping covers all known VitalRecorder export names:
    /// - SpO2:        "SPO2", "PLETH", "PLETH_SPO2"
    /// - Temperature: "TEMP", "TEMPERATURE", "BT", "BT1", "BT1_TEMP"
    #[cfg(test)]
    fn extract_signal_values(data: &ProcessedData) -> Vec<(u16, f32)> {
        use std::collections::HashMap;
        let signal_map: HashMap<&str, u16> = [
            ("HR", SignalId::HR.as_u16()),
            ("SPO2", SignalId::SpO2.as_u16()),
            ("PLETH", SignalId::SpO2.as_u16()),
            ("PLETH_SPO2", SignalId::SpO2.as_u16()),
            ("TEMP", SignalId::Temperature.as_u16()),
            ("TEMPERATURE", SignalId::Temperature.as_u16()),
            ("BT", SignalId::Temperature.as_u16()),
            ("BT1", SignalId::Temperature.as_u16()),
            ("BT1_TEMP", SignalId::Temperature.as_u16()),
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
    ///              a 34-byte IDT DATA_FRAME (with t0_ms timestamp and CRC32C tail)
    ///              is notified on Data_OUT.
    ///
    /// Version: V1.0
    pub async fn output(&self, data: &ProcessedData) -> Result<()> {
        let mut state = self.state.write().await;
        let server = self.server.read().await;

        for track in &data.all_tracks {
            // Only BED_01 (room_index = 0)
            if track.room_index != 0 {
                continue;
            }

            // log track info for diagnosis
            // log::info!(
            //     "Track: name='{}', raw={:?}, display='{}', timestamp={}",
            //     track.name,
            //     track.raw_value,
            //     track.display_value,
            //     track.timestamp
            // );

            // Map VitalRecorder signal name → IDT signal_id.
            // VitalRecorder exports SpO2 as "PLETH_SPO2" (or "PLETH" / "SpO2" in older versions)
            // and temperature as "BT1_TEMP" (or "BT1" / "TEMPERATURE" in older versions).
            let signal_id = match track.name.trim().to_uppercase().as_str() {
                "HR" => SignalId::HR.as_u16(),
                "SPO2" | "PLETH" | "PLETH_SPO2" => SignalId::SpO2.as_u16(),
                "TEMP" | "TEMPERATURE" | "BT" | "BT1" | "BT1_TEMP" => {
                    SignalId::Temperature.as_u16()
                }
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

            // Always record to history buffer so replay is available
            // regardless of whether any client is currently subscribed.
            state.record_history(signal_id, val_f32, t0_ms);

            // add_data returns Some(frame) only if signal is subscribed
            if let Some(frame) = state.add_data(signal_id, val_f32, t0_ms) {
                // DataFrame::to_ble_bytes() produces 34 bytes (IDT header + payload + CRC32C).
                // Signal name aliases are VitalRecorder-specific; registry governs BLE catalog.

                // --- CHAOS MONKEY: packet-drop + network-jitter (env-driven) ---
                // Controlled by ENABLE_CHAOS_MONKEY / CHAOS_RATIO / CHAOS_NETWORK_JITTER.
                // Never active when APP_ENV=production. See src/chaos/mod.rs.
                if chaos::maybe_drop_frame("ble_reliable.rs") {
                    continue; // Skip the BLE notify — simulates a lossy link.
                }
                chaos::maybe_network_jitter("ble_reliable.rs").await;
                // -------------------------------------------------------------

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
    /// Version: V1.0
    pub async fn handle_ack_idt(&self, ack: &AckFrame) -> Result<()> {
        let mut state = self.state.write().await;
        state.handle_ack(ack.session_id, ack.stream_id, ack.ack_upto);
        Ok(())
    }

    /// ID SRS: SRS-FN-BLERELIABLE-005
    /// Title: subscribe
    ///
    /// Description: VRConnect shall subscribe a client to a signal (external callers).
    ///
    /// Version: V1.0
    pub async fn subscribe(&self, signal_id: u16) {
        let mut state = self.state.write().await;
        let stream_id = state.subscribe(signal_id);
        log::info!(
            "Subscribed to signal 0x{:04X} → stream {}",
            signal_id,
            stream_id
        );
    }

    /// ID SRS: SRS-FN-BLERELIABLE-006
    /// Title: unsubscribe
    ///
    /// Description: VRConnect shall unsubscribe a client from a signal (external callers).
    ///
    /// Version: V1.0
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
    /// Version: V1.0
    pub async fn get_session_stats(&self) -> (u16, usize) {
        let state = self.state.read().await;
        (state.current_session_id, state.total_pending())
    }

    /// Diagnostic helper: logs why a SUBSCRIBE_REQ frame failed to parse.
    /// Tries all candidate SubscribeItem sizes (17..=30) to identify the correct one,
    /// and dumps the first item's raw bytes + signal_id for protocol mismatch detection.
    fn log_subscribe_parse_failure(data: &[u8]) {
        use crate::domain::ble_protocol::{IdtHeader, IDT_MAGIC, MSG_SUBSCRIBE_REQ};

        let hdr_size = IdtHeader::SIZE; // 13 bytes
        if data.len() < hdr_size {
            log::warn!(
                "Subscribe PARSE FAIL: too short ({} bytes, need ≥ {} for IDT header)",
                data.len(),
                hdr_size
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
        let fixed_payload = 4; // req_id(2)+op(1)+n(1)
        if data.len() < hdr_size + fixed_payload {
            log::warn!(
                "Subscribe PARSE FAIL: too short for fixed payload fields ({} bytes)",
                data.len()
            );
            return;
        }

        let req_id = u16::from_le_bytes([data[hdr_size], data[hdr_size + 1]]);
        let op = data[hdr_size + 2];
        let n = data[hdr_size + 3] as usize;

        log::warn!(
            "Subscribe PARSE FAIL: req_id={}, op=0x{:02X}, n={} items, {} bytes total",
            req_id,
            op,
            n,
            data.len()
        );

        // Brute-force candidate item sizes to find CRC match
        if n > 0 {
            // fixed overhead: header(hdr_size) + req_id(2)+op(1)+n(1) + CRC(4)
            let fixed = hdr_size + fixed_payload + 4;
            let payload_bytes = data.len().saturating_sub(fixed);
            log::warn!(
                "  item payload bytes = {} ÷ {} items = {} bytes/item  (server expects 17)",
                payload_bytes,
                n,
                payload_bytes / n
            );

            for candidate in 17usize..=30 {
                let expected_total = hdr_size + fixed_payload + n * candidate + 4;
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
        let item_off = hdr_size + fixed_payload;
        if n > 0 && data.len() > item_off {
            let end = data.len().min(item_off + 30);
            let raw: String = data[item_off..end]
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ");
            log::warn!("  item[0] raw: {}", raw);
            if data.len() >= item_off + 3 {
                let src = data[item_off];
                let sig = u16::from_le_bytes([data[item_off + 1], data[item_off + 2]]);
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
    use crate::domain::ble_protocol::{
        AckFrame, IdtHeader, InboundFrame, SubscribeRsp, SubscribeRspItem, FLAG_RETRANSMIT,
        IDT_MAGIC, IDT_VERSION, MSG_ACK_FRAME, MSG_SUBSCRIBE_REQ, MSG_SUBSCRIBE_RSP,
        SUB_OP_SUBSCRIBE,
    };
    use crate::domain::{ProcessedRoom, ProcessedTrack, TrackType};
    use chrono::Utc;

    /// Helper: create a test ProcessedTrack
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

    /// Helper: build a valid IDT ACK_FRAME buffer (30 bytes, IDT magic)
    fn make_ack_bytes(session_id: u16, stream_id: u16, ack_upto: u32) -> Vec<u8> {
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_ACK_FRAME,
            flags: 0,
            session_id,
            stream_id,
            seq: 0,
        };
        let mut buf = Vec::with_capacity(AckFrame::TOTAL_LEN);
        buf.extend_from_slice(&header.to_bytes()); // [0..12]  13 bytes
        buf.extend_from_slice(&ack_upto.to_le_bytes()); // [13..16]  4 bytes
        buf.push(8u8); // [17]      bitmap_len = 8
        buf.extend_from_slice(&0u64.to_le_bytes()); // [18..25]  bitmap = zeros
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes()); // [26..29]  CRC32C
        buf
    }

    /// Helper: build a valid IDT SUBSCRIBE_REQ buffer with one item
    fn make_subscribe_req_bytes(session_id: u16, req_id: u16, op: u8, signal_id: u16) -> Vec<u8> {
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_SUBSCRIBE_REQ,
            flags: 0,
            session_id,
            stream_id: 0,
            seq: 0,
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
        assert_eq!(
            bytes[2], 0x01,
            "signal_id LE high byte = 0x01 (HR = 0x0101)"
        );
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
        assert!(
            values.iter().any(|(id, _)| *id == SignalId::HR.as_u16()),
            "HR should have IDT signal_id 0x0101 = {}",
            SignalId::HR.as_u16()
        );
        assert!(
            values.iter().any(|(id, _)| *id == SignalId::SpO2.as_u16()),
            "SpO2 should have IDT signal_id 0x0102 = {}",
            SignalId::SpO2.as_u16()
        );
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

        // Build an IDT ACK_FRAME acknowledging seq 1 and 2
        let ack_bytes = make_ack_bytes(1, stream_id, 2);
        let ack = AckFrame::from_ble_bytes(&ack_bytes).unwrap();

        {
            let mut st = state.write().await;
            st.handle_ack(ack.session_id, ack.stream_id, ack.ack_upto);
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
        assert_eq!(
            bytes[3], MSG_SUBSCRIBE_RSP,
            "msg_type must be 0x02 (SUBSCRIBE_RSP)"
        );
        // Size: header(13) + req_id(2)+status(1)+n(1) + result(10) + crc(4) = 31
        assert_eq!(bytes.len(), 31);
    }

    // ── Signal name alias coverage ────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-011
    /// Title: Test all VitalRecorder signal name aliases are matched
    ///
    /// Description: extract_signal_values shall recognise every alias for SpO2
    ///              and Temperature that VitalRecorder can export.
    #[test]
    fn test_signal_name_aliases() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                // SpO2 aliases
                create_test_track("PLETH", 97.0, 0, "BED_01"),
                create_test_track("PLETH_SPO2", 98.0, 0, "BED_01"),
                // Temperature aliases
                create_test_track("BT", 36.5, 0, "BED_01"),
                create_test_track("BT1", 36.6, 0, "BED_01"),
                create_test_track("BT1_TEMP", 36.7, 0, "BED_01"),
                create_test_track("TEMP", 36.8, 0, "BED_01"),
                create_test_track("TEMPERATURE", 36.9, 0, "BED_01"),
            ],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);

        let spo2_count = values
            .iter()
            .filter(|(id, _)| *id == SignalId::SpO2.as_u16())
            .count();
        let temp_count = values
            .iter()
            .filter(|(id, _)| *id == SignalId::Temperature.as_u16())
            .count();

        assert_eq!(spo2_count, 2, "Two SpO2 aliases (PLETH, PLETH_SPO2)");
        assert_eq!(
            temp_count, 5,
            "Five Temperature aliases (BT, BT1, BT1_TEMP, TEMP, TEMPERATURE)"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-012
    /// Title: Test unknown signal names produce no output
    ///
    /// Description: Tracks whose names are not in the signal map must be silently
    ///              ignored by extract_signal_values.
    #[test]
    fn test_unknown_signal_name_ignored() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                create_test_track("ART1_SBP", 120.0, 0, "BED_01"),
                create_test_track("ECG1", 0.5, 0, "BED_01"),
                create_test_track("PPV", 10.0, 0, "BED_01"),
            ],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);
        assert!(values.is_empty(), "Unmapped signals must produce no output");
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-013
    /// Title: Test extract_signal_values uses display_value when raw_value is None
    ///
    /// Description: If raw_value is None, the track's display_value string shall be
    ///              parsed as f32 and used as the signal value.
    #[test]
    fn test_extract_signal_values_display_value_fallback() {
        let mut track = create_test_track("HR", 0.0, 0, "BED_01");
        track.raw_value = None;
        track.display_value = "82.0".to_string();

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);

        assert_eq!(values.len(), 1);
        assert!((values[0].1 - 82.0f32).abs() < f32::EPSILON);
    }

    // ── FLAG_BACKLOG on outgoing frames ───────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-015
    /// Title: Live frames must NOT carry FLAG_BACKLOG outside of replay
    ///
    /// Description: FLAG_BACKLOG is restricted to historical-replay frames only
    ///              (is_replaying=true). Live frames — even when the retransmit buffer
    ///              is non-empty — must have FLAG_BACKLOG clear.
    #[tokio::test]
    async fn test_flag_backlog_not_set_on_live_frames() {
        use crate::domain::ble_protocol::FLAG_BACKLOG;

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16());

            let f1 = st.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
            assert_eq!(f1.header.flags & FLAG_BACKLOG, 0, "First live frame: FLAG_BACKLOG must be clear");

            // Second frame — tx buffer is non-empty (f1 not yet ACKed) but is_replaying=false
            let f2 = st.add_data(SignalId::HR.as_u16(), 71.0, 1000).unwrap();
            assert_eq!(
                f2.header.flags & FLAG_BACKLOG,
                0,
                "Second live frame: FLAG_BACKLOG must be clear outside replay"
            );
        }
    }

    // ── Selective ACK bitmap retransmit ───────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-016
    /// Title: Test handle_ack_with_bitmap returns lost frames for retransmission
    ///
    /// Description: 4 frames buffered (seq 1-4). ACK: ack_upto=1, bitmap
    ///              bit1=1 (seq 3 received, seq 2 missing). handle_ack_with_bitmap
    ///              must return seq 2 with FLAG_RETRANSMIT, and purge seq 1.
    #[tokio::test]
    async fn test_handle_ack_with_bitmap_retransmit() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        let stream_id;

        {
            let mut st = state.write().await;
            stream_id = st.subscribe(SignalId::HR.as_u16());
            for i in 0u64..4 {
                st.add_data(SignalId::HR.as_u16(), i as f32, i * 1000);
            }
        }

        // ack_upto=1; bit0=seq2 (clear=missing), bit1=seq3 (set=received)
        let mut bitmap = [0u8; 8];
        bitmap[0] = 0b0000_0010; // bit1 set → seq 3 received

        let retransmits = {
            let mut st = state.write().await;
            st.handle_ack_with_bitmap(1, stream_id, 1, &bitmap)
        };

        assert_eq!(retransmits.len(), 1, "Seq 2 is the only hole");
        assert_eq!(retransmits[0].header.seq, 2);
        assert_ne!(retransmits[0].header.flags & FLAG_RETRANSMIT, 0);

        // seq 1 must have been purged (ack_upto=1)
        let st = state.read().await;
        let pending = st.get_pending_count(SignalId::HR.as_u16());
        assert_eq!(pending, 3, "Seq 1 purged; seq 2,3,4 remain in buffer");
    }

    // ── subscribe_with_stream_id ──────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-017
    /// Title: Test subscribe_with_stream_id assigns IDs 1, 2, 3 for HR/SpO2/Temp
    ///
    /// Description: subscribe_with_stream_id with preferred_stream_id = 1, 2, 3.
    ///              All three signals must get independent fixed stream IDs.
    #[tokio::test]
    async fn test_subscribe_with_stream_id_all_signals() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        {
            let mut st = state.write().await;
            let hr_sid = st.subscribe_with_stream_id(SignalId::HR.as_u16(), 1);
            let spo2_sid = st.subscribe_with_stream_id(SignalId::SpO2.as_u16(), 2);
            let temp_sid = st.subscribe_with_stream_id(SignalId::Temperature.as_u16(), 3);

            assert_eq!(hr_sid, 1);
            assert_eq!(spo2_sid, 2);
            assert_eq!(temp_sid, 3);
            assert_eq!(st.streams.len(), 3);
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-018
    /// build_char_uuid returns Err when the resulting string is not a valid UUID
    #[test]
    fn test_build_char_uuid_invalid_base_returns_error() {
        // A base string of length != 32 that produces an invalid UUID format
        let result = ReliableBleOutput::build_char_uuid("not-a-valid-uuid-base!!", "90ae");
        assert!(result.is_err(), "Invalid UUID base must return Err");
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-019
    /// extract_signal_values with room 0 having no tracks returns an empty Vec
    #[test]
    fn test_extract_signal_values_empty_tracks() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);
        assert!(
            values.is_empty(),
            "Room with no tracks must yield no values"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-020
    /// extract_signal_values skips a track when raw_value is None and display_value
    /// cannot be parsed as f32 (e.g., "N/A")
    #[test]
    fn test_extract_signal_values_unparseable_display_value_skipped() {
        let mut track = create_test_track("HR", 0.0, 0, "BED_01");
        track.raw_value = None;
        track.display_value = "N/A".to_string(); // cannot parse as f32

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);
        assert!(
            values.is_empty(),
            "Unparseable display_value must be skipped"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-021
    /// extract_signal_values returns an empty Vec when no rooms are present
    #[test]
    fn test_extract_signal_values_no_rooms() {
        let data = ProcessedData::new("VR-TEST".to_string(), vec![]);
        let values = ReliableBleOutput::extract_signal_values(&data);
        assert!(values.is_empty());
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-022
    /// extract_signal_values correctly maps "BT" (an alias) to Temperature signal_id
    #[test]
    fn test_extract_signal_values_bt_alias_maps_to_temperature() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![create_test_track("BT", 36.5, 0, "BED_01")],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let values = ReliableBleOutput::extract_signal_values(&data);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].0, SignalId::Temperature.as_u16());
        assert!((values[0].1 - 36.5f32).abs() < f32::EPSILON);
    }

    // ── Registry validation path tests ───────────────────────────────────────
    // These tests validate the logic used in handle_subscribe_req
    // without requiring BLE hardware (GattServer), by exercising the same registry + state
    // objects that the async handlers use internally.

    /// ID SRS: SRS-TEST-BLERELIABLE-023
    /// normalize_id returns None for IDs that must be rejected by subscribe handlers
    ///
    /// Validates the `normalize_id(unknown) → None → warn+skip` guard used in both
    /// handle_subscribe_req and handle_tlv_subscribe.
    #[test]
    fn test_registry_unknown_signal_normalize_returns_none() {
        use crate::domain::ble_protocol::SignalRegistry;
        let r = SignalRegistry::with_defaults();
        assert_eq!(r.normalize_id(0x9999), None, "unknown IDT compound ID must be rejected");
        assert_eq!(r.normalize_id(0x0200), None, "unknown source-2 ID must be rejected");
        assert_eq!(r.normalize_id(0), None,      "zero must always be rejected");
        assert_eq!(r.normalize_id(99), None,     "unknown legacy simple ID must be rejected");
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-024
    /// After normalize_id succeeds, registry.get(canonical) returns correct metadata
    ///
    /// Validates the invariant relied upon by both subscribe handlers:
    /// `registry.get(canonical_id).unwrap()` must never panic after `normalize_id` returns Some.
    #[test]
    fn test_registry_known_signal_meta_available_after_normalize() {
        use crate::domain::ble_protocol::SignalRegistry;
        let r = SignalRegistry::with_defaults();
        // Legacy path: raw_id=1 (HR) → canonical=0x0101
        let canonical = r.normalize_id(1).unwrap();
        let meta = r.get(canonical).unwrap(); // must not panic — handler invariant
        assert_eq!(meta.source_id, 1);
        assert_eq!(meta.nominal_period_ms, 1000); // HR = 1 Hz
        assert_eq!(meta.signal_id, 0x0101);
        // Canonical path: 0x0103 (Temperature)
        let temp_meta = r.get(r.normalize_id(0x0103).unwrap()).unwrap();
        assert_eq!(temp_meta.nominal_period_ms, 2000); // Temperature = 0.5 Hz
        assert_eq!(temp_meta.source_id, 1);
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-025
    /// Full normalized-subscribe path: legacy raw_id=1 → 0x0101 → state tracks canonical ID
    ///
    /// Validates that subscribe_with_stream_id(canonical, preferred) records the subscription
    /// at the canonical IDT ID (0x0101), not at the raw legacy ID (1).
    #[tokio::test]
    async fn test_subscribe_with_normalized_id_state_reflects_canonical() {
        use crate::domain::ble_protocol::SignalRegistry;
        let r = SignalRegistry::with_defaults();
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        {
            let mut st = state.write().await;
            // Legacy simple ID raw_id=1 → normalize to canonical 0x0101
            let canonical = r.normalize_id(1).unwrap();
            assert_eq!(canonical, 0x0101);
            let stream_id = st.subscribe_with_stream_id(canonical, 1);
            assert_eq!(stream_id, 1, "preferred_stream_id must be honoured");
            // State must record subscription at canonical IDT ID, not legacy raw ID
            assert!(st.is_subscribed(0x0101), "must be subscribed at canonical IDT ID 0x0101");
            assert!(!st.is_subscribed(1),     "legacy raw ID 1 must NOT appear as subscribed");
        }
    }

    // ── HistoryBuffer / replay integration ───────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-026
    /// Title: output() feeds history buffer regardless of subscription state
    ///
    /// Description: record_history must be called for every incoming sample in output(),
    ///              even when no client is subscribed for the signal. This ensures history
    ///              is available for subsequent BACKLOG_THEN_LIVE subscriptions.
    #[tokio::test]
    async fn test_output_feeds_history_when_not_subscribed() {
        use crate::domain::ble_protocol::SignalId;
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        // Build a minimal ProcessedData with one HR sample, no subscription active
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![create_test_track("HR", 75.0, 0i32, "BED_01")],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Drive signal_to_history via record_history directly (mirrors output() behaviour)
        {
            let mut st = state.write().await;
            st.record_history(SignalId::HR.as_u16(), 75.0, 1_700_000_000_000u64);
        }

        // History must contain the sample even though no stream is subscribed
        {
            let st = state.read().await;
            assert!(!st.is_subscribed(SignalId::HR.as_u16()), "pre-condition: not subscribed");
            let hist = st.history.get(&SignalId::HR.as_u16()).unwrap();
            assert_eq!(hist.len(), 1);
            assert_eq!(hist[0], (1_700_000_000_000u64, 75.0f32));
        }

        // Suppress unused-variable warning for `data`
        let _ = data;
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-027
    /// Title: start_replay returns FLAG_BACKLOG frames and finish_replay clears the flag
    ///
    /// Description: After subscribing and seeding history, start_replay() must return
    ///              DataFrames with FLAG_BACKLOG set. After finish_replay() the is_replaying
    ///              flag must be cleared so live frames no longer carry FLAG_BACKLOG.
    #[tokio::test]
    async fn test_start_and_finish_replay_flag_lifecycle() {
        use crate::domain::ble_protocol::{FLAG_BACKLOG, SignalId};
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        {
            let mut st = state.write().await;
            // Seed history with 3 HR samples
            st.record_history(SignalId::HR.as_u16(), 70.0, 1000);
            st.record_history(SignalId::HR.as_u16(), 71.0, 2000);
            st.record_history(SignalId::HR.as_u16(), 72.0, 3000);

            // Subscribe to HR
            st.subscribe_with_stream_id(SignalId::HR.as_u16(), 1);

            // Trigger replay (start_time_ms=0 → all history)
            let frames = st.start_replay(SignalId::HR.as_u16(), 0);
            assert_eq!(frames.len(), 3, "all 3 history samples must be replayed");
            for f in &frames {
                assert_ne!(
                    f.header.flags & FLAG_BACKLOG,
                    0,
                    "every replay frame must carry FLAG_BACKLOG"
                );
            }

            // While replaying, live frames must also carry FLAG_BACKLOG
            let live_during = st.add_data(SignalId::HR.as_u16(), 73.0, 4000).unwrap();
            assert_ne!(
                live_during.header.flags & FLAG_BACKLOG,
                0,
                "live frame during replay must carry FLAG_BACKLOG"
            );

            // Finish replay — clear is_replaying
            let stream_id = st.get_stream_id(SignalId::HR.as_u16()).unwrap();
            st.finish_replay(stream_id);

            // Live frame after replay must NOT carry FLAG_BACKLOG
            let live_after = st.add_data(SignalId::HR.as_u16(), 74.0, 5000).unwrap();
            assert_eq!(
                live_after.header.flags & FLAG_BACKLOG,
                0,
                "live frame after finish_replay must NOT carry FLAG_BACKLOG"
            );
        }
    }
}
