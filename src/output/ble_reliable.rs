// /src/output/ble_reliable.rs
// Module: output.ble_reliable
// Purpose: BLE GATT server output using the IDT ("ICU Data Transport") reliable protocol.
//          Flutter-compatible implementation: DATA_FRAME (34b with CRC32C [TODO-1 resolved]),
//          ACK_FRAME (Flutter custom 17b, no IDT magic), NACK_FRAME (IDT),
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
//
// ── Flutter Compatibility Deviations ─────────────────────────────────────────
// The following behaviours intentionally deviate from the IDT spec and are driven
// by current limitations of the Flutter client (flutter_blue_plus / main-central.dart).
// Each deviation is tagged [DEV-x] and cross-referenced in the relevant code section.
//
//   [TODO-1 resolved] DATA_FRAME — CRC32C tail now appended (34 bytes total).
//   [DEV-2] ACK_FRAME — Flutter-custom 17-byte wire format (no IDT magic / header).
//           Flutter sends: [session_id(2)][stream_id(2)][ack_upto(4)][bitmap_len=8(1)][bitmap(8)]
//           The IDT spec expects a full IDT-framed ACK (magic=0xD17A, msg_type=0x20).
//
//   [DEV-3] SUBSCRIBE_REQ — Flutter-custom TLV format (byte[0]=0x20 marker, not IDT magic).
//           The IDT spec expects a full SUBSCRIBE_REQ IDT frame (magic=0xD17A, msg_type=0x01).
//           Both formats are accepted; TLV takes priority via parse_tlv_subscribe_req().
//
//   [DEV-4] SUBSCRIBE_RSP — delayed 300 ms after reception of SUBSCRIBE_REQ.
//           Flutter enables CCCD notifications *after* writing to the Subscribe characteristic,
//           so without the delay the first Notify would be silently dropped by the stack.
//
//   [DEV-5] FLAG_BACKLOG (bit1) — set when the retransmit buffer is non-empty
//           (unacknowledged in-flight frames). The IDT spec reserves this flag exclusively
//           for historical data replay (BACKLOG_THEN_LIVE mode, see TODO-3 below).
//
// ── TODO: full IDT compliance (deferred to v1.1+) ────────────────────────────
//
//   [TODO-1] Resolved — DATA_FRAME CRC32C appended; DataFrame now 34 bytes.
//
//   [TODO-2] ACK_FRAME wire format: switch to standard IDT framing (magic=0xD17A, full header).
//            Requires coordinated update to AckFrame serialisation + Flutter sendAck().
//
//   [TODO-3] History / BACKLOG_THEN_LIVE mode (IDT subscribe mode=1):
//            When a client subscribes with mode=1, replay recent samples from a
//            per-signal ring buffer (HistoryBuffer) before switching to live streaming.
//            Requires: HistoryBuffer struct in domain/, feed from output(), handle mode
//            field in handle_tlv_subscribe / handle_subscribe_req, set FLAG_BACKLOG only
//            during replay. Flutter change: mode byte 0x00 → 0x01 in subscribeStreams().
//
//   [TODO-4] PING/PONG heartbeat (IDT msg_type=0x30 / 0x31):
//            Useful for detecting stale sessions without a full reconnect cycle.
//            Not needed for the current prototype (flutter_blue_plus handles connectivity).
//
//   [TODO-5] STATUS frames (IDT msg_type=0x40):
//            Server-to-client error/state reporting. Not yet implemented.

use crate::domain::ble_protocol::{
    has_idt_magic, parse_tlv_subscribe_req, AckFrame, Catalog, InboundFrame, SignalId,
    SignalRegistry, SubscribeReq, SubscribeRsp, SubscribeRspItem, SUB_OP_SUBSCRIBE,
    SUB_OP_UNSUBSCRIBE,
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
        //         Surfaces Flutter debugFreezeAck / debugDropAck from the Rust log alone,
        //         without requiring any Flutter-side instrumentation.
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
                // ── Data_IN: ACK_FRAME or NACK_FRAME from client ──────────────
                // Magic-based routing:
                //   len==17       → Flutter custom ACK [DEV-2] (no IDT magic)
                //   has_idt_magic → IDT NACK_FRAME (only valid IDT type on this char)
                //   otherwise     → unknown format, discard + warn
                "Data_IN" => {
                    let data = &event.data;
                    if data.len() == 17 {
                        // [DEV-2] Flutter custom ACK — no IDT magic, fixed 17 bytes (TEST)
                        match AckFrame::from_ble_bytes(data) {
                            Some(ack) => {
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
                            None => {
                                log::warn!(
                                    "Data_IN: 17-byte payload did not parse as Flutter ACK — discarded"
                                );
                            }
                        }
                    } else if has_idt_magic(data) {
                        // IDT-framed message on Data_IN — only NACK_FRAME is valid here
                        match InboundFrame::from_ble_bytes(data) {
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
                                    "Data_IN: IDT magic present but not a NACK_FRAME \
                                     (msg_type=0x{:02X}, {} bytes) — discarded",
                                    data.get(3).copied().unwrap_or(0),
                                    data.len()
                                );
                            }
                        }
                    } else {
                        log::warn!(
                            "Data_IN: unrecognized payload ({} bytes, byte[0]=0x{:02X}) — \
                             not a Flutter ACK (17b) or IDT frame (magic=0xD17A), discarded",
                            data.len(),
                            data.first().copied().unwrap_or(0)
                        );
                    }
                }

                // ── Subscribe: SUBSCRIBE_REQ (IDT or Flutter TLV) ────────────
                // Magic-based routing (TLV takes priority per [DEV-3]):
                //   byte[0]==0x20 → Flutter TLV SUBSCRIBE_REQ [DEV-3]; warn on parse fail
                //   has_idt_magic → IDT SUBSCRIBE_REQ; warn + discard if wrong type
                //   otherwise     → unknown format, discard + warn
                // RSP is delayed 300 ms so Flutter has time to enable CCCD [DEV-4].
                "Subscribe" => {
                    let data = &event.data;
                    // Always dump raw bytes at INFO level — essential for protocol debugging
                    let hex: String = data
                        .iter()
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    log::info!("Subscribe raw ({} bytes): {}", data.len(), hex);

                    if data.first() == Some(&0x20) {
                        // [DEV-3] Flutter TLV format — byte[0]=0x20 marker, no IDT magic
                        if let Some((req_id, signal_ids)) = parse_tlv_subscribe_req(data) {
                            log::info!(
                                "TLV subscribe: req_id={}, signals={:?}",
                                req_id,
                                signal_ids
                            );
                            Self::handle_tlv_subscribe(
                                req_id, signal_ids, &state, &server, &registry,
                            )
                            .await;
                        } else {
                            log::warn!(
                                "Subscribe: TLV marker 0x20 present but parse failed ({} bytes) — discarded",
                                data.len()
                            );
                        }
                    } else if has_idt_magic(data) {
                        // IDT SUBSCRIBE_REQ
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
                            "Subscribe: unrecognized format ({} bytes, byte[0]=0x{:02X}) — discarded",
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
    ///              Designed to surface Flutter-side ACK suppression (debugFreezeAck,
    ///              debugDropAck) from the Rust log alone — no Flutter instrumentation needed.
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
                    "[ACK Watchdog] {} frames pending — ACK channel may be \
                     slow or frozen (debugFreezeAck / debugDropAck active?).",
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
                            "SUBSCRIBE: signal 0x{:04X} → stream {}",
                            canonical_id,
                            stream_id
                        );
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
            // Also send on Control char — some Flutter implementations listen here (I.pdf)
            if let Err(e) = srv.notify("Control", &bytes).await {
                log::debug!(
                    "SUBSCRIBE_RSP on Control: {} (client may not be subscribed)",
                    e
                );
            } else {
                log::info!("SUBSCRIBE_RSP also sent on Control ({} bytes)", bytes.len());
            }
        }
    }

    /// Handle a TLV-format SUBSCRIBE_REQ from the Flutter app (non-IDT wire format).
    /// Normalizes legacy signal IDs (1,2,3) to IDT compound IDs (0x0101-0x0103),
    /// subscribes each signal, and sends SUBSCRIBE_RSP on Data_OUT and Control.
    ///
    /// Stream ID assignment: use raw_id directly (1, 2, 3) for the session/DATA_FRAME
    /// stream_id (stored LE → Flutter LE-reads it correctly).
    /// The SUBSCRIBE_RSP result item encodes stream_id.swap_bytes() so that Flutter's
    /// BE-reading of the RSP field also recovers the correct raw_id.
    ///   raw_id=1 → session stream_id=1 → DATA_FRAME LE [0x01,0x00] → Dart LE-read → 1
    ///   raw_id=1 → RSP item stream_id=256 → LE [0x00,0x01] → Dart BE-read → 1
    ///
    /// RSP is delayed 300 ms so the client has time to enable CCCD notifications
    /// before the notification arrives.
    /// Signal IDs are validated against `registry`; unknown IDs are rejected with a warning.
    async fn handle_tlv_subscribe(
        req_id: u16,
        signal_ids: Vec<u16>,
        state: &Arc<RwLock<BleSessionState>>,
        server: &Arc<RwLock<GattServer>>,
        registry: &Arc<SignalRegistry>,
    ) {
        let session_id = state.read().await.current_session_id;
        let mut rsp_items: Vec<SubscribeRspItem> = Vec::new();

        {
            let mut st = state.write().await;
            for raw_id in &signal_ids {
                // Validate + normalize via registry (handles legacy 1/2/3 → IDT 0x01xx)
                let canonical_id = match registry.normalize_id(*raw_id) {
                    Some(id) => id,
                    None => {
                        log::warn!(
                            "TLV SUBSCRIBE: unknown signal_id 0x{:04X} — not in registry, rejected",
                            raw_id
                        );
                        continue;
                    }
                };
                if canonical_id != *raw_id {
                    log::info!(
                        "TLV Signal ID: app sent 0x{:04X} → normalized to 0x{:04X} (legacy→IDT)",
                        raw_id,
                        canonical_id
                    );
                }
                // Safety: normalize_id succeeded, so get() is guaranteed Some
                let meta = registry.get(canonical_id).unwrap();
                // Use raw_id directly for DATA_FRAME stream_id (Flutter LE-reads header field)
                // RSP result item uses stream_id.swap_bytes() so Flutter's BE-read of the RSP
                // field also recovers raw_id.
                let preferred_stream_id = *raw_id;
                let stream_id = st.subscribe_with_stream_id(canonical_id, preferred_stream_id);
                rsp_items.push(SubscribeRspItem {
                    source_id: meta.source_id,
                    signal_id: canonical_id,
                    stream_id: stream_id.swap_bytes(),
                    effective_period_ms: meta.nominal_period_ms,
                    effective_batch_max: 1,
                });
                log::info!(
                    "TLV SUBSCRIBE: signal 0x{:04X} → session stream_id={} → RSP encodes {}",
                    canonical_id,
                    stream_id,
                    stream_id.swap_bytes()
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
                log::info!(
                    "TLV SUBSCRIBE_RSP also sent on Control ({} bytes)",
                    bytes.len()
                );
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
    ///              a 34-byte IDT DATA_FRAME (with t0_ms timestamp and CRC32C tail —
    ///              see TODO-1 resolved) is notified on Data_OUT.
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

            // add_data returns Some(frame) only if signal is subscribed
            if let Some(frame) = state.add_data(signal_id, val_f32, t0_ms) {
                // [TODO-1 resolved] DataFrame::to_ble_bytes() produces 34 bytes (CRC32C appended).
                // Signal name aliases are VitalRecorder-specific; registry governs BLE catalog.
                // Future: move aliases into SignalMeta.aliases when adding waveform signals.

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
        IDT_MAGIC, IDT_VERSION, MSG_SUBSCRIBE_REQ, MSG_SUBSCRIBE_RSP, SUB_OP_SUBSCRIBE,
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

    /// Helper: build a valid Flutter ACK buffer (17 bytes, no IDT magic)
    fn make_ack_bytes(session_id: u16, stream_id: u16, ack_upto: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(17);
        buf.extend_from_slice(&session_id.to_le_bytes()); // [0,1]
        buf.extend_from_slice(&stream_id.to_le_bytes()); // [2,3]
        buf.extend_from_slice(&ack_upto.to_le_bytes()); // [4..7]
        buf.push(8u8); // [8] bitmap_len = 8
        buf.extend_from_slice(&0u64.to_le_bytes()); // [9..16] bitmap = zeros
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

        // Build a Flutter ACK (17 bytes, no IDT magic) acknowledging seq 1 and 2
        let ack_bytes = make_ack_bytes(1, stream_id, 2);
        let ack = AckFrame::from_ble_bytes(&ack_bytes).unwrap();

        {
            let mut st = state.write().await;
            // Flutter ACK: fields directly on AckFrame (no header)
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

    // ── TLV subscribe parsing ─────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-014
    /// Title: Test parse_tlv_subscribe_req with the real Flutter hex payload
    ///
    /// Description: The exact bytes from Flutter's subscribeStreams() must yield
    ///              req_id=42 and signal_ids=[1,2,3].
    #[test]
    fn test_parse_tlv_subscribe_req_real_flutter_bytes() {
        let hex = "20 3F 00 01 02 00 2A 00 02 01 00 02 \
                   03 18 00 01 01 00 01 02 02 00 01 00 03 01 00 00 04 04 00 00 00 00 00 05 01 00 01 \
                   03 18 00 01 01 00 01 02 02 00 02 00 03 01 00 00 04 04 00 00 00 00 00 05 01 00 01 \
                   03 18 00 01 01 00 01 02 02 00 03 00 03 01 00 00 04 04 00 00 00 00 00 05 01 00 01";
        let bytes: Vec<u8> = hex
            .split_whitespace()
            .map(|s| u8::from_str_radix(s, 16).unwrap())
            .collect();

        let (req_id, signal_ids) = parse_tlv_subscribe_req(&bytes).unwrap();
        assert_eq!(req_id, 42);
        assert_eq!(signal_ids, vec![1u16, 2, 3]);
    }

    // ── FLAG_BACKLOG on outgoing frames ───────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-015
    /// Title: Test FLAG_BACKLOG is set on frames when the retransmit buffer is non-empty
    ///
    /// Description: After one unacknowledged frame, the second add_data call shall
    ///              produce a frame with FLAG_BACKLOG set.
    #[tokio::test]
    async fn test_flag_backlog_via_add_data() {
        use crate::domain::ble_protocol::FLAG_BACKLOG;

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16());

            let f1 = st.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
            assert_eq!(f1.header.flags & FLAG_BACKLOG, 0, "First frame: no backlog");

            let f2 = st.add_data(SignalId::HR.as_u16(), 71.0, 1000).unwrap();
            assert_ne!(
                f2.header.flags & FLAG_BACKLOG,
                0,
                "Second frame: unacked buffer → FLAG_BACKLOG must be set"
            );
        }
    }

    // ── Selective ACK bitmap retransmit ───────────────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-016
    /// Title: Test handle_ack_with_bitmap returns lost frames for retransmission
    ///
    /// Description: 4 frames buffered (seq 1-4). Flutter ACK: ack_upto=1, bitmap
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
    /// Description: The TLV subscribe path uses preferred_stream_id = raw_id (1,2,3).
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
}
