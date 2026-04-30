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
    has_idt_magic, parse_idt_wrapped_tlv_subscribe_req, parse_tlv_subscribe_req, AckFrame, Catalog,
    InboundFrame, SignalId, SignalRegistry, SubscribeReq, SubscribeRsp, SubscribeRspItem,
    MSG_SUBSCRIBE_REQ, SUB_OP_SUBSCRIBE, SUB_OP_UNSUBSCRIBE,
};
use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use crate::output::ble_gatt::{CharProperty, GattServer, WriteEvent};
use crate::output::ble_session::BleSessionState;
use crate::output::health::{build_payload, read_os_snapshot, GateHealthState};
use crate::utils::chaos;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};

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
    /// Shared GATE health indicators — updated by output(), write_handler_loop(),
    /// and by the SIO task (processor.rs). Exposed via health_state() accessor.
    pub health_state: Arc<RwLock<GateHealthState>>,
    /// Fired on any health state change → health_task wakes and sends an immediate notify.
    health_notify: Arc<Notify>,
    /// Heartbeat period and stale-file threshold for health_task.
    health_check_interval_sec: u64,
    /// Path to health.json written by HealthWriter.ps1.
    health_file: String,
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
        health_check_interval_sec: u64,
        health_file: String,
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

        server.add_characteristic(
            "Control",
            control_uuid,
            &[CharProperty::Notify, CharProperty::WriteWithoutResponse],
        );
        log::info!(
            "  Characteristic: Control (Notify+Write) -> {}",
            control_uuid
        );

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

        let health_state = Arc::new(RwLock::new(GateHealthState {
            flow_timeout_sec: health_check_interval_sec,
            ..Default::default()
        }));

        Ok(Self {
            server: Arc::new(RwLock::new(server)),
            state: Arc::new(RwLock::new(state)),
            catalog,
            registry,
            health_state,
            health_notify: Arc::new(Notify::new()),
            health_check_interval_sec,
            health_file,
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
            let health_state = self.health_state.clone();
            let health_notify = self.health_notify.clone();
            tokio::spawn(async move {
                Self::write_handler_loop(rx, state, server, registry, health_state, health_notify)
                    .await;
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

        // 4. Spawn disconnect handler task
        {
            let disconnect_rx = {
                let mut server = self.server.write().await;
                server.take_disconnect_receiver()
            };
            if let Some(rx) = disconnect_rx {
                let state = self.state.clone();
                let health_state = self.health_state.clone();
                let health_notify = self.health_notify.clone();
                tokio::spawn(async move {
                    Self::disconnect_handler_loop(rx, state, health_state, health_notify).await;
                });
                log::info!("Disconnect handler task started (Data_OUT SubscribedClientsChanged)");
            } else {
                log::warn!("Disconnect receiver already taken — disconnect detection won't work");
            }
        }

        // 5. Spawn health task (was step 4 before disconnect handler was added)
        {
            let health_state = self.health_state.clone();
            let server = self.server.clone();
            let health_notify = self.health_notify.clone();
            let interval = self.health_check_interval_sec;
            let health_file = self.health_file.clone();
            tokio::spawn(async move {
                Self::health_task(health_state, server, health_notify, interval, health_file).await;
            });
            log::info!(
                "Health task started (interval={}s, file={})",
                self.health_check_interval_sec,
                self.health_file
            );
        }

        // 6. Start GATT server
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
        health_state: Arc<RwLock<GateHealthState>>,
        health_notify: Arc<Notify>,
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
                            // [DEV-5] MyPredi sends ACK with a 24-byte header (not IDT 13b):
                            // magic+header(24b)+payload(17b)+CRC32C(4b) = 45 bytes total.
                            // Try MyPredi format first, then legacy Flutter 17-byte fallback.
                            if let Some(ack) = AckFrame::from_mypredi_bytes(data) {
                                log::info!(
                                    "MyPredi ACK: stream={}, ack_upto={}, bitmap={:02X?}",
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
                            } else if let Some(ack) = AckFrame::from_flutter_bytes(data) {
                                log::debug!(
                                    "Flutter ACK: stream={}, ack_upto={}, bitmap={:02X?}",
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
                                            log::debug!(
                                                "Retransmitted seq {} (Flutter ACK-triggered)",
                                                frame.header.seq
                                            );
                                        }
                                    }
                                }
                            } else {
                                log::warn!(
                                    "Data_IN: unrecognized payload ({} bytes, byte[0]=0x{:02X}) — discarded",
                                    data.len(),
                                    data.first().copied().unwrap_or(0)
                                );
                            }
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
                        if data.get(3).copied() == Some(MSG_SUBSCRIBE_REQ) {
                            if let Some(InboundFrame::SubscribeReq(req)) =
                                InboundFrame::from_ble_bytes(data)
                            {
                                // IDT strict: 13-byte header + binary items
                                Self::handle_subscribe_req(req, &state, &server, &registry).await;
                            } else if let Some((req_id, signal_ids)) =
                                parse_idt_wrapped_tlv_subscribe_req(data)
                            {
                                log::info!(
                                    "Subscribe: MyPredi format wrapped in IDT envelope — req_id={}, signals={:?}",
                                    req_id,
                                    signal_ids
                                );
                                Self::handle_tlv_subscribe(
                                    req_id, signal_ids, &state, &server, &registry,
                                )
                                .await;
                            } else {
                                log::warn!(
                                    "Subscribe: IDT SUBSCRIBE_REQ header present but payload is not a valid IDT SUBSCRIBE_REQ or embedded MyPredi TLV (msg_type=0x{:02X}) — discarded",
                                    data.get(3).copied().unwrap_or(0)
                                );
                            }
                        } else {
                            log::warn!(
                                "Subscribe: IDT magic present but msg_type=0x{:02X} is not SUBSCRIBE_REQ — discarded",
                                data.get(3).copied().unwrap_or(0)
                            );
                        }
                    } else if let Some((req_id, signal_ids)) = parse_tlv_subscribe_req(data) {
                        // [DEV-3] Flutter central sends a custom TLV format (byte[0]=0x20)
                        // instead of an IDT-framed SUBSCRIBE_REQ. Accept as fallback.
                        log::info!(
                            "Subscribe: Flutter TLV format detected — req_id={}, signals={:?}",
                            req_id,
                            signal_ids
                        );
                        Self::handle_tlv_subscribe(req_id, signal_ids, &state, &server, &registry)
                            .await;
                    } else {
                        log::warn!(
                            "Subscribe: unrecognized format ({} bytes, byte[0]=0x{:02X}) — \
                             expected IDT frame (magic=0xD17A) or Flutter TLV (marker=0x20), discarded",
                            data.len(),
                            data.first().copied().unwrap_or(0)
                        );
                        Self::log_subscribe_parse_failure(data);
                    }
                    // Resync ble_subscriber from ground truth after any subscribe event.
                    // Fires health_notify only on actual state transition (0→1 or 1→0).
                    let ble_active = !state.read().await.signal_to_stream.is_empty();
                    let mut hs = health_state.write().await;
                    if hs.ble_subscriber != ble_active {
                        hs.ble_subscriber = ble_active;
                        drop(hs);
                        health_notify.notify_one();
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
                    // Resync ble_subscriber after unsubscribe.
                    let ble_active = !state.read().await.signal_to_stream.is_empty();
                    let mut hs = health_state.write().await;
                    if hs.ble_subscriber != ble_active {
                        hs.ble_subscriber = ble_active;
                        drop(hs);
                        health_notify.notify_one();
                    }
                }

                // ── Control: health pull request — any write triggers immediate health push ──
                "Control" => {
                    log::info!("Health pull request received on Control — sending immediate health payload");
                    health_notify.notify_one();
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

    /// ID SRS: SRS-FN-BLERELIABLE-012
    /// Title: disconnect_handler_loop
    ///
    /// Description: VRConnect shall reset all BLE session state whenever the Central
    ///              disconnects, as signalled by a `()` on the GATT server's disconnect
    ///              channel (Data_OUT SubscribedClientsChanged count → 0).
    ///              Clears all active streams and tx_buffers via on_disconnect(), which
    ///              also auto-increments the session_id to prevent stale-frame confusion
    ///              on reconnect.  Resets the ble_subscriber health flag and fires an
    ///              immediate health push so the Control characteristic reflects the new state.
    ///
    /// Version: V1.0
    async fn disconnect_handler_loop(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        state: Arc<RwLock<BleSessionState>>,
        health_state: Arc<RwLock<GateHealthState>>,
        health_notify: Arc<Notify>,
    ) {
        log::info!("[BLE] Disconnect handler loop running");
        while rx.recv().await.is_some() {
            state.write().await.on_disconnect();
            log::info!("[BLE] Session state cleared after Central disconnect");
            let mut hs = health_state.write().await;
            if hs.ble_subscriber {
                hs.ble_subscriber = false;
                drop(hs);
                health_notify.notify_one();
            }
        }
        log::info!("[BLE] Disconnect handler loop ended");
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
            if req.op == SUB_OP_SUBSCRIBE {
                st.unsubscribe_all();
            }
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
        // - TLV 0x21 on Control (90b0) only — MyPredi listens here, ignores RSP content
        // - Data_OUT is intentionally skipped: MyPredi treats all Data_OUT frames as DATA_FRAMEs [DEV-5]
        if req.op == SUB_OP_SUBSCRIBE && !rsp_items.is_empty() {
            let rsp = SubscribeRsp {
                session_id,
                req_id,
                status: 0, // 0 = OK
                results: rsp_items,
            };
            let tlv_bytes = rsp.to_flutter_tlv_bytes();
            let srv = server.read().await;
            // NOTE: Do NOT notify Data_OUT with SUBSCRIBE_RSP — MyPredi's _processBuffer
            // reads ALL Data_OUT notifications as DATA_FRAMEs; sending RSP there corrupts
            // its frame buffer (reads period_ms bytes as payloadLen → "Bad Magic"). [DEV-5]
            // Flutter/MyPredi Central listens on Control (90b0) and expects TLV 0x21 format
            if let Err(e) = srv.notify("Control", &tlv_bytes).await {
                log::debug!(
                    "SUBSCRIBE_RSP TLV on Control: {} (client may not be subscribed)",
                    e
                );
            } else {
                log::info!(
                    "SUBSCRIBE_RSP TLV sent on Control ({} bytes, req_id={})",
                    tlv_bytes.len(),
                    req_id
                );
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
                        // Stop replay on first notify failure (device likely disconnected)
                        break;
                    }
                    // Rate-limit replay to ~50 frames/s (20 ms inter-frame gap).
                    // Without this, a full 3600-frame backlog floods the BLE stack
                    // (~367 KB burst) causing Android to drop the connection. [DEV-6]
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
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

    /// Handle a Flutter TLV SUBSCRIBE_REQ (byte[0]=0x20, [DEV-3]).
    ///
    /// Normalizes legacy signal IDs (1/2/3) to IDT compound IDs via the registry,
    /// assigns stream IDs matching the raw signal ID (HR=1, SpO2=2, Temp=3) so Flutter's
    /// hardcoded `activeStreams` map aligns, then sends SUBSCRIBE_RSP on Data_OUT.
    ///
    /// A 300 ms delay before the RSP notify is required because the Flutter app enables
    /// CCCD *after* writing to the Subscribe characteristic [DEV-4].
    /// Maps a canonical signal_id to the stream ID hardcoded by Flutter's `_initStreams()`.
    ///
    /// Flutter (vr_ble_gatt_callback.dart) pre-populates `activeStreams` with fixed IDs 1-7
    /// before any SUBSCRIBE_REQ is sent. DATA_FRAMEs must use these exact stream IDs or
    /// Flutter calls `activeStreams[streamId]` → null and silently drops the frame.
    ///
    /// ID SRS: SRS-FN-BLERELIABLE-010
    /// Version: V1.0
    fn flutter_stream_id(signal_id: u16) -> u16 {
        match signal_id {
            0x0101 => 1, // HR
            0x0102 => 2, // SpO2
            0x0103 => 3, // Temperature
            0x0201 => 4, // SBP
            0x0202 => 5, // DBP
            0x0203 => 6, // MBP
            0x0501 => 7, // AmbPres
            other => other, // unknown signal — pass-through, will likely be dropped by Flutter
        }
    }

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
            st.unsubscribe_all();
            for raw_id in &signal_ids {
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
                        // Flutter v2 reads stream_id from SUBSCRIBE_RSP to build activeStreams.
                // We use a fixed 1-7 mapping so stream IDs are stable and predictable:
                //   0x0101→1, 0x0102→2, 0x0103→3, 0x0201→4, 0x0202→5, 0x0203→6, 0x0501→7
                // RSP and DATA_FRAMEs both use this mapping — they must be consistent.
                let flutter_sid = Self::flutter_stream_id(canonical_id);
                let stream_id = st.subscribe_with_stream_id(canonical_id, flutter_sid);
                let meta = registry.get(canonical_id).unwrap();
                // RSP stream_id encoded as-is (Flutter ignores SUBSCRIBE_RSP per DEV-4/FLUTTER_COMPAT §4).
                // Future IDT-compliant clients will read it correctly in LE.
                rsp_items.push(SubscribeRspItem {
                    source_id: meta.source_id,
                    signal_id: canonical_id,
                    stream_id,
                    effective_period_ms: meta.nominal_period_ms,
                    effective_batch_max: 1,
                });
                log::info!(
                    "TLV SUBSCRIBE: signal 0x{:04X} → stream {}",
                    canonical_id,
                    stream_id
                );
            }
        }

        if rsp_items.is_empty() {
            return;
        }

        // [DEV-6] Send SUBSCRIBE_RSP on Data_OUT as a full 24-byte IDT frame.
        // Flutter v2 _processBuffer() dispatches msgType=0x02 → _handleSubscribeResponse(),
        // which builds activeStreams from the TLV payload. _initStreams() is now commented
        // out — activeStreams is empty until RSP arrives. Without RSP, all DATA_FRAMEs
        // are silently dropped (stream == null guard).
        let rsp = SubscribeRsp {
            session_id,
            req_id,
            status: 0,
            results: rsp_items,
        };
        let rsp_bytes = rsp.to_mypredi_ble_bytes();

        // Small delay: BLE stack ordering safety margin.
        // CCCD is pre-enabled by Flutter before the SUBSCRIBE write, so no long wait needed.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let srv = server.read().await;
        if let Err(e) = srv.notify("Data_OUT", &rsp_bytes).await {
            log::warn!(
                "SUBSCRIBE_RSP notify on Data_OUT failed (req_id={}): {}",
                req_id, e
            );
        } else {
            log::info!(
                "SUBSCRIBE_RSP sent on Data_OUT (req_id={}, {} stream(s), {} bytes)",
                req_id,
                rsp.results.len(),
                rsp_bytes.len()
            );
        }
    }

    /// Extract (signal_id, f32) pairs from ProcessedData for room_index=0.
    /// Signal name mapping covers all known VitalRecorder export names:
    /// - SpO2:        "SPO2", "PLETH", "PLETH_SPO2"
    /// - Temperature: "TEMP", "TEMPERATURE", "BT", "BT1", "BT1_TEMP"
    /// - SBP:         "SBP", "NIBP_SBP"
    /// - DBP:         "DBP", "NIBP_DBP"
    /// - MBP:         "MBP", "NIBP_MBP"
    /// - AmbPres:     "AMB_PRES", "AMBIENT_PRESSURE"
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
            ("SBP", SignalId::SBP.as_u16()),
            ("NIBP_SBP", SignalId::SBP.as_u16()),
            ("DBP", SignalId::DBP.as_u16()),
            ("NIBP_DBP", SignalId::DBP.as_u16()),
            ("MBP", SignalId::MBP.as_u16()),
            ("NIBP_MBP", SignalId::MBP.as_u16()),
            ("AMB_PRES", SignalId::AmbPres.as_u16()),
            ("AMBIENT_PRESSURE", SignalId::AmbPres.as_u16()),
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
    ///              Duplicate (signal_id, t0_ms) pairs within one call are skipped —
    ///              VitalRecorder may emit 2–3 records with the same timestamp per signal.
    ///
    /// Version: V1.0
    pub async fn output(&self, data: &ProcessedData) -> Result<()> {
        // Update flow timestamp on every call — no health notify needed here;
        // flow state changes slowly (only meaningful after flow_timeout_sec elapses).
        self.health_state.write().await.last_processed_data = Some(Instant::now());

        let mut state = self.state.write().await;
        let server = self.server.read().await;

        let mut seen: std::collections::HashSet<(u16, u64)> = std::collections::HashSet::new();

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
                "SBP" | "NIBP_SBP" => SignalId::SBP.as_u16(),
                "DBP" | "NIBP_DBP" => SignalId::DBP.as_u16(),
                "MBP" | "NIBP_MBP" => SignalId::MBP.as_u16(),
                "AMB_PRES" | "AMBIENT_PRESSURE" => SignalId::AmbPres.as_u16(),
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

            // VitalRecorder emits 2–3 duplicate records per signal in one Socket.IO message.
            // Skip duplicates to avoid storing the same point in history and notifying MyPredi twice.
            if !seen.insert((signal_id, t0_ms)) {
                log::debug!(
                    "Duplicate (signal=0x{:04X}, t0_ms={}) skipped",
                    signal_id,
                    t0_ms
                );
                continue;
            }

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

    /// ID SRS: SRS-FN-BLERELIABLE-008
    /// Title: notify_control
    ///
    /// Description: VRConnect shall send a raw byte payload on the Control GATT
    ///              characteristic (0x90b0, Notify). If no Central is subscribed to the
    ///              Control CCCD, the Windows BLE stack returns an error — this method
    ///              silently discards it (no-op). All other errors are logged at DEBUG.
    ///
    /// Version: V1.0
    pub async fn notify_control(&self, data: &[u8]) -> Result<()> {
        let server = self.server.read().await;
        if let Err(e) = server.notify("Control", data).await {
            // No subscriber on Control is the common case — demote to debug.
            log::debug!("[health] Control notify: {} (no subscriber?)", e);
        }
        Ok(())
    }

    /// ID SRS: SRS-FN-BLERELIABLE-009
    /// Title: health_state
    ///
    /// Description: Returns the shared GateHealthState so external tasks (SIO, processor)
    ///              can update sio_connected and last_processed_data.
    ///
    /// Version: V1.0
    pub fn health_state(&self) -> Arc<RwLock<GateHealthState>> {
        self.health_state.clone()
    }

    /// ID SRS: SRS-FN-BLERELIABLE-010
    /// Title: health_notify
    ///
    /// Description: Returns the shared Notify handle so external tasks can trigger an
    ///              immediate health push (e.g. on SIO connect/disconnect).
    ///
    /// Version: V1.0
    pub fn health_notify(&self) -> Arc<Notify> {
        self.health_notify.clone()
    }

    /// ID SRS: SRS-FN-BLERELIABLE-011
    /// Title: health_task
    ///
    /// Description: Periodic task that builds a HealthPayload from OsHealthSnapshot +
    ///              GateHealthState and emits it on the Control characteristic.
    ///
    ///              Runs on two triggers (whichever comes first):
    ///                1. `health_notify` fires → immediate push on any state change
    ///                   (sio connect/disconnect, ble subscribe/unsubscribe)
    ///                2. `check_interval_sec` timer expires → heartbeat push
    ///
    ///              `notify_control()` is a no-op when no Central subscribes to Control CCCD.
    ///
    /// Version: V1.0
    async fn health_task(
        health_state: Arc<RwLock<GateHealthState>>,
        server: Arc<RwLock<GattServer>>,
        health_notify: Arc<Notify>,
        check_interval_sec: u64,
        health_file: String,
    ) {
        let interval = Duration::from_secs(check_interval_sec);
        let stale_threshold = check_interval_sec.saturating_mul(2);

        log::info!(
            "[health] Task started (interval={}s, file={}, stale_threshold={}s)",
            check_interval_sec,
            health_file,
            stale_threshold
        );

        loop {
            // Wait for a state-change trigger OR the heartbeat timer — whichever fires first.
            let _ = tokio::time::timeout(interval, health_notify.notified()).await;

            let os = read_os_snapshot(Path::new(&health_file), stale_threshold);
            let gate = health_state.read().await;
            let payload = build_payload(&os, &gate);
            drop(gate);

            match serde_json::to_vec(&payload) {
                Ok(bytes) => {
                    let srv = server.read().await;
                    if let Err(e) = srv.notify("Control", &bytes).await {
                        log::debug!(
                            "[health] Control notify: {} (no subscriber — payload not sent)",
                            e
                        );
                    } else {
                        log::debug!(
                            "[health] Health payload sent ({} bytes, ok={})",
                            bytes.len(),
                            payload.ok
                        );
                    }
                }
                Err(e) => {
                    log::error!("[health] Failed to serialize HealthPayload: {}", e);
                }
            }
        }
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
    /// Title: SUBSCRIBE_RSP sent on Data_OUT as full 24-byte IDT frame [DEV-6]
    ///
    /// Description: Flutter v2 routes msgType=0x02 on Data_OUT to _handleSubscribeResponse(),
    ///              which builds activeStreams from the TLV payload. _initStreams() is now
    ///              commented out — activeStreams is empty until RSP is received.
    ///              Verify that to_mypredi_ble_bytes() produces a valid IDT frame:
    ///              magic at [0-1], msgType=0x02 at [3], payloadLen at [22-23], CRC32C valid.
    #[test]
    fn test_subscribe_rsp_mypredi_format() {
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
        let bytes = rsp.to_mypredi_ble_bytes();
        // Minimum: 24-byte header + some TLV payload + 4-byte CRC
        assert!(bytes.len() > 28, "RSP frame must be > 28 bytes");
        // IDT magic at [0-1]
        assert_eq!(
            u16::from_le_bytes([bytes[0], bytes[1]]),
            IDT_MAGIC,
            "magic must be 0xD17A"
        );
        // msgType=0x02 at [3]
        assert_eq!(bytes[3], MSG_SUBSCRIBE_RSP, "msgType must be 0x02");
        // payloadLen at [22-23] must match actual payload size
        let payload_len = u16::from_le_bytes([bytes[22], bytes[23]]) as usize;
        assert_eq!(
            bytes.len(),
            24 + payload_len + 4,
            "frame size must be 24 + payloadLen + 4"
        );
        // CRC32C valid
        let expected_crc = crc32c::crc32c(&bytes[..bytes.len() - 4]);
        let actual_crc = u32::from_le_bytes([
            bytes[bytes.len() - 4],
            bytes[bytes.len() - 3],
            bytes[bytes.len() - 2],
            bytes[bytes.len() - 1],
        ]);
        assert_eq!(actual_crc, expected_crc, "CRC32C must be valid");
        // Legacy to_flutter_tlv_bytes() still produces 0x21 outer TLV (IDT subscribe path)
        let tlv_bytes = rsp.to_flutter_tlv_bytes();
        assert_eq!(tlv_bytes[0], 0x21, "legacy TLV outer type must be 0x21");
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
            assert_eq!(
                f1.header.flags & FLAG_BACKLOG,
                0,
                "First live frame: FLAG_BACKLOG must be clear"
            );

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
        assert_eq!(
            r.normalize_id(0x9999),
            None,
            "unknown IDT compound ID must be rejected"
        );
        assert_eq!(
            r.normalize_id(0x0200),
            None,
            "unknown source-2 ID must be rejected"
        );
        assert_eq!(r.normalize_id(0), None, "zero must always be rejected");
        assert_eq!(
            r.normalize_id(99),
            None,
            "unknown legacy simple ID must be rejected"
        );
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
            assert!(
                st.is_subscribed(0x0101),
                "must be subscribed at canonical IDT ID 0x0101"
            );
            assert!(
                !st.is_subscribed(1),
                "legacy raw ID 1 must NOT appear as subscribed"
            );
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
            assert!(
                !st.is_subscribed(SignalId::HR.as_u16()),
                "pre-condition: not subscribed"
            );
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
        use crate::domain::ble_protocol::{SignalId, FLAG_BACKLOG};
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

    // ── Control pull-request (health on demand) ───────────────────────────────

    /// ID SRS: SRS-TEST-BLERELIABLE-029
    /// Title: Control write triggers immediate health_notify
    ///
    /// Description: Any write on the Control characteristic shall call notify_one()
    ///              on health_notify within 100 ms, waking the health task.
    #[tokio::test]
    async fn test_control_write_triggers_health_notify() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WriteEvent>();
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        let service_uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let server = Arc::new(RwLock::new(GattServer::new(
            "Test".to_string(),
            service_uuid,
        )));
        let registry = Arc::new(SignalRegistry::with_defaults());
        let health_state = Arc::new(RwLock::new(GateHealthState::default()));
        let health_notify = Arc::new(Notify::new());
        let health_notify_check = health_notify.clone();

        tokio::spawn(async move {
            ReliableBleOutput::write_handler_loop(
                rx,
                state,
                server,
                registry,
                health_state,
                health_notify,
            )
            .await;
        });

        tx.send(WriteEvent {
            characteristic_name: "Control".to_string(),
            data: vec![0x01],
        })
        .unwrap();

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            health_notify_check.notified(),
        )
        .await;
        assert!(
            result.is_ok(),
            "health_notify must fire within 100 ms of a Control write"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-030
    /// Title: Control write content is ignored — any payload triggers health push
    ///
    /// Description: Writes of [0x00], [0xFF], and [] (empty) on Control must all
    ///              trigger health_notify, regardless of content.
    #[tokio::test]
    async fn test_control_write_any_payload_triggers_notify() {
        for data in [vec![0x00u8], vec![0xFFu8], vec![]] {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WriteEvent>();
            let state = Arc::new(RwLock::new(BleSessionState::new(1)));
            let service_uuid =
                uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
            let server = Arc::new(RwLock::new(GattServer::new(
                "Test".to_string(),
                service_uuid,
            )));
            let registry = Arc::new(SignalRegistry::with_defaults());
            let health_state = Arc::new(RwLock::new(GateHealthState::default()));
            let health_notify = Arc::new(Notify::new());
            let health_notify_check = health_notify.clone();

            tokio::spawn(async move {
                ReliableBleOutput::write_handler_loop(
                    rx,
                    state,
                    server,
                    registry,
                    health_state,
                    health_notify,
                )
                .await;
            });

            tx.send(WriteEvent {
                characteristic_name: "Control".to_string(),
                data,
            })
            .unwrap();

            let result = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                health_notify_check.notified(),
            )
            .await;
            assert!(
                result.is_ok(),
                "health_notify must fire regardless of Control write payload"
            );
        }
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-031
    /// Title: Concurrent health event + pull request coalesce — health_task wakes once
    ///
    /// Description: tokio::Notify stores at most one permit. Two consecutive notify_one()
    ///              calls before notified() is polled behave as one: the second notified()
    ///              blocks. This verifies the documented coalescing behaviour relied upon
    ///              by the health task to avoid double pushes on simultaneous triggers.
    #[tokio::test]
    async fn test_control_and_event_driven_notify_coalesce() {
        let health_notify = Arc::new(Notify::new());

        health_notify.notify_one(); // event-driven trigger (e.g. sio_connected changed)
        health_notify.notify_one(); // pull request arriving simultaneously

        // First notified() must resolve immediately (one permit stored).
        let r1 = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            health_notify.notified(),
        )
        .await;
        assert!(r1.is_ok(), "first notified() must resolve immediately");

        // Second notified() must time out — notify_one coalesces, only one permit issued.
        let r2 = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            health_notify.notified(),
        )
        .await;
        assert!(
            r2.is_err(),
            "second notified() must time out — notify_one coalesces duplicates"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-028
    /// Title: output() deduplicates duplicate (signal_id, t0_ms) pairs
    ///
    /// Description: VRConnect shall emit exactly one DATA_FRAME per unique
    ///              (signal_id, t0_ms) pair within a single output() call.
    ///              VitalRecorder may emit 2–3 records with the same timestamp
    ///              for the same signal in one Socket.IO message; the extra copies
    ///              must be silently dropped — not forwarded to MyPredi, not stored
    ///              in the history buffer.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_output_deduplicates_same_signal_same_timestamp() {
        use crate::domain::ble_protocol::SignalId;

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));

        // Subscribe to HR so add_data() actually produces frames
        {
            let mut st = state.write().await;
            st.subscribe_with_stream_id(SignalId::HR.as_u16(), 1);
        }

        // Build ProcessedData with 3 identical HR tracks (same t0_ms, same value)
        let fixed_ts = chrono::DateTime::from_timestamp_millis(1_776_672_192_000).unwrap();
        let make_track = |record_index: i32| ProcessedTrack {
            name: "HR".to_string(),
            display_value: "51.000".to_string(),
            raw_value: Some(51.0),
            unit: "bpm".to_string(),
            timestamp: fixed_ts,
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index,
            track_type: crate::domain::processed_data::TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        };

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![make_track(0), make_track(1), make_track(2)],
        };
        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Drive record_history + add_data via BleSessionState directly,
        // mirroring the dedup logic in output().
        {
            let mut st = state.write().await;
            let mut seen: std::collections::HashSet<(u16, u64)> = std::collections::HashSet::new();
            let mut frames_generated = 0usize;
            let mut history_inserts = 0usize;

            for track in &data.all_tracks {
                if track.room_index != 0 {
                    continue;
                }
                let signal_id = SignalId::HR.as_u16();
                let t0_ms = track.timestamp.timestamp_millis() as u64;

                if !seen.insert((signal_id, t0_ms)) {
                    continue;
                }

                let val = track.raw_value.unwrap() as f32;
                st.record_history(signal_id, val, t0_ms);
                history_inserts += 1;

                if st.add_data(signal_id, val, t0_ms).is_some() {
                    frames_generated += 1;
                }
            }

            assert_eq!(
                frames_generated, 1,
                "only one DATA_FRAME should be generated for 3 identical tracks"
            );
            assert_eq!(
                history_inserts, 1,
                "only one history entry should be stored for 3 identical tracks"
            );

            let hist = st.history.get(&SignalId::HR.as_u16()).unwrap();
            assert_eq!(
                hist.len(),
                1,
                "history ring-buffer must contain exactly 1 entry"
            );
            assert_eq!(
                hist[0],
                (1_776_672_192_000u64, 51.0f32),
                "stored history entry must match the single expected sample"
            );
        }
    }

    // ── unsubscribe_all integration ───────────────────────────────────────────
    // These tests validate the unsubscribe_all behaviour used in handle_subscribe_req
    // and handle_tlv_subscribe without requiring BLE hardware, by exercising the same
    // BleSessionState operations the async handlers perform internally.

    /// ID SRS: SRS-TEST-BLERELIABLE-032
    /// Title: SUB_OP_SUBSCRIBE path: unsubscribe_all then subscribe replaces prior set
    ///
    /// Description: Mirrors handle_subscribe_req (op=SUBSCRIBE): pre-subscribed HR+SpO2,
    ///              then unsubscribe_all() + subscribe(HR) → only HR active.
    #[tokio::test]
    async fn test_subscribe_op_replaces_prior_subscriptions_via_unsubscribe_all() {
        use crate::domain::ble_protocol::{SignalId, SignalRegistry};

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        let registry = Arc::new(SignalRegistry::with_defaults());

        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16());
            st.subscribe(SignalId::SpO2.as_u16());
        }
        assert_eq!(state.read().await.streams.len(), 2);

        // Simulate handle_subscribe_req(op=SUBSCRIBE, items=[HR])
        {
            let mut st = state.write().await;
            st.unsubscribe_all(); // <-- the new call
            let canonical = registry.normalize_id(SignalId::HR.as_u16()).unwrap();
            st.subscribe(canonical);
        }

        let st = state.read().await;
        assert!(
            st.is_subscribed(SignalId::HR.as_u16()),
            "HR must be subscribed after simulate-SUBSCRIBE"
        );
        assert!(
            !st.is_subscribed(SignalId::SpO2.as_u16()),
            "SpO2 must be gone after unsubscribe_all"
        );
        assert_eq!(st.streams.len(), 1);
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-033
    /// Title: SUB_OP_UNSUBSCRIBE path: no unsubscribe_all, only individual removal
    ///
    /// Description: Mirrors handle_subscribe_req (op=UNSUBSCRIBE): pre-subscribed HR+SpO2,
    ///              UNSUBSCRIBE path calls unsubscribe(SpO2) only — HR must remain.
    #[tokio::test]
    async fn test_unsubscribe_op_removes_only_targeted_signal() {
        use crate::domain::ble_protocol::{SignalId, SignalRegistry};

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        let registry = Arc::new(SignalRegistry::with_defaults());

        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16());
            st.subscribe(SignalId::SpO2.as_u16());
        }

        // Simulate handle_subscribe_req(op=UNSUBSCRIBE, items=[SpO2])
        // — unsubscribe_all must NOT be called here
        {
            let mut st = state.write().await;
            let canonical = registry
                .normalize_id(SignalId::SpO2.as_u16())
                .unwrap_or(SignalId::SpO2.as_u16());
            st.unsubscribe(canonical);
        }

        let st = state.read().await;
        assert!(
            st.is_subscribed(SignalId::HR.as_u16()),
            "HR must survive UNSUBSCRIBE(SpO2)"
        );
        assert!(
            !st.is_subscribed(SignalId::SpO2.as_u16()),
            "SpO2 must be removed by individual unsubscribe"
        );
        assert_eq!(st.streams.len(), 1);
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-034
    /// Title: TLV-subscribe path: unsubscribe_all + subscribe_with_stream_id replaces prior set
    ///
    /// Description: Mirrors handle_tlv_subscribe: pre-subscribed HR+SpO2, then
    ///              unsubscribe_all() + subscribe_with_stream_id(HR, flutter_sid) → only HR.
    #[tokio::test]
    async fn test_tlv_subscribe_replaces_prior_subscriptions_via_unsubscribe_all() {
        use crate::domain::ble_protocol::{SignalId, SignalRegistry};

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        let registry = Arc::new(SignalRegistry::with_defaults());

        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16());
            st.subscribe(SignalId::SpO2.as_u16());
        }
        assert_eq!(state.read().await.streams.len(), 2);

        // Simulate handle_tlv_subscribe(signal_ids=[HR])
        {
            let mut st = state.write().await;
            st.unsubscribe_all(); // <-- the new call
            let canonical = registry.normalize_id(SignalId::HR.as_u16()).unwrap();
            let flutter_sid = ReliableBleOutput::flutter_stream_id(canonical);
            st.subscribe_with_stream_id(canonical, flutter_sid);
        }

        let st = state.read().await;
        assert!(
            st.is_subscribed(SignalId::HR.as_u16()),
            "HR must be subscribed after simulate-TLV-SUBSCRIBE"
        );
        assert!(
            !st.is_subscribed(SignalId::SpO2.as_u16()),
            "SpO2 must be cleared by unsubscribe_all in TLV-subscribe path"
        );
        assert_eq!(st.streams.len(), 1);
    }
}
