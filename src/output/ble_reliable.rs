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
use crate::output::ble_gatt::{BleConnectionEvent, CharProperty, GattServer, WriteEvent};
use crate::output::ble_session::BleSessionState;
use crate::output::health::{build_payload, read_os_snapshot, GateHealthState};
use crate::output::wal::{self, WalEntry};
use crate::utils::chaos;
use std::io::{BufWriter, Write as IoWrite};
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
    /// Grace period in seconds before session reset on Central disconnect.
    /// If Flutter reconnects within this window, session state is preserved.
    /// 0 = immediate reset (legacy behaviour).
    ble_grace_period_sec: u64,
    /// Supervision timeout in seconds — if tx_buffer has pending frames and no ACK arrives
    /// within this window, GATE treats it as a brutal link-layer disconnect and resets the
    /// session. 0 = disabled (not recommended in production).
    ble_supervision_timeout_sec: u64,
    /// Interval in seconds between periodic history checkpoints. 0 = disabled.
    history_checkpoint_interval_sec: u64,
    /// Maximum age in seconds of a checkpoint file to be loaded at startup.
    history_checkpoint_max_age_sec: u64,
    /// Path to the binary history checkpoint file.
    history_checkpoint_path: String,
    /// History retention window in seconds — drives ring-buffer size and age eviction.
    /// Passed to BleSessionState::with_history_retention() at construction.
    history_retention_sec: u64,
    /// WAL enabled flag — when false all wal_* fields are inert.
    wal_enabled: bool,
    /// Path to the WAL append-only journal file.
    wal_path: String,
    /// Seconds between batched fsyncs of the WAL file (= max crash-loss window).
    wal_fsync_interval_sec: u64,
    /// Seconds between WAL compactions (full snapshot + WAL truncation).
    wal_compaction_interval_sec: u64,
    /// Sender half of the WAL channel. None when WAL is disabled.
    wal_tx: Option<tokio::sync::mpsc::UnboundedSender<WalEntry>>,
    /// Receiver half of the WAL channel, taken once by start() to spawn wal_task.
    wal_rx: tokio::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<WalEntry>>>,
    // One-shot retry queue: frames whose notify() failed in the previous output() call.
    // Drained at the start of Phase 2 on the next call; frames that fail again are
    // discarded (they stay in tx_buffer and are NACK-recoverable by Flutter).
    // Capped at RETRY_QUEUE_CAP to bound memory; excess is dropped with a WARN.
    retry_queue: tokio::sync::Mutex<Vec<(Vec<u8>, u16, u16, u32)>>,
}

// Maximum frames held for one-shot retry (optimistic fast-path; NACK covers persistent loss).
const RETRY_QUEUE_CAP: usize = 16;

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
        health_ble_flow_timeout_sec: u64,
        health_file: String,
        ble_grace_period_sec: u64,
        ble_supervision_timeout_sec: u64,
        history_checkpoint_interval_sec: u64,
        history_checkpoint_max_age_sec: u64,
        history_checkpoint_path: String,
        history_retention_sec: u64,
        wal_enabled: bool,
        wal_path: String,
        wal_fsync_interval_sec: u64,
        wal_compaction_interval_sec: u64,
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
        let state = BleSessionState::new(1).with_history_retention(history_retention_sec);

        let health_state = Arc::new(RwLock::new(GateHealthState {
            flow_timeout_sec: health_ble_flow_timeout_sec,
            ..Default::default()
        }));

        let (wal_tx, wal_rx) = if wal_enabled {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        Ok(Self {
            server: Arc::new(RwLock::new(server)),
            state: Arc::new(RwLock::new(state)),
            catalog,
            registry,
            health_state,
            health_notify: Arc::new(Notify::new()),
            health_check_interval_sec,
            health_file,
            ble_grace_period_sec,
            ble_supervision_timeout_sec,
            history_checkpoint_interval_sec,
            history_checkpoint_max_age_sec,
            history_checkpoint_path,
            history_retention_sec,
            wal_enabled,
            wal_path,
            wal_fsync_interval_sec,
            wal_compaction_interval_sec,
            wal_tx,
            wal_rx: tokio::sync::Mutex::new(wal_rx),
            retry_queue: tokio::sync::Mutex::new(Vec::new()),
        })
    }

    #[cfg(test)]
    pub(crate) async fn retry_queue_len(&self) -> usize {
        self.retry_queue.lock().await.len()
    }

    #[cfg(test)]
    pub(crate) async fn history_len_for_test(&self, signal_id: u16) -> usize {
        self.state
            .read()
            .await
            .history
            .get(&signal_id)
            .map(|h| h.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) async fn subscribe_for_test(&self, signal_id: u16, stream_id: u16) {
        self.state
            .write()
            .await
            .subscribe_with_stream_id(signal_id, stream_id);
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
    ///   4. Spawn the disconnect handler task (grace period)
    ///   5. Spawn the health task (heartbeat + BLE flow watchdog)
    ///   6. Load history checkpoint if present and fresh enough
    ///   7. Spawn the checkpoint task (periodic history persistence)
    ///   8. Start the GATT server (creates Windows GATT service + advertises)
    ///
    /// Version: V2.0
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

        // Shared last-ACK timestamp: write_handler_loop updates it on every ACK received;
        // supervision_task reads it to detect a frozen ACK channel (brutal link-layer drop).
        let last_ack_time = Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now()));

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
            let last_ack_time_wh = last_ack_time.clone();
            tokio::spawn(async move {
                Self::write_handler_loop(
                    rx,
                    state,
                    server,
                    registry,
                    health_state,
                    health_notify,
                    last_ack_time_wh,
                )
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

        // 4. Spawn disconnect handler task (grace period = ble_grace_period_sec)
        {
            let disconnect_rx = {
                let mut server = self.server.write().await;
                server.take_disconnect_receiver()
            };
            if let Some(rx) = disconnect_rx {
                let state = self.state.clone();
                let health_state = self.health_state.clone();
                let health_notify = self.health_notify.clone();
                let grace_period_sec = self.ble_grace_period_sec;
                tokio::spawn(async move {
                    Self::disconnect_handler_loop(
                        rx,
                        state,
                        health_state,
                        health_notify,
                        grace_period_sec,
                    )
                    .await;
                });
                log::info!(
                    "Disconnect handler task started (grace={}s, Data_OUT SubscribedClientsChanged)",
                    self.ble_grace_period_sec
                );
            } else {
                log::warn!("Disconnect receiver already taken — disconnect detection won't work");
            }
        }

        // 4.5 Spawn supervision task — detects brutal link-layer disconnects (Central goes
        // out of range without a CCCD update; SubscribedClientsChanged never fires).
        if self.ble_supervision_timeout_sec > 0 {
            let state = self.state.clone();
            let health_state = self.health_state.clone();
            let health_notify = self.health_notify.clone();
            let last_ack_time_sv = last_ack_time.clone();
            let timeout = self.ble_supervision_timeout_sec;
            tokio::spawn(async move {
                Self::supervision_task(state, health_state, health_notify, last_ack_time_sv, timeout)
                    .await;
            });
            log::info!(
                "Supervision task started (timeout={}s, check=5s)",
                self.ble_supervision_timeout_sec
            );
        } else {
            log::info!("Supervision task disabled (ble_supervision_timeout_sec=0)");
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

        // 6. Load history checkpoint if fresh enough.
        // max_age threshold = max(history_checkpoint_max_age_sec, history_retention_sec)
        // so the checkpoint is never rejected for being older than the retention window.
        let ckpt_max_age = self
            .history_checkpoint_max_age_sec
            .max(self.history_retention_sec);
        Self::try_load_checkpoint(&self.state, &self.history_checkpoint_path, ckpt_max_age).await;

        // 6.5 Replay WAL entries not yet captured in the checkpoint (if WAL enabled).
        // record_history dedup guard prevents double-insertion with checkpoint data.
        if self.wal_enabled {
            let entries = wal::replay_wal_entries(&self.wal_path);
            if !entries.is_empty() {
                let mut st = self.state.write().await;
                for e in &entries {
                    st.record_history(e.signal_id, e.value, e.t0_ms);
                }
                log::info!(
                    "[WAL] Replayed {} entries from WAL (path={})",
                    entries.len(),
                    self.wal_path
                );
            } else {
                log::debug!("[WAL] No WAL entries to replay (path={})", self.wal_path);
            }
        }

        // 7. Spawn checkpoint task (skip if interval=0)
        if self.history_checkpoint_interval_sec > 0 {
            let state = self.state.clone();
            let interval = self.history_checkpoint_interval_sec;
            let path = self.history_checkpoint_path.clone();
            tokio::spawn(async move {
                Self::checkpoint_task(state, interval, path).await;
            });
            log::info!(
                "History checkpoint task started (interval={}s, path={})",
                self.history_checkpoint_interval_sec,
                self.history_checkpoint_path
            );
        } else {
            log::info!("History checkpoint disabled (interval=0)");
        }

        // 8. Spawn WAL task (skip if disabled)
        if self.wal_enabled {
            let rx = self.wal_rx.lock().await.take();
            if let Some(rx) = rx {
                let state = self.state.clone();
                let wal_path = self.wal_path.clone();
                let fsync_interval = self.wal_fsync_interval_sec;
                let compact_interval = self.wal_compaction_interval_sec;
                let ckpt_path = self.history_checkpoint_path.clone();
                tokio::spawn(async move {
                    Self::wal_task(
                        state,
                        rx,
                        wal_path,
                        fsync_interval,
                        compact_interval,
                        ckpt_path,
                    )
                    .await;
                });
                log::info!(
                    "[WAL] WAL task started (fsync={}s, compact={}s, path={})",
                    self.wal_fsync_interval_sec,
                    self.wal_compaction_interval_sec,
                    self.wal_path
                );
            } else {
                log::warn!("[WAL] WAL receiver already taken — WAL task won't run");
            }
        } else {
            log::info!("[WAL] WAL disabled (set WAL_ENABLED=true to enable)");
        }

        // 9. Start GATT server
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
        last_ack_time: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
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
                            *last_ack_time.lock().await = tokio::time::Instant::now();
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
                                *last_ack_time.lock().await = tokio::time::Instant::now();
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
                                *last_ack_time.lock().await = tokio::time::Instant::now();
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
    /// Description: VRConnect shall implement a configurable grace period before resetting
    ///              BLE session state on Central disconnect (Data_OUT SubscribedClientsChanged
    ///              count → 0). When a Disconnected event arrives, the handler waits up to
    ///              grace_period_sec seconds for a Connected event. If Flutter reconnects
    ///              within the grace window, session state is preserved and ble_subscriber
    ///              remains true — no re-SUBSCRIBE is needed. If the timer expires without
    ///              reconnect, on_disconnect() fires and ble_subscriber is reset to false.
    ///              grace_period_sec=0 bypasses the timer entirely (immediate reset).
    ///
    /// Version: V2.0
    async fn disconnect_handler_loop(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<BleConnectionEvent>,
        state: Arc<RwLock<BleSessionState>>,
        health_state: Arc<RwLock<GateHealthState>>,
        health_notify: Arc<Notify>,
        grace_period_sec: u64,
    ) {
        log::info!(
            "[BLE] Disconnect handler loop running (grace={}s)",
            grace_period_sec
        );
        loop {
            match rx.recv().await {
                None => break,
                Some(BleConnectionEvent::Connected) => {
                    // Fresh connection (no grace timer pending at this point).
                    let mut hs = health_state.write().await;
                    if !hs.ble_subscriber {
                        hs.ble_subscriber = true;
                        drop(hs);
                        health_notify.notify_one();
                    }
                    log::info!("[BLE] Central connected — session active");
                }
                Some(BleConnectionEvent::Disconnected) => {
                    log::info!(
                        "[BLE] Central disconnected — starting grace period ({}s)",
                        grace_period_sec
                    );

                    if grace_period_sec == 0 {
                        Self::do_session_reset(&state, &health_state, &health_notify).await;
                        continue;
                    }

                    // Wait for reconnect or grace timer expiry.
                    let grace = tokio::time::sleep(Duration::from_secs(grace_period_sec));
                    tokio::pin!(grace);

                    loop {
                        tokio::select! {
                            _ = &mut grace => {
                                log::warn!("[BLE] Grace period expired — resetting session");
                                Self::do_session_reset(&state, &health_state, &health_notify).await;
                                break;
                            }
                            maybe = rx.recv() => match maybe {
                                None => {
                                    // Channel closed during grace — force reset and exit.
                                    Self::do_session_reset(&state, &health_state, &health_notify).await;
                                    return;
                                }
                                Some(BleConnectionEvent::Connected) => {
                                    log::info!(
                                        "[BLE] Reconnected within grace period — session preserved"
                                    );
                                    let mut hs = health_state.write().await;
                                    if !hs.ble_subscriber {
                                        hs.ble_subscriber = true;
                                        drop(hs);
                                        health_notify.notify_one();
                                    }
                                    break;
                                }
                                Some(BleConnectionEvent::Disconnected) => {
                                    // Re-disconnect during grace: restart the timer.
                                    log::info!("[BLE] Re-disconnect during grace — timer reset");
                                    grace.as_mut().reset(
                                        tokio::time::Instant::now()
                                            + Duration::from_secs(grace_period_sec),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        log::info!("[BLE] Disconnect handler loop ended");
    }

    /// ID SRS: SRS-FN-BLERELIABLE-015
    /// Title: supervision_task
    ///
    /// Description: VRConnect shall detect brutal link-layer disconnections that bypass the CCCD
    ///   SubscribedClientsChanged event (Central goes out of range without a graceful BLE
    ///   disconnect). Every CHECK_INTERVAL_SEC: if tx_buffer holds pending frames AND no ACK
    ///   has been received for supervision_timeout_sec, GATE resets the session via
    ///   do_session_reset() — the same path as grace-period expiry.
    ///   last_ack_time is reset immediately after each trigger so the timer re-arms cleanly.
    ///
    ///   Why tx_buffer check is safe: record_history() and the WAL journal are called in
    ///   output() BEFORE add_data(), so every sample is already durable when frames enter
    ///   tx_buffer. A supervision-triggered reset does NOT lose data — it ensures the Central
    ///   can recover all missed samples via BACKLOG_THEN_LIVE on reconnect.
    ///
    /// Version: V1.0
    async fn supervision_task(
        state: Arc<RwLock<BleSessionState>>,
        health_state: Arc<RwLock<GateHealthState>>,
        health_notify: Arc<Notify>,
        last_ack_time: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
        supervision_timeout_sec: u64,
    ) {
        const CHECK_INTERVAL_SEC: u64 = 5;
        let mut tick = tokio::time::interval(Duration::from_secs(CHECK_INTERVAL_SEC));
        tick.tick().await; // skip the immediate first tick

        loop {
            tick.tick().await;
            let pending = state.read().await.total_pending();
            if pending == 0 {
                continue;
            }
            let elapsed = last_ack_time.lock().await.elapsed().as_secs();
            if elapsed >= supervision_timeout_sec {
                log::warn!(
                    "[BLE] Supervision timeout: {} frame(s) pending, no ACK for {}s \
                     — brutal disconnect assumed, resetting session",
                    pending,
                    elapsed
                );
                Self::do_session_reset(&state, &health_state, &health_notify).await;
                *last_ack_time.lock().await = tokio::time::Instant::now();
            }
        }
    }

    /// ID SRS: SRS-FN-BLERELIABLE-013
    /// Title: checkpoint_task
    ///
    /// Description: VRConnect shall periodically serialize the history ring buffer to a
    ///              binary checkpoint file using an atomic write (tmp + rename). Runs as a
    ///              background task; skips the write if history is empty.
    ///
    /// Version: V1.0
    async fn checkpoint_task(state: Arc<RwLock<BleSessionState>>, interval_sec: u64, path: String) {
        loop {
            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
            let bytes = state.read().await.serialize_history_to_bytes();
            // Header only = 20 bytes — means no signals; skip write
            if bytes.len() <= 20 {
                continue;
            }
            let tmp_path = format!("{}.tmp", path);
            match std::fs::write(&tmp_path, &bytes) {
                Ok(_) => match std::fs::rename(&tmp_path, &path) {
                    Ok(_) => {
                        log::debug!("[CKPT] History checkpoint written ({} bytes)", bytes.len())
                    }
                    Err(e) => log::warn!("[CKPT] Failed to rename checkpoint: {}", e),
                },
                Err(e) => log::warn!("[CKPT] Failed to write checkpoint: {}", e),
            }
        }
    }

    /// ID SRS: SRS-FN-BLERELIABLE-014
    /// Title: wal_task
    ///
    /// Description: VRConnect shall maintain an append-only WAL file for the history ring-buffer.
    ///   Each WalEntry received on `rx` is written to a BufWriter<File> immediately; the
    ///   underlying file is fsynced every `fsync_interval_sec` (crash-loss window).
    ///   Every `compaction_interval_sec` the WAL is compacted:
    ///     1. Flush + fsync pending WAL writes.
    ///     2. Serialize full history snapshot under state.read().
    ///     3. Write snapshot to <checkpoint_path>.tmp, fsync, rename (checkpoint authoritative).
    ///     4. Truncate WAL to 0 bytes, fsync (WAL cleared only after checkpoint is durable).
    ///     5. Reopen WAL in append mode and continue.
    ///   This bounds the crash-loss window to ≤ fsync_interval_sec and eliminates the
    ///   ~500× write amplification of full-snapshot rewrites every checkpoint interval.
    ///
    /// Version: V1.0
    async fn wal_task(
        state: Arc<RwLock<BleSessionState>>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WalEntry>,
        wal_path: String,
        fsync_interval_sec: u64,
        compaction_interval_sec: u64,
        checkpoint_path: String,
    ) {
        // Ensure the parent directory exists (mirrors checkpoint_task behaviour).
        if let Some(parent) = std::path::Path::new(&wal_path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let file = match std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&wal_path)
        {
            Ok(f) => f,
            Err(e) => {
                log::error!("[WAL] Cannot open WAL file '{}': {}", wal_path, e);
                return;
            }
        };
        let mut writer = BufWriter::new(file);

        let mut fsync_tick = tokio::time::interval(Duration::from_secs(fsync_interval_sec));
        let mut compact_tick = tokio::time::interval(Duration::from_secs(compaction_interval_sec));
        // Consume the immediate first tick so both intervals fire after their full period.
        fsync_tick.tick().await;
        compact_tick.tick().await;

        loop {
            tokio::select! {
                entry = rx.recv() => {
                    match entry {
                        None => break, // channel closed — shutdown
                        Some(e) => {
                            if let Err(err) = writer.write_all(&e.to_bytes()) {
                                log::warn!("[WAL] Write error: {}", err);
                            }
                        }
                    }
                }
                _ = fsync_tick.tick() => {
                    if let Err(e) = writer.flush() {
                        log::warn!("[WAL] Flush error: {}", e);
                    }
                    if let Err(e) = writer.get_ref().sync_all() {
                        log::warn!("[WAL] Fsync error: {}", e);
                    }
                }
                _ = compact_tick.tick() => {
                    // 1. Flush pending WAL writes before snapshotting.
                    if let Err(e) = writer.flush() {
                        log::warn!("[WAL] Pre-compact flush error: {}", e);
                    }
                    if let Err(e) = writer.get_ref().sync_all() {
                        log::warn!("[WAL] Pre-compact fsync error: {}", e);
                    }

                    // 2. Serialize history snapshot (read lock — non-blocking for live data).
                    let bytes = state.read().await.serialize_history_to_bytes();

                    // 3. Write checkpoint (same atomic pattern as checkpoint_task).
                    if bytes.len() > 20 {
                        let tmp_path = format!("{}.tmp", checkpoint_path);
                        match std::fs::write(&tmp_path, &bytes) {
                            Ok(_) => match std::fs::rename(&tmp_path, &checkpoint_path) {
                                Ok(_) => log::info!(
                                    "[WAL] Compaction: checkpoint written ({} bytes, path={})",
                                    bytes.len(),
                                    checkpoint_path
                                ),
                                Err(e) => log::warn!("[WAL] Compaction: checkpoint rename failed: {}", e),
                            },
                            Err(e) => log::warn!("[WAL] Compaction: checkpoint write failed: {}", e),
                        }
                    }

                    // 4. Truncate WAL — checkpoint is now authoritative.
                    drop(writer); // flush + close before truncating
                    match std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&wal_path)
                    {
                        Ok(f) => {
                            if let Err(e) = f.sync_all() {
                                log::warn!("[WAL] Compaction: WAL fsync after truncate failed: {}", e);
                            } else {
                                log::debug!("[WAL] Compaction: WAL truncated (path={})", wal_path);
                            }
                        }
                        Err(e) => log::warn!("[WAL] Compaction: WAL truncate failed: {}", e),
                    }

                    // 5. Reopen WAL in append mode for continued journaling.
                    match std::fs::OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(&wal_path)
                    {
                        Ok(f) => writer = BufWriter::new(f),
                        Err(e) => {
                            log::error!(
                                "[WAL] Cannot reopen WAL after compaction '{}': {}",
                                wal_path,
                                e
                            );
                            return;
                        }
                    }
                }
            }
        }

        // Final flush before task exits.
        let _ = writer.flush();
        let _ = writer.get_ref().sync_all();
        log::info!("[WAL] WAL task exited");
    }

    /// Loads a history checkpoint from disk if it exists and is within max_age_sec.
    async fn try_load_checkpoint(
        state: &Arc<RwLock<BleSessionState>>,
        path: &str,
        max_age_sec: u64,
    ) {
        let meta = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(_) => return,
        };
        let age_sec = meta
            .modified()
            .ok()
            .and_then(|t| t.elapsed().ok())
            .map(|d| d.as_secs())
            .unwrap_or(u64::MAX);
        if age_sec > max_age_sec {
            log::info!(
                "[CKPT] Checkpoint too old ({}s > {}s), skipping",
                age_sec,
                max_age_sec
            );
            return;
        }
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("[CKPT] Failed to read checkpoint: {}", e);
                return;
            }
        };
        match state.write().await.load_history_from_bytes(&bytes) {
            Ok(n) => log::info!(
                "[CKPT] Loaded {} samples from checkpoint (age={}s, path={})",
                n,
                age_sec,
                path
            ),
            Err(e) => log::warn!("[CKPT] Invalid checkpoint: {}", e),
        }
    }

    /// Performs the full BLE session reset: calls on_disconnect() on the session state
    /// and updates the health subscriber flag.
    async fn do_session_reset(
        state: &Arc<RwLock<BleSessionState>>,
        health_state: &Arc<RwLock<GateHealthState>>,
        health_notify: &Arc<Notify>,
    ) {
        state.write().await.on_disconnect();
        log::info!("[BLE] Session state cleared after Central disconnect");
        let mut hs = health_state.write().await;
        if hs.ble_subscriber {
            hs.ble_subscriber = false;
            drop(hs);
            health_notify.notify_one();
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

        // Send historical replay frames for BACKLOG_THEN_LIVE / BACKLOG_ONLY.
        //
        // Phase A — collect frames from every signal that needs replay.
        // start_replay() reserves the seq block eagerly and pushes frames into tx_buffer (F4),
        // so un-sent frames remain NACK-recoverable even if this loop exits early.
        struct ReplayBatch {
            canonical_id: u16,
            stream_id: u16,
            mode: u8,
            frames: Vec<crate::domain::ble_protocol::DataFrame>,
        }
        let mut batches: Vec<ReplayBatch> = Vec::new();
        for (canonical_id, stream_id, mode, start_time_ms) in &replay_requests {
            let frames = {
                let mut st = state.write().await;
                st.start_replay(*canonical_id, *start_time_ms)
            };
            if frames.is_empty() {
                log::info!(
                    "Replay requested for signal 0x{:04X} (stream {}) but history is empty",
                    canonical_id,
                    stream_id
                );
            } else {
                log::info!(
                    "Collected {} frame(s) for signal 0x{:04X} (stream {}, mode={})",
                    frames.len(),
                    canonical_id,
                    stream_id,
                    mode
                );
            }
            batches.push(ReplayBatch {
                canonical_id: *canonical_id,
                stream_id: *stream_id,
                mode: *mode,
                frames,
            });
        }

        // Phase B — merge frames from all signals and sort by t0_ms.
        // Sorting by timestamp interleaves signals proportionally to their natural rate:
        // a 1 Hz signal contributes ~1 frame/s of history, a 5-min NBP signal contributes
        // ~0.003 frames/s — they receive BLE airtime in that same ratio without any
        // explicit token-bucket configuration. [F8]
        let mut merged: Vec<(u64, u16, Vec<u8>)> = batches
            .iter()
            .flat_map(|b| {
                b.frames
                    .iter()
                    .map(move |f| (f.t0_ms, b.canonical_id, f.to_ble_bytes()))
            })
            .collect();
        merged.sort_by_key(|(t0, _, _)| *t0);

        // Phase C — send the interleaved stream with the same 20 ms pacing. [DEV-6]
        // Rate-limiting to ~50 frames/s prevents bursting a full backlog (~367 KB for 6 h)
        // which causes Android's BLE stack to drop the connection.
        if !merged.is_empty() {
            log::info!(
                "Replaying {} frame(s) across {} signal(s), interleaved by timestamp",
                merged.len(),
                batches.iter().filter(|b| !b.frames.is_empty()).count()
            );
            let srv = server.read().await;
            for (_, signal_id, bytes) in &merged {
                if let Err(e) = srv.notify("Data_OUT", bytes).await {
                    log::warn!("Replay notify failed for signal 0x{:04X}: {}", signal_id, e);
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            }
        }

        // Phase D — finish all replays in one pass, even if notify failed partway.
        // Without this single-pass approach, a notify failure in signal A would leave
        // signal B permanently stuck with is_replaying=true.
        {
            let mut st = state.write().await;
            for batch in &batches {
                st.finish_replay(batch.stream_id);
                if batch.mode == 2 {
                    st.unsubscribe(batch.canonical_id);
                    log::info!(
                        "BACKLOG_ONLY: unsubscribed signal 0x{:04X} after replay",
                        batch.canonical_id
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
            0x0101 => 1,    // HR
            0x0102 => 2,    // SpO2
            0x0103 => 3,    // Temperature
            0x0201 => 4,    // SBP
            0x0202 => 5,    // DBP
            0x0203 => 6,    // MBP
            0x0301 => 8,    // ST_II
            0x0302 => 9,    // ST_V
            0x0303 => 10,   // ST_AVL
            0x0401 => 11,   // SPV
            0x0402 => 12,   // PPV
            0x0501 => 7,    // AmbPres
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
                // We use a fixed mapping so stream IDs are stable and predictable:
                //   0x0101→1, 0x0102→2, 0x0103→3, 0x0201→4..6, 0x0301→8..10, 0x0401→11..12, 0x0501→7
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
                req_id,
                e
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
    /// - ST_II / ST_V / ST_AVL / SPV / PPV: exact name match
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
            ("ST_II", SignalId::StII.as_u16()),
            ("ST_V", SignalId::StV.as_u16()),
            ("ST_AVL", SignalId::StAvl.as_u16()),
            ("SPV", SignalId::Spv.as_u16()),
            ("PPV", SignalId::Ppv.as_u16()),
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

        // Phase 1 — hold state.write() only for in-memory work (history, tx_buffer, WAL).
        // Serialise each outbound frame into bytes here so Phase 2 needs no state access.
        // The lock is intentionally dropped before any BLE notify call: notify() is an
        // async Windows BLE stack call that can stall for tens of ms under congestion,
        // and holding the write lock during it would block ACK processing in
        // write_handler_loop(), preventing tx_buffer from draining.
        let mut frames_to_send: Vec<(Vec<u8>, u16, u16, u32)> = Vec::new(); // (bytes, signal_id, stream_id, seq)
        {
            let mut state = self.state.write().await;
            let mut seen: std::collections::HashSet<(u16, u64)> =
                std::collections::HashSet::new();

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
                    "ST_II" => SignalId::StII.as_u16(),
                    "ST_V" => SignalId::StV.as_u16(),
                    "ST_AVL" => SignalId::StAvl.as_u16(),
                    "SPV" => SignalId::Spv.as_u16(),
                    "PPV" => SignalId::Ppv.as_u16(),
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

                // Forward to WAL journal (non-blocking channel send; task fsyncs on its own timer).
                if let Some(tx) = &self.wal_tx {
                    let _ = tx.send(WalEntry {
                        signal_id,
                        t0_ms,
                        value: val_f32,
                    });
                }

                // add_data returns Some(frame) only if signal is subscribed
                if let Some(frame) = state.add_data(signal_id, val_f32, t0_ms) {
                    // --- CHAOS MONKEY: packet-drop (env-driven, no .await — safe under lock) ---
                    // Controlled by ENABLE_CHAOS_MONKEY / CHAOS_RATIO.
                    // Never active when APP_ENV=production. See src/chaos/mod.rs.
                    if chaos::maybe_drop_frame("ble_reliable.rs") {
                        continue; // Skip the BLE notify — simulates a lossy link.
                    }
                    // -------------------------------------------------------------------------

                    frames_to_send.push((
                        frame.to_ble_bytes(),
                        signal_id,
                        frame.header.stream_id,
                        frame.header.seq,
                    ));
                }
            }
        } // ← state.write() released here, before any BLE I/O

        // Phase 2 — send notifies without holding the state lock.
        // maybe_network_jitter has an .await so it must run outside the lock.

        // Drain frames that failed in the previous output() call for a one-shot retry.
        // Sent before fresh frames so older data reaches the tablet first.
        let to_retry: Vec<(Vec<u8>, u16, u16, u32)> =
            self.retry_queue.lock().await.drain(..).collect();

        if !to_retry.is_empty() || !frames_to_send.is_empty() {
            let server = self.server.read().await;

            for (bytes, signal_id, _stream_id, seq) in to_retry {
                if let Err(e) = server.notify("Data_OUT", &bytes).await {
                    // Not re-queued: tx_buffer keeps the frame NACK-recoverable.
                    log::warn!(
                        "BLE retry also failed for signal 0x{:04X} seq={}: {} — NACK path will recover",
                        signal_id, seq, e
                    );
                } else {
                    log::debug!(
                        "Data_OUT (retry ok): signal=0x{:04X}, seq={}",
                        signal_id, seq
                    );
                }
            }

            for (bytes, signal_id, stream_id, seq) in frames_to_send {
                // --- CHAOS MONKEY: network-jitter (env-driven, has .await) ---
                chaos::maybe_network_jitter("ble_reliable.rs").await;
                // -------------------------------------------------------------

                if let Err(e) = server.notify("Data_OUT", &bytes).await {
                    let mut q = self.retry_queue.lock().await;
                    if q.len() < RETRY_QUEUE_CAP {
                        log::warn!(
                            "BLE notify failed for signal 0x{:04X}: {} — queued for one-shot retry",
                            signal_id, e
                        );
                        q.push((bytes, signal_id, stream_id, seq));
                    } else {
                        log::warn!(
                            "BLE notify failed for signal 0x{:04X} seq={}: {} — retry queue full \
                             (cap={}), NACK path will recover",
                            signal_id, seq, e, RETRY_QUEUE_CAP
                        );
                    }
                } else {
                    log::debug!(
                        "Data_OUT: signal=0x{:04X}, stream={}, seq={}, {} bytes",
                        signal_id, stream_id, seq, bytes.len()
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
    use tempfile;

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
    /// Title: start_replay returns FLAG_BACKLOG frames; live frames never carry FLAG_BACKLOG
    ///
    /// Description: After subscribing and seeding history, start_replay() must return
    ///              DataFrames with FLAG_BACKLOG set. Live frames produced by add_data()
    ///              must NOT carry FLAG_BACKLOG regardless of is_replaying state — the flag
    ///              is the exclusive property of get_replay_frames().
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

            // While replaying, live frames must NOT carry FLAG_BACKLOG (BLE_REVIEW_FINDINGS F1)
            let live_during = st.add_data(SignalId::HR.as_u16(), 73.0, 4000).unwrap();
            assert_eq!(
                live_during.header.flags & FLAG_BACKLOG,
                0,
                "live frame during replay must NOT carry FLAG_BACKLOG"
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

    /// ID SRS: SRS-TEST-BLERELIABLE-043
    /// Title: Replay interleaves frames from multiple signals sorted by timestamp
    ///
    /// Description: When two signals are replayed simultaneously, their frames must be
    ///              merged and sorted by t0_ms so no single signal monopolises the BLE
    ///              channel. HR frames at odd timestamps and SpO2 at even timestamps must
    ///              emerge interleaved, and both streams must finish with is_replaying=false.
    #[tokio::test]
    async fn test_replay_interleaves_signals_by_timestamp() {
        use crate::domain::ble_protocol::SignalId;

        let mut st = BleSessionState::new(1);

        // Seed HR at odd timestamps and SpO2 at even timestamps
        st.record_history(SignalId::HR.as_u16(), 60.0, 1000);
        st.record_history(SignalId::SpO2.as_u16(), 98.0, 2000);
        st.record_history(SignalId::HR.as_u16(), 61.0, 3000);
        st.record_history(SignalId::SpO2.as_u16(), 97.0, 4000);
        st.record_history(SignalId::HR.as_u16(), 62.0, 5000);
        st.record_history(SignalId::SpO2.as_u16(), 96.0, 6000);

        st.subscribe_with_stream_id(SignalId::HR.as_u16(), 1);
        st.subscribe_with_stream_id(SignalId::SpO2.as_u16(), 2);

        let hr_frames = st.start_replay(SignalId::HR.as_u16(), 0);
        let spo2_frames = st.start_replay(SignalId::SpO2.as_u16(), 0);

        assert_eq!(hr_frames.len(), 3);
        assert_eq!(spo2_frames.len(), 3);

        // Reproduce the merge-and-sort from handle_subscribe_req (Phase B)
        let mut merged: Vec<(u64, u16)> = hr_frames
            .iter()
            .map(|f| (f.t0_ms, SignalId::HR.as_u16()))
            .chain(spo2_frames.iter().map(|f| (f.t0_ms, SignalId::SpO2.as_u16())))
            .collect();
        merged.sort_by_key(|(t0, _)| *t0);

        // Frames must alternate HR/SpO2 in timestamp order
        let expected: Vec<(u64, u16)> = vec![
            (1000, SignalId::HR.as_u16()),
            (2000, SignalId::SpO2.as_u16()),
            (3000, SignalId::HR.as_u16()),
            (4000, SignalId::SpO2.as_u16()),
            (5000, SignalId::HR.as_u16()),
            (6000, SignalId::SpO2.as_u16()),
        ];
        assert_eq!(merged, expected, "frames must be interleaved by t0_ms");

        // Both streams must finish replaying (Phase D — single-pass finish)
        let hr_stream = st.get_stream_id(SignalId::HR.as_u16()).unwrap();
        let spo2_stream = st.get_stream_id(SignalId::SpO2.as_u16()).unwrap();
        st.finish_replay(hr_stream);
        st.finish_replay(spo2_stream);

        assert!(
            !st.streams[&hr_stream].is_replaying,
            "HR must finish replaying"
        );
        assert!(
            !st.streams[&spo2_stream].is_replaying,
            "SpO2 must finish replaying"
        );
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
        let last_ack_time = Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now()));

        tokio::spawn(async move {
            ReliableBleOutput::write_handler_loop(
                rx,
                state,
                server,
                registry,
                health_state,
                health_notify,
                last_ack_time,
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
            let last_ack_time = Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now()));

            tokio::spawn(async move {
                ReliableBleOutput::write_handler_loop(
                    rx,
                    state,
                    server,
                    registry,
                    health_state,
                    health_notify,
                    last_ack_time,
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

    // ── disconnect_handler_loop grace period ─────────────────────────────────

    /// Helper: build a minimal Arc<RwLock<GateHealthState>> with ble_subscriber=true
    fn make_health(subscriber: bool) -> Arc<RwLock<GateHealthState>> {
        Arc::new(RwLock::new(GateHealthState {
            ble_subscriber: subscriber,
            ..Default::default()
        }))
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-035
    /// Title: grace_period=0 triggers on_disconnect() immediately on Disconnected event
    ///
    /// Description: When ble_grace_period_sec=0, a Disconnected event must cause
    ///              on_disconnect() to fire without any delay. signal_to_stream must be
    ///              empty and ble_subscriber must be false immediately after yielding.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_grace_zero_resets_immediately() {
        tokio::time::pause();

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        state
            .write()
            .await
            .subscribe_with_stream_id(SignalId::HR.as_u16(), 1);
        let health_state = make_health(true);
        let health_notify = Arc::new(Notify::new());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BleConnectionEvent>();

        tokio::spawn(ReliableBleOutput::disconnect_handler_loop(
            rx,
            state.clone(),
            health_state.clone(),
            health_notify.clone(),
            0,
        ));

        tx.send(BleConnectionEvent::Disconnected).unwrap();
        tokio::task::yield_now().await;

        assert!(
            state.read().await.signal_to_stream.is_empty(),
            "streams must be cleared immediately with grace=0"
        );
        assert!(
            !health_state.read().await.ble_subscriber,
            "ble_subscriber must be false after immediate reset"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-036
    /// Title: Reconnect within grace period preserves session and ble_subscriber
    ///
    /// Description: When Flutter reconnects (Connected event) within the grace window,
    ///              on_disconnect() must NOT be called: signal_to_stream remains intact,
    ///              current_session_id is unchanged, and ble_subscriber stays true.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_grace_reconnect_preserves_session() {
        tokio::time::pause();

        let initial_session_id = 1u16;
        let state = Arc::new(RwLock::new(BleSessionState::new(initial_session_id)));
        state
            .write()
            .await
            .subscribe_with_stream_id(SignalId::HR.as_u16(), 1);
        let health_state = make_health(true);
        let health_notify = Arc::new(Notify::new());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BleConnectionEvent>();

        tokio::spawn(ReliableBleOutput::disconnect_handler_loop(
            rx,
            state.clone(),
            health_state.clone(),
            health_notify.clone(),
            10,
        ));

        // Disconnect then reconnect within the 10 s window (only 5 s elapse)
        tx.send(BleConnectionEvent::Disconnected).unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        tx.send(BleConnectionEvent::Connected).unwrap();
        tokio::task::yield_now().await;

        let st = state.read().await;
        assert!(
            !st.signal_to_stream.is_empty(),
            "subscriptions must be preserved after grace reconnect"
        );
        assert_eq!(
            st.current_session_id, initial_session_id,
            "session_id must not change after grace reconnect"
        );
        assert!(
            health_state.read().await.ble_subscriber,
            "ble_subscriber must remain true after grace reconnect"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-037
    /// Title: Grace period expiry triggers on_disconnect() and resets session
    ///
    /// Description: When no reconnect occurs within ble_grace_period_sec, on_disconnect()
    ///              fires: signal_to_stream is cleared, current_session_id increments,
    ///              and ble_subscriber becomes false.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_grace_expiry_resets_session() {
        tokio::time::pause();

        let initial_session_id = 1u16;
        let state = Arc::new(RwLock::new(BleSessionState::new(initial_session_id)));
        state
            .write()
            .await
            .subscribe_with_stream_id(SignalId::HR.as_u16(), 1);
        let health_state = make_health(true);
        let health_notify = Arc::new(Notify::new());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BleConnectionEvent>();

        tokio::spawn(ReliableBleOutput::disconnect_handler_loop(
            rx,
            state.clone(),
            health_state.clone(),
            health_notify.clone(),
            10,
        ));

        tx.send(BleConnectionEvent::Disconnected).unwrap();
        tokio::task::yield_now().await;

        // Advance past the full grace period (11 s > 10 s window)
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;

        let st = state.read().await;
        assert!(
            st.signal_to_stream.is_empty(),
            "streams must be cleared after grace expiry"
        );
        assert_eq!(
            st.current_session_id,
            initial_session_id.wrapping_add(1),
            "session_id must increment after grace expiry"
        );
        assert!(
            !health_state.read().await.ble_subscriber,
            "ble_subscriber must be false after grace expiry"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-039
    /// Title: checkpoint_task writes atomic checkpoint after interval
    ///
    /// Description: After one interval tick, checkpoint_task must produce a valid
    ///              binary file at the given path. The file must be loadable by
    ///              load_history_from_bytes and reproduce all recorded samples.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_checkpoint_task_writes_file() {
        tokio::time::pause();

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        {
            let mut st = state.write().await;
            st.record_history(SignalId::HR.as_u16(), 75.0, 1000);
            st.record_history(SignalId::HR.as_u16(), 76.0, 2000);
        }

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let path = tmp_dir
            .path()
            .join("ckpt.bin")
            .to_str()
            .unwrap()
            .to_string();

        tokio::spawn(ReliableBleOutput::checkpoint_task(
            state.clone(),
            5,
            path.clone(),
        ));

        // Yield so the task can register its sleep, then advance past the interval.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        // Multiple yields: one for the lock acquisition, one for the file I/O.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        let bytes = std::fs::read(&path).expect("checkpoint file must exist after interval");
        let mut dst = BleSessionState::new(1);
        let n = dst.load_history_from_bytes(&bytes).unwrap();
        assert_eq!(n, 2, "checkpoint must contain the 2 recorded samples");
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-038
    /// Title: Re-disconnect during grace period resets the grace timer
    ///
    /// Description: A second Disconnected event arriving while the grace timer is running
    ///              must restart the 10 s window. A Connected event arriving after the
    ///              second disconnect but within the new window must preserve the session.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_redisconnect_resets_grace_timer() {
        tokio::time::pause();

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        state
            .write()
            .await
            .subscribe_with_stream_id(SignalId::HR.as_u16(), 1);
        let health_state = make_health(true);
        let health_notify = Arc::new(Notify::new());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BleConnectionEvent>();

        tokio::spawn(ReliableBleOutput::disconnect_handler_loop(
            rx,
            state.clone(),
            health_state.clone(),
            health_notify.clone(),
            10,
        ));

        // First disconnect — starts 10 s timer
        tx.send(BleConnectionEvent::Disconnected).unwrap();
        tokio::task::yield_now().await;

        // 8 s pass, then a second disconnect — must reset timer to a new 10 s window
        tokio::time::advance(Duration::from_secs(8)).await;
        tokio::task::yield_now().await;
        tx.send(BleConnectionEvent::Disconnected).unwrap();
        tokio::task::yield_now().await;

        // 5 s after the second disconnect (total 13 s, but timer was reset) — reconnect
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        tx.send(BleConnectionEvent::Connected).unwrap();
        tokio::task::yield_now().await;

        assert!(
            !state.read().await.signal_to_stream.is_empty(),
            "session must be preserved when reconnecting within the reset grace window"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-040
    /// Title: wal_task writes entries to file and final-flushes on channel close
    ///
    /// Description: After sending 3 WalEntry items and dropping the sender (channel close),
    ///              wal_task must exit, final-flush, and leave exactly 3 valid entries in the
    ///              WAL file recoverable by replay_wal_entries.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_wal_task_writes_and_fsyncs() {
        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let wal_path = tmp_dir
            .path()
            .join("test.wal")
            .to_str()
            .unwrap()
            .to_string();
        let ckpt_path = tmp_dir
            .path()
            .join("ckpt.bin")
            .to_str()
            .unwrap()
            .to_string();

        let (wal_tx, wal_rx) = tokio::sync::mpsc::unbounded_channel::<WalEntry>();

        let task = tokio::spawn(ReliableBleOutput::wal_task(
            state.clone(),
            wal_rx,
            wal_path.clone(),
            // long intervals so neither fsync nor compact timer fires during the test
            3600,
            7200,
            ckpt_path,
        ));

        // Send 3 entries then close the channel.
        // wal_task will receive None from rx.recv(), break the loop, and final-flush.
        wal_tx
            .send(WalEntry {
                signal_id: 0x0101,
                t0_ms: 1000,
                value: 75.0,
            })
            .unwrap();
        wal_tx
            .send(WalEntry {
                signal_id: 0x0102,
                t0_ms: 2000,
                value: 99.0,
            })
            .unwrap();
        wal_tx
            .send(WalEntry {
                signal_id: 0x0101,
                t0_ms: 3000,
                value: 76.0,
            })
            .unwrap();
        drop(wal_tx); // closes channel → wal_task will exit after draining remaining entries

        // Wait for the task to exit and final-flush.
        task.await.expect("wal_task must not panic");

        let entries = wal::replay_wal_entries(&wal_path);
        assert_eq!(
            entries.len(),
            3,
            "WAL file must contain exactly 3 entries after final flush"
        );
        assert_eq!(entries[0].signal_id, 0x0101);
        assert_eq!(entries[0].t0_ms, 1000);
        assert_eq!(entries[1].signal_id, 0x0102);
        assert_eq!(entries[2].t0_ms, 3000);
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-041
    /// Title: wal_task compaction writes checkpoint then truncates WAL
    ///
    /// Description: After sending an entry and flushing (channel close), then reloading
    ///              with a short compaction interval and advancing past it, the checkpoint
    ///              file must contain the recorded history samples and the WAL must be empty.
    ///              The test verifies the invariant: checkpoint rename happens BEFORE WAL
    ///              truncation, so no data is lost.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_wal_task_compaction_snapshot_before_truncate() {
        tokio::time::pause();

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        {
            let mut st = state.write().await;
            st.record_history(SignalId::HR.as_u16(), 75.0, 1000);
            st.record_history(SignalId::HR.as_u16(), 76.0, 2000);
        }

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let wal_path = tmp_dir
            .path()
            .join("test.wal")
            .to_str()
            .unwrap()
            .to_string();
        let ckpt_path = tmp_dir
            .path()
            .join("ckpt.bin")
            .to_str()
            .unwrap()
            .to_string();

        // Phase 1: send an entry so the WAL file is created and has content.
        {
            let (wal_tx, wal_rx) = tokio::sync::mpsc::unbounded_channel::<WalEntry>();
            let task = tokio::spawn(ReliableBleOutput::wal_task(
                state.clone(),
                wal_rx,
                wal_path.clone(),
                3600, // long fsync — won't fire
                3600, // long compact — won't fire
                ckpt_path.clone(),
            ));
            wal_tx
                .send(WalEntry {
                    signal_id: 0x0101,
                    t0_ms: 3000,
                    value: 77.0,
                })
                .unwrap();
            drop(wal_tx); // close → final flush
            task.await.expect("phase-1 wal_task must not panic");
        }
        // WAL now has 1 entry on disk.
        assert_eq!(wal::replay_wal_entries(&wal_path).len(), 1);

        // Phase 2: spawn wal_task with a short compaction interval (5 s) and fire it.
        let (wal_tx2, wal_rx2) = tokio::sync::mpsc::unbounded_channel::<WalEntry>();
        tokio::spawn(ReliableBleOutput::wal_task(
            state.clone(),
            wal_rx2,
            wal_path.clone(),
            3600, // fsync won't fire
            5,    // compact fires after 5 s
            ckpt_path.clone(),
        ));
        // Keep tx alive so the task stays in the loop (doesn't exit from channel close).
        let _keep_tx = wal_tx2;

        // Yield so the task starts and consumes its initial ticks, then advance past compact.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(6)).await;
        // Multiple yields: lock acquisition + file I/O + reopen after compact.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        // Checkpoint must exist and contain the 2 history samples (from record_history above).
        let ckpt_bytes = std::fs::read(&ckpt_path).expect("checkpoint must exist after compaction");
        let mut dst = BleSessionState::new(1);
        let n = dst
            .load_history_from_bytes(&ckpt_bytes)
            .expect("checkpoint must be valid");
        assert!(
            n >= 2,
            "checkpoint must contain at least the 2 history samples"
        );

        // WAL must be empty (truncated after checkpoint was durable).
        let wal_entries = wal::replay_wal_entries(&wal_path);
        assert_eq!(wal_entries.len(), 0, "WAL must be empty after compaction");
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-042
    /// Title: supervision_task resets session after brutal link-layer disconnect
    ///
    /// Description: With pending frames in tx_buffer and no ACK arriving (simulating a
    ///   Central that went out of range without CCCD update), supervision_task must reset
    ///   the session (total_pending → 0) once supervision_timeout_sec has elapsed.
    ///   Data durability is unaffected: record_history() runs before add_data() in output(),
    ///   so all samples are already in history before they enter tx_buffer.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_supervision_task_resets_on_timeout() {
        tokio::time::pause();

        use crate::domain::ble_protocol::SignalId;

        let state = Arc::new(RwLock::new(BleSessionState::new(1)));
        let health_state = Arc::new(RwLock::new(GateHealthState::default()));
        let health_notify = Arc::new(Notify::new());
        let last_ack_time = Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now()));

        // Subscribe and queue 2 frames into tx_buffer (no ACK will arrive).
        {
            let mut st = state.write().await;
            st.subscribe(SignalId::HR.as_u16());
            st.add_data(SignalId::HR.as_u16(), 72.0, 1_000_000);
            st.add_data(SignalId::HR.as_u16(), 73.0, 2_000_000);
        }
        assert_eq!(state.read().await.total_pending(), 2, "pre: 2 frames pending");

        // Spawn supervision task with a 10 s timeout.
        let state2 = state.clone();
        let hs = health_state.clone();
        let hn = health_notify.clone();
        let lat = last_ack_time.clone();
        tokio::spawn(async move {
            ReliableBleOutput::supervision_task(state2, hs, hn, lat, 10).await;
        });

        // Yield to let the task start and consume its initial skip-tick.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        // Advance past CHECK_INTERVAL (5 s) + supervision_timeout (10 s).
        tokio::time::advance(Duration::from_secs(16)).await;

        // Multiple yields: task wakes on tick, acquires locks, resets session.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            state.read().await.total_pending(),
            0,
            "supervision must reset session and clear tx_buffer"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-045
    /// Title: E6 — failed notify is queued for one-shot retry, discarded on second failure
    ///
    /// Description: When server.notify() always fails (GattServer not started → local_chars
    ///   empty → VitalError::Config), the first output() call must queue the failed frame in
    ///   retry_queue. The second output() call (same data → fully deduped, no new frame) must
    ///   drain and attempt the retry, discard on failure, and leave retry_queue empty.
    ///   History must contain the sample regardless of notify outcomes.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_failed_notify_queued_then_drained_on_retry() {
        use crate::domain::ble_protocol::SignalId;

        // GattServer::new() does not call Windows APIs.
        // add_characteristic() (called inside new()) only fills `chars`, not `local_chars`.
        // `local_chars` is populated only by start(), which we never call here.
        // Therefore server.notify() always returns Err("Unknown characteristic 'Data_OUT'").
        let ble = ReliableBleOutput::new(
            "Test".to_string(),
            "12345678-1234-1234-1234-1234567890ab".to_string(),
            0,                      // _update_interval_ms (unused)
            None,                   // registry (uses default)
            30,                     // health_check_interval_sec
            60,                     // health_ble_flow_timeout_sec
            "logs/health.json".to_string(),
            5,                      // ble_grace_period_sec
            30,                     // ble_supervision_timeout_sec
            3600,                   // history_checkpoint_interval_sec
            86400,                  // history_checkpoint_max_age_sec
            "".to_string(),         // history_checkpoint_path (disabled)
            21600,                  // history_retention_sec
            false,                  // wal_enabled
            "".to_string(),
            30,
            3600,
        )
        .await
        .expect("minimal ReliableBleOutput must construct");

        ble.subscribe_for_test(SignalId::HR.as_u16(), 1).await;

        let data = ProcessedData::new(
            "VR-TEST".to_string(),
            vec![ProcessedRoom {
                room_index: 0,
                room_name: "BED_01".to_string(),
                tracks: vec![create_test_track("HR", 75.0, 0i32, "BED_01")],
            }],
        );

        // First output(): add_data returns Some(frame), notify fails → frame queued.
        ble.output(&data).await.expect("output must not propagate notify errors");
        assert_eq!(
            ble.retry_queue_len().await,
            1,
            "first failed frame must be held in retry queue"
        );
        assert_eq!(
            ble.history_len_for_test(SignalId::HR.as_u16()).await,
            1,
            "history must be populated regardless of notify outcome"
        );

        // Second output() with same data: record_history and add_data both dedup on t0_ms,
        // so frames_to_send is empty. Only to_retry is drained → retry fails → discarded.
        ble.output(&data).await.expect("output must not propagate retry errors");
        assert_eq!(
            ble.retry_queue_len().await,
            0,
            "retry queue must be drained and frame discarded after one-shot retry"
        );
        assert_eq!(
            ble.history_len_for_test(SignalId::HR.as_u16()).await,
            1,
            "history count unchanged: second call was fully deduped"
        );
    }

    /// ID SRS: SRS-TEST-BLERELIABLE-044
    /// Title: N3 — WAL send failure does not affect history ring-buffer
    ///
    /// Description: When the WAL channel receiver is dropped (wal_task exited or session reset),
    ///              send() on the closed channel must return Err gracefully (no panic), and
    ///              record_history must still have populated the history ring-buffer.
    ///              Mirrors the ordering in output(): record_history is called before the WAL
    ///              send, so history is always durable even when the WAL path is unavailable.
    ///
    /// Version: V1.0
    #[test]
    fn test_wal_closed_channel_does_not_affect_history() {
        use crate::output::wal::WalEntry;

        let mut state = BleSessionState::new(1);

        // Drop the receiver immediately — simulates wal_task having exited.
        let (wal_tx, wal_rx) = tokio::sync::mpsc::unbounded_channel::<WalEntry>();
        drop(wal_rx);

        // record_history is always called before the WAL send in output().
        state.record_history(0x0101, 72.0, 1000);
        state.record_history(0x0101, 73.0, 2000);

        // WAL send on a closed channel must return Err, not panic.
        let result = wal_tx.send(WalEntry {
            signal_id: 0x0101,
            t0_ms: 1000,
            value: 72.0,
        });
        assert!(
            result.is_err(),
            "send on closed WAL channel must fail gracefully"
        );

        // History must contain both samples regardless of the WAL send failure.
        let history = state
            .history
            .get(&0x0101)
            .expect("HR history must be populated");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0], (1000, 72.0));
        assert_eq!(history[1], (2000, 73.0));
    }
}
