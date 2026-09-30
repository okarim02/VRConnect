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
    InboundFrame, SignalId, SignalRegistry, SubscribeReq, SubscribeReqEntry, SubscribeRsp,
    SubscribeRspItem, MSG_SUBSCRIBE_REQ, SUB_OP_SUBSCRIBE, SUB_OP_UNSUBSCRIBE,
};
use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use crate::output::ble_gatt::{
    BleConnectionEvent, CharProperty, GattServer, WriteEvent, CCCD_SUBSCRIBERS_LOST_MSG,
};
use crate::output::ble_session::BleSessionState;
use crate::output::health::{build_payload, read_os_snapshot, GateHealthState};
use crate::output::wal::{self, WalEntry};
use crate::utils::chaos;
use std::io::{BufWriter, Write as IoWrite};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};

/// A single one-shot retry entry: `(frame_bytes, signal_id, stream_id, seq)`.
type RetryEntry = (Vec<u8>, u16, u16, u32);

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
    /// Shared last-ACK timestamp — reset on ACK receipt, on successful subscribe, and in
    /// output() on the tx_buffer idle→busy transition (state.total_pending() 0 → >0).
    /// That last reset point matters for throttled streams (period_ms): without it,
    /// elapsed-since-ACK is measured from the last real ACK (~period_ms ago), so
    /// supervision_task can race the client's ACK and misfire a "brutal disconnect" reset
    /// on a perfectly healthy, merely-throttled connection. Resetting only on idle→busy
    /// (not on every send) keeps a truly dead, unthrottled link detectable: pending never
    /// returns to 0 for it, so the clock never gets refreshed and supervision still fires.
    last_ack_time: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
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
    retry_queue: tokio::sync::Mutex<Vec<RetryEntry>>,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        device_name: String,
        service_uuid_str: String,
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
            last_ack_time: Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now())),
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

        self.start_background_tasks().await;

        // 9. Start GATT server
        {
            let mut server = self.server.write().await;
            server.start().await?;
        }
        log::info!("Reliable BLE GATT server started successfully (IDT v1)");
        log::info!("Waiting for BLE client connections...");
        Ok(())
    }

    /// ID SRS: SRS-FN-BLERELIABLE-017
    /// Title: start_background_tasks
    ///
    /// Description: VRConnect shall prepare the catalog and spawn all background tasks
    ///              (steps 1–8 of `start`: write handler, ACK watchdog, disconnect
    ///              handler, supervision, health, checkpoint load/save, WAL replay/task).
    ///              Contains no hardware access — the WinRT GATT service is only created
    ///              by `start` (step 9), which keeps this part unit-testable.
    ///
    /// Version: V1.0
    async fn start_background_tasks(&self) {
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

        // write_handler_loop updates self.last_ack_time on every ACK received;
        // supervision_task reads it to detect a frozen ACK channel (brutal link-layer drop).
        let last_ack_time = self.last_ack_time.clone();

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
                let grace_period = Duration::from_secs(self.ble_grace_period_sec);
                tokio::spawn(async move {
                    Self::disconnect_handler_loop(
                        rx,
                        state,
                        health_state,
                        health_notify,
                        grace_period,
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
                Self::supervision_task(
                    state,
                    health_state,
                    health_notify,
                    last_ack_time_sv,
                    timeout,
                )
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
    }

    /// ID SRS: SRS-FN-BLERELIABLE-012
    /// Title: apply_ack_and_retransmit
    ///
    /// Description: VRConnect shall apply a parsed ACK (session_id/stream_id/ack_upto/bitmap —
    /// already decoded from any of the three inbound wire formats: IDT, MyPredi, or legacy
    /// Flutter) and dispatch any resulting retransmits. Factors out the retransmit-loop +
    /// last_ack_time update that the three Data_IN ACK branches in `write_handler_loop` used
    /// to duplicate independently. `verbose_retransmit_log` selects between the two distinct
    /// retransmit-success log styles those branches used (IDT/MyPredi vs. Flutter) — preserved
    /// as-is rather than unified, since changing operator-facing log output is a separate
    /// decision from removing the duplication.
    ///
    /// Version: V1.0
    #[allow(clippy::too_many_arguments)]
    async fn apply_ack_and_retransmit(
        state: &Arc<RwLock<BleSessionState>>,
        server: &Arc<RwLock<GattServer>>,
        last_ack_time: &Arc<tokio::sync::Mutex<tokio::time::Instant>>,
        session_id: u16,
        stream_id: u16,
        ack_upto: u32,
        bitmap: &[u8; 8],
        verbose_retransmit_log: bool,
    ) {
        let retransmits = {
            let mut st = state.write().await;
            st.handle_ack_with_bitmap(session_id, stream_id, ack_upto, bitmap)
        };
        if !retransmits.is_empty() {
            let srv = server.read().await;
            for frame in retransmits {
                let bytes = frame.to_ble_bytes();
                if let Err(e) = srv.notify("Data_OUT", &bytes).await {
                    log::warn!("Retransmit failed for seq {}: {}", frame.header.seq, e);
                } else if verbose_retransmit_log {
                    log::info!(
                        "--> RETRANSMITTED seq {} for stream {}",
                        frame.header.seq,
                        frame.header.stream_id
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
                            Self::apply_ack_and_retransmit(
                                &state,
                                &server,
                                &last_ack_time,
                                ack.session_id,
                                ack.stream_id,
                                ack.ack_upto,
                                &ack.bitmap,
                                true,
                            )
                            .await;
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
                            // MyPredi sends ACK with a 24-byte header (not IDT 13b):
                            // magic+header(24b)+payload(17b)+CRC32C(4b) = 45 bytes total.
                            // Try MyPredi format first, then legacy Flutter 17-byte fallback.
                            if let Some(ack) = AckFrame::from_mypredi_bytes(data) {
                                log::info!(
                                    "MyPredi ACK: stream={}, ack_upto={}, bitmap={:02X?}",
                                    ack.stream_id,
                                    ack.ack_upto,
                                    ack.bitmap
                                );
                                Self::apply_ack_and_retransmit(
                                    &state,
                                    &server,
                                    &last_ack_time,
                                    ack.session_id,
                                    ack.stream_id,
                                    ack.ack_upto,
                                    &ack.bitmap,
                                    true,
                                )
                                .await;
                            } else if let Some(ack) = AckFrame::from_flutter_bytes(data) {
                                log::debug!(
                                    "Flutter ACK: stream={}, ack_upto={}, bitmap={:02X?}",
                                    ack.stream_id,
                                    ack.ack_upto,
                                    ack.bitmap
                                );
                                Self::apply_ack_and_retransmit(
                                    &state,
                                    &server,
                                    &last_ack_time,
                                    ack.session_id,
                                    ack.stream_id,
                                    ack.ack_upto,
                                    &ack.bitmap,
                                    false,
                                )
                                .await;
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
                            } else if let Some((req_id, entries)) =
                                parse_idt_wrapped_tlv_subscribe_req(data)
                            {
                                log::info!(
                                    "Subscribe: MyPredi format wrapped in IDT envelope — req_id={}, signals={:?}",
                                    req_id,
                                    entries
                                );
                                Self::handle_tlv_subscribe(
                                    req_id, entries, &state, &server, &registry,
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
                    } else if let Some((req_id, entries)) = parse_tlv_subscribe_req(data) {
                        // Flutter central sends a custom TLV format (byte[0]=0x20)
                        // instead of an IDT-framed SUBSCRIBE_REQ. Accept as fallback.
                        // Entries also carry an optional per-signal period_ms request.
                        log::info!(
                            "Subscribe: Flutter TLV format detected — req_id={}, signals={:?}",
                            req_id,
                            entries
                        );
                        Self::handle_tlv_subscribe(req_id, entries, &state, &server, &registry)
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
                    // Reset supervision timer on successful subscribe so Flutter has a full
                    // supervision_timeout_sec window to send its first ACK.  Without this,
                    // a stale last_ack_time (e.g. from VRConnect startup or the previous
                    // session) causes the supervision task to fire within seconds of the
                    // SUBSCRIBE_REQ, resetting the session mid-handshake and pushing Flutter
                    // into a SUBSCRIBE_REQ retry loop (observed: 635s elapsed → immediate
                    // fire → 14-minute loop with 30+ retries, session_id cycling 2→3).
                    if ble_active {
                        *last_ack_time.lock().await = tokio::time::Instant::now();
                        log::debug!("[supervision] Timer reset after successful subscribe");
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

    /// ID SRS: SRS-FN-BLERELIABLE-016
    /// Title: spawn_grace_timer
    ///
    /// Description: VRConnect shall arm the grace-period timer on a dedicated OS thread
    ///              instead of tokio::time::sleep. The tokio time driver is only polled
    ///              when a worker thread parks; under runtime saturation (observed during
    ///              Socket.IO handshake error bursts, os error 10053) a 10 s sleep fired
    ///              after 2 min 22 s. An OS thread sleeping with std::thread::sleep and
    ///              signalling through a oneshot channel wakes the handler task directly
    ///              via the scheduler, independent of the time driver. If thread spawn
    ///              fails, the sender is dropped and the receiver resolves immediately —
    ///              failing safe towards an immediate session reset rather than a session
    ///              that never resets.
    ///
    /// Version: V1.0
    fn spawn_grace_timer(duration: Duration) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = std::thread::Builder::new()
            .name("ble-grace-timer".into())
            .spawn(move || {
                std::thread::sleep(duration);
                let _ = tx.send(());
            });
        rx
    }

    /// ID SRS: SRS-FN-BLERELIABLE-012
    /// Title: disconnect_handler_loop
    ///
    /// Description: VRConnect shall implement a configurable grace period before resetting
    ///              BLE session state on Central disconnect (Data_OUT SubscribedClientsChanged
    ///              count → 0). When a Disconnected event arrives, the handler waits up to
    ///              grace_period for a Connected event. If Flutter reconnects
    ///              within the grace window, session state is preserved and ble_subscriber
    ///              remains true — no re-SUBSCRIBE is needed. If the timer expires without
    ///              reconnect, on_disconnect() fires and ble_subscriber is reset to false.
    ///              grace_period=0 bypasses the timer entirely (immediate reset).
    ///              The timer runs on a dedicated OS thread (spawn_grace_timer) so its
    ///              expiry cannot be delayed by tokio time-driver starvation.
    ///
    /// Version: V2.1
    async fn disconnect_handler_loop(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<BleConnectionEvent>,
        state: Arc<RwLock<BleSessionState>>,
        health_state: Arc<RwLock<GateHealthState>>,
        health_notify: Arc<Notify>,
        grace_period: Duration,
    ) {
        log::info!(
            "[BLE] Disconnect handler loop running (grace={:?})",
            grace_period
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
                        "{CCCD_SUBSCRIBERS_LOST_MSG} — starting grace period ({:?})",
                        grace_period
                    );

                    if grace_period.is_zero() {
                        Self::do_session_reset(&state, &health_state, &health_notify).await;
                        continue;
                    }

                    // Wait for reconnect or grace timer expiry. OS-thread timer:
                    // immune to tokio time-driver starvation (SRS-FN-BLERELIABLE-016).
                    let mut grace_rx = Self::spawn_grace_timer(grace_period);

                    loop {
                        tokio::select! {
                            _ = &mut grace_rx => {
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
                                    // Dropping the old receiver abandons the previous
                                    // timer thread; its send fails silently on wake-up.
                                    log::info!("[BLE] Re-disconnect during grace — timer reset");
                                    grace_rx = Self::spawn_grace_timer(grace_period);
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
    ///
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
                        // item.period_ms == 0 means "not specified" on the wire (IDT strict
                        // SubscribeItem has no separate optionality bit for this field).
                        let requested_period_ms = (item.period_ms > 0).then_some(item.period_ms);
                        let (effective_period_ms, gate_period_ms) =
                            Self::negotiate_period_ms(requested_period_ms, meta.nominal_period_ms);
                        let stream_id =
                            st.subscribe_with_period(canonical_id, None, gate_period_ms);
                        rsp_items.push(SubscribeRspItem {
                            source_id: meta.source_id,
                            signal_id: canonical_id,
                            stream_id,
                            effective_period_ms,
                            effective_batch_max: 1,
                        });
                        log::info!(
                            "SUBSCRIBE: signal 0x{:04X} → stream {} (mode={}{})",
                            canonical_id,
                            stream_id,
                            item.mode,
                            if gate_period_ms > 0 {
                                format!(", throttled to {} ms", effective_period_ms)
                            } else {
                                String::new()
                            }
                        );
                        // FORCE_BACKLOG_REPLAY=true overrides Flutter mode=0 (LIVE) → mode=1
                        // (BACKLOG_THEN_LIVE) so historical replay triggers without a Flutter update.
                        let force_replay = std::env::var("FORCE_BACKLOG_REPLAY")
                            .ok()
                            .and_then(|v| v.parse::<bool>().ok())
                            .unwrap_or(false);
                        let effective_mode = if force_replay && item.mode == 0 {
                            log::info!(
                                "[backlog] FORCE_BACKLOG_REPLAY active — overriding mode 0→1 for signal 0x{:04X}",
                                canonical_id
                            );
                            1u8
                        } else {
                            item.mode
                        };
                        // Queue replay if mode=1 (BACKLOG_THEN_LIVE) or mode=2 (BACKLOG_ONLY)
                        if effective_mode == 1 || effective_mode == 2 {
                            replay_requests.push((
                                canonical_id,
                                stream_id,
                                effective_mode,
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
        // - Data_OUT is intentionally skipped: MyPredi treats all Data_OUT frames as DATA_FRAMEs
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
            // its frame buffer (reads period_ms bytes as payloadLen → "Bad Magic").
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

        // Phase C — send the interleaved stream with the same 20 ms pacing.
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

    /// Negotiate the per-stream `period_ms` throttle for a SUBSCRIBE_REQ item.
    ///
    /// `nominal` (the signal's catalog rate) is a floor: a client cannot ask to go
    /// faster than the nominal rate, but a slower rate is honored as requested.
    /// Returns `(effective_period_ms, gate_period_ms)`:
    /// - `effective_period_ms` is what SUBSCRIBE_RSP reports back to the client.
    /// - `gate_period_ms` is what is stored on the `StreamEntry` throttle gate; it is
    ///   `0` (gate inactive) whenever the client made no request at all — this is the
    ///   case for every app deployed before this field existed, so their live
    ///   throughput is completely unchanged.
    ///
    /// This logic is factored out as a pure function because `handle_tlv_subscribe`
    /// and `handle_subscribe_req` require a real `GattServer` and are never exercised
    /// directly by unit tests (existing tests replicate their logic by hand — see the
    /// note above `test_subscribe_op_replaces_prior_subscriptions_via_unsubscribe_all`).
    /// A pure function keeps the one part of the negotiation that deserves coverage
    /// testable without that hardware dependency.
    ///
    /// ID SRS: SRS-FN-BLERELIABLE-018
    /// Version: V1.0
    fn negotiate_period_ms(requested: Option<u32>, nominal: u32) -> (u32, u32) {
        match requested {
            Some(p) if p > 0 => {
                let effective = p.max(nominal);
                (effective, effective)
            }
            _ => (nominal, 0),
        }
    }

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

    /// Handle a Flutter TLV SUBSCRIBE_REQ (byte[0]=0x20).
    ///
    /// Normalizes legacy signal IDs (1/2/3) to IDT compound IDs via the registry,
    /// assigns stream IDs matching the raw signal ID (HR=1, SpO2=2, Temp=3) so Flutter's
    /// hardcoded `activeStreams` map aligns, then sends SUBSCRIBE_RSP on Data_OUT.
    ///
    /// A 300 ms delay before the RSP notify is required because the Flutter app enables
    /// CCCD *after* writing to the Subscribe characteristic.
    async fn handle_tlv_subscribe(
        req_id: u16,
        entries: Vec<SubscribeReqEntry>,
        state: &Arc<RwLock<BleSessionState>>,
        server: &Arc<RwLock<GattServer>>,
        registry: &Arc<SignalRegistry>,
    ) {
        let session_id = state.read().await.current_session_id;
        let mut rsp_items: Vec<SubscribeRspItem> = Vec::new();

        {
            let mut st = state.write().await;
            st.unsubscribe_all();
            for (raw_id, requested_period_ms) in &entries {
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
                let meta = registry.get(canonical_id).unwrap();
                let (effective_period_ms, gate_period_ms) =
                    Self::negotiate_period_ms(*requested_period_ms, meta.nominal_period_ms);
                let stream_id =
                    st.subscribe_with_period(canonical_id, Some(flutter_sid), gate_period_ms);
                // RSP stream_id encoded as-is (Flutter ignores the RSP stream_id).
                // Future IDT-compliant clients will read it correctly in LE.
                rsp_items.push(SubscribeRspItem {
                    source_id: meta.source_id,
                    signal_id: canonical_id,
                    stream_id,
                    effective_period_ms,
                    effective_batch_max: 1,
                });
                if gate_period_ms > 0 {
                    log::info!(
                        "TLV SUBSCRIBE: signal 0x{:04X} → stream {} (throttled to {} ms, requested {:?})",
                        canonical_id,
                        stream_id,
                        effective_period_ms,
                        requested_period_ms
                    );
                } else {
                    log::info!(
                        "TLV SUBSCRIBE: signal 0x{:04X} → stream {}",
                        canonical_id,
                        stream_id
                    );
                }
            }
        }

        if rsp_items.is_empty() {
            return;
        }

        // Send SUBSCRIBE_RSP on Data_OUT as a full 24-byte IDT frame.
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
            // Was tx_buffer empty before this batch? Used below to detect the idle→busy
            // transition — the only point where last_ack_time needs a fresh window (see
            // ReliableBleOutput::supervision_task and last_ack_time's doc comment).
            let was_idle = state.total_pending() == 0;
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
            // Idle→busy transition (still under the state lock, so supervision_task can
            // never observe the new pending frames alongside a stale last_ack_time — it
            // reads total_pending() then last_ack_time in that same order). A dead link
            // never returns to pending==0, so this never re-arms for it; only a throttled
            // stream coming back from an idle gap gets a fresh window.
            if was_idle && state.total_pending() > 0 {
                *self.last_ack_time.lock().await = tokio::time::Instant::now();
            }
        } // ← state.write() released here, before any BLE I/O

        // Phase 2 — send notifies without holding the state lock.
        // maybe_network_jitter has an .await so it must run outside the lock.

        // Drain frames that failed in the previous output() call for a one-shot retry.
        // Sent before fresh frames so older data reaches the tablet first.
        let to_retry: Vec<RetryEntry> = self.retry_queue.lock().await.drain(..).collect();

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
                        signal_id,
                        seq
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
                            signal_id,
                            e
                        );
                        q.push((bytes, signal_id, stream_id, seq));
                    } else {
                        log::warn!(
                            "BLE notify failed for signal 0x{:04X} seq={}: {} — retry queue full \
                             (cap={}), NACK path will recover",
                            signal_id,
                            seq,
                            e,
                            RETRY_QUEUE_CAP
                        );
                    }
                } else {
                    log::debug!(
                        "Data_OUT: signal=0x{:04X}, stream={}, seq={}, {} bytes",
                        signal_id,
                        stream_id,
                        seq,
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

            // Re-read the GATT advertising status every heartbeat. Nothing else in
            // GATE observes it: it is set once at startup and, if it dies afterwards
            // (driver fault, machine sleep, stack reset), no log line reports it.
            let adv_state = server.read().await.advertising_state().await;
            match adv_state {
                Some(state) if state.is_started() => {
                    log::debug!("[health] GATT advertising status: {}", state);
                }
                Some(state) => {
                    log::warn!(
                        "[health] GATT advertising status: {} — expected Started. \
                         The device is no longer discoverable; a Central that drops \
                         will not be able to reconnect.",
                        state
                    );
                }
                None => {
                    log::warn!(
                        "[health] GATT advertising status unreadable (server not started \
                         or WinRT read failed) — reporting adv=0"
                    );
                }
            }

            {
                let mut gate = health_state.write().await;
                gate.adv_started = adv_state.map(|s| s.is_started());
            }

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
                            "[health] Health payload sent ({} bytes, ok={} gate={} sio={} ble={} flow={} vr={} disk={} wd_vr={} wd_gate={} adv={})",
                            bytes.len(), payload.ok, payload.gate, payload.sio,
                            payload.ble, payload.flow, payload.vr, payload.disk,
                            payload.wd_vr, payload.wd_gate, payload.adv
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
mod tests;
