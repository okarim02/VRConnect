// /src/output/ble_session.rs
// Module: output.ble_session
// Purpose: Multi-stream Session Engine for the IDT ("ICU Data Transport") BLE protocol — V1.0
//
//          Each subscribed signal gets its own independent IDT stream:
//            signal_id → stream_id (allocated at subscribe time, idempotent)
//            per-stream: sequence counter + retransmit buffer (VecDeque<DataFrame>)
//
//          ACK handling: cumulative (ack_upto), no bitmap.
//          NACK handling: explicit seq_list; frames returned with FLAG_RETRANSMIT.
//          Session change: if a new session_id is detected in an ACK, all buffers reset.
//
//          Isolated from Bluetooth radio for safe unit testing.

use crate::domain::ble_protocol::{DataFrame, FLAG_BACKLOG, FLAG_RETRANSMIT};
use std::collections::{HashMap, VecDeque};

// ─────────────────────────────────────────────────────────────────────────────
// StreamEntry
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLESESSION-001
/// Title: StreamEntry
///
/// Description: One active IDT stream bound to a single signal_id.
///              Each stream has its own sequence counter and retransmit buffer,
///              independent from all other streams.
///
/// Version: V1.0
pub struct StreamEntry {
    /// Allocated IDT stream_id for this signal
    pub stream_id: u16,
    /// IDT signal_id (e.g. 0x0101=HR, 0x0102=SpO2, 0x0103=Temperature)
    pub signal_id: u16,
    /// Source identifier (always 1 for scope signals in V1)
    pub source_id: u8,
    /// Last sent sequence number for this stream (0 = no frame sent yet)
    pub last_seq: u32,
    /// Timestamp of the last DATA_FRAME emitted on this stream (ms since epoch).
    /// `None` = no frame sent yet (first sample always passes).
    /// Used to deduplicate cross-message duplicates: VitalRecorder uses a sliding
    /// window and may re-send the same timestamp in consecutive Socket.IO messages.
    /// A new sample is only forwarded if t0_ms > last_t0_ms.
    pub last_t0_ms: Option<u64>,
    /// Effective period_ms negotiated for this stream's live BLE output. `0` = the
    /// client requested no throttling (every deployed app today) — the gate in
    /// add_data() is inactive and live throughput is unchanged. Distinct from
    /// `nominal_period_ms` in the signal catalog: this is the *gate* value, already
    /// floored to the nominal when a client does request a slower rate.
    pub period_ms: u32,
    /// t0_ms of the last sample actually EMITTED on the live BLE stream (throttle
    /// gate). Distinct from `last_t0_ms`, which advances on every sample accepted by
    /// cross-message dedup: a sample can be dedup-accepted but throttled, in which
    /// case `last_t0_ms` advances while `last_sent_t0_ms` does not.
    pub last_sent_t0_ms: Option<u64>,
    /// Retransmit buffer: bounded VecDeque of sent-but-unacknowledged frames
    pub tx_buffer: VecDeque<DataFrame>,
    /// True while historical replay frames are being sent (BACKLOG_THEN_LIVE / BACKLOG_ONLY).
    /// Used by finish_replay() to clear the replaying state; FLAG_BACKLOG is set exclusively
    /// by get_replay_frames(), not by add_data().
    pub is_replaying: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// BleSessionState
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLESESSION-002
/// Title: BleSessionState
///
/// Description: VRConnect shall maintain per-signal IDT stream state for reliable
///              BLE communication, including multi-stream sequence tracking,
///              retransmit buffer management, and subscription handling.
///
/// Version: V1.0
pub struct BleSessionState {
    /// Current IDT session identifier (from the BLE Central handshake)
    pub current_session_id: u16,
    /// Active streams indexed by stream_id
    pub streams: HashMap<u16, StreamEntry>,
    /// Maps signal_id → stream_id for O(1) lookup during data output
    pub signal_to_stream: HashMap<u16, u16>,
    /// Next stream_id to allocate on subscribe (starts at 1, monotone)
    pub next_stream_id: u16,
    /// Maximum frames per stream buffer (medical safety: prevents unbounded growth)
    pub max_buffer_size: usize,
    /// Per-signal ring-buffer of historical samples: signal_id → VecDeque<(t0_ms, value)>.
    /// Fed continuously from add_data(); bounded at max_history_size per signal.
    /// Used by get_replay_frames() to serve BACKLOG_THEN_LIVE / BACKLOG_ONLY subscriptions.
    pub history: HashMap<u16, VecDeque<(u64, f32)>>,
    /// Maximum historical samples kept per signal (hard count cap).
    /// Set via with_history_retention(); default matches HISTORY_RETENTION_SEC at 1 Hz.
    pub max_history_size: usize,
    /// Maximum age in milliseconds of a sample in the history ring buffer.
    /// Samples older than (current_t0_ms - max_history_age_ms) are evicted on insert.
    /// Prevents sparse signals (e.g. PNI every 5 min) from accumulating entries
    /// spanning multiple days within the size cap.
    /// Set via with_history_retention(). 0 = age eviction disabled.
    pub max_history_age_ms: u64,
}

impl BleSessionState {
    /// ID SRS: SRS-FN-BLESESSION-001
    /// Title: new
    ///
    /// Description: VRConnect shall create a new BleSessionState with the given
    ///              session ID and no active streams.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `session_id` - Initial IDT session identifier
    pub fn new(session_id: u16) -> Self {
        Self {
            current_session_id: session_id,
            streams: HashMap::new(),
            signal_to_stream: HashMap::new(),
            next_stream_id: 1,
            max_buffer_size: 1000, // Medical: prevent unbounded memory growth
            history: HashMap::new(),
            max_history_size: 21600, // 6 h at 1 Hz — matches default HISTORY_RETENTION_SEC
            max_history_age_ms: 21600 * 1000, // 6 h age eviction threshold
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-008
    /// Title: handle_ack
    ///
    /// Description: VRConnect shall process a cumulative IDT ACK_FRAME with selective bitmap.
    ///   - If a new session_id is detected: reset all stream buffers and sequence counters.
    ///   - Purge frames with seq ≤ ack_upto from the named stream's buffer.
    ///   - Check bitmap for selective ACKs (bit i in bitmap = seq [ack_upto+1+i] received).
    ///   - Return list of frames NOT in bitmap (lost frames) with FLAG_RETRANSMIT set.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `session_id` - Session identifier from the received ACK header
    /// * `stream_id`  - Stream identifier from the received ACK header
    /// * `ack_upto`   - Last contiguously acknowledged sequence number (inclusive)
    /// * `bitmap`     - 64-bit selective ACK bitmap (bit i = 1 means seq [ack_upto+1+i] received)
    ///
    /// # Returns
    /// Vec of cloned DataFrame (lost frames) with FLAG_RETRANSMIT set for retransmission
    pub fn handle_ack_with_bitmap(
        &mut self,
        session_id: u16,
        stream_id: u16,
        ack_upto: u32,
        bitmap: &[u8; 8],
    ) -> Vec<DataFrame> {
        if session_id != self.current_session_id {
            // New session detected: reset all buffers
            self.current_session_id = session_id;
            for entry in self.streams.values_mut() {
                entry.tx_buffer.clear();
                entry.last_seq = 0;
            }
            return vec![];
        }

        let Some(entry) = self.streams.get_mut(&stream_id) else {
            return vec![];
        };

        let mut retransmits = Vec::new();

        // 1. Find the "leading edge" (the highest bit set in the bitmap)
        // This tells us the latest out-of-order frame the client has received.
        let mut highest_acked_offset: Option<u32> = None;
        for offset in 0..64u32 {
            let bit_index = offset as usize;
            let byte_index = bit_index / 8;
            let bit_in_byte = bit_index % 8;

            if (bitmap[byte_index] >> bit_in_byte) & 1 == 1 {
                highest_acked_offset = Some(offset);
            }
        }

        // 2. Only check for lost frames BEFORE the highest received offset.
        // If highest_acked_offset is None, no newer frames arrived yet (frames are just in-flight).
        if let Some(max_offset) = highest_acked_offset {
            let buffer_seqs: std::collections::HashSet<u32> =
                entry.tx_buffer.iter().map(|f| f.header.seq).collect();

            for offset in 0..max_offset {
                let seq = ack_upto.wrapping_add(1).wrapping_add(offset);
                let bit_index = offset as usize;
                let byte_index = bit_index / 8;
                let bit_in_byte = bit_index % 8;
                let is_acked = (bitmap[byte_index] >> bit_in_byte) & 1 == 1;

                // If frame is in our buffer, but the bit is 0, it's a hole! Retransmit it.
                if buffer_seqs.contains(&seq) && !is_acked {
                    if let Some(frame) = entry.tx_buffer.iter().find(|f| f.header.seq == seq) {
                        let mut retransmit = frame.clone();
                        retransmit.header.flags |= FLAG_RETRANSMIT;
                        retransmits.push(retransmit);
                    }
                }
            }
        }

        // Purge all frames with seq ≤ ack_upto (cumulatively acknowledged)
        entry.tx_buffer.retain(|f| f.header.seq > ack_upto);

        retransmits
    }

    /// ID SRS: SRS-FN-BLESESSION-002
    /// Title: with_buffer_size
    ///
    /// Description: VRConnect shall allow configuring the maximum retransmit buffer
    ///              size per stream. Used for testing and resource-constrained deployments.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `size` - Maximum number of frames per stream retransmit buffer
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// ID SRS: SRS-FN-BLESESSION-017
    /// Title: with_history_size
    ///
    /// Description: Configure the maximum number of historical samples stored per signal.
    ///              Used for testing and resource-constrained deployments.
    ///
    /// Version: V1.0
    pub fn with_history_size(mut self, size: usize) -> Self {
        self.max_history_size = size;
        self
    }

    /// ID SRS: SRS-FN-BLESESSION-024
    /// Title: with_history_retention
    ///
    /// Description: Configure the history ring buffer by retention window in seconds.
    ///              Sets max_history_size = retention_sec (sized for 1 Hz continuous signals)
    ///              and max_history_age_ms = retention_sec × 1000 (per-sample age eviction).
    ///              Age eviction prevents sparse signals (e.g. PNI every 5 min) from
    ///              accumulating entries spanning multiple days within the size cap.
    ///              Called from ble_reliable.rs::new() with HISTORY_RETENTION_SEC config value.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `retention_sec` - Retention window in seconds (default: 21600 = 6 h)
    pub fn with_history_retention(mut self, retention_sec: u64) -> Self {
        self.max_history_size = retention_sec as usize;
        self.max_history_age_ms = retention_sec.saturating_mul(1000);
        self
    }

    /// ID SRS: SRS-FN-BLESESSION-019
    /// Title: insert_stream
    ///
    /// Description: VRConnect shall insert a new stream, keeping `streams` and
    ///              `signal_to_stream` in sync. Single choke point for stream
    ///              creation, so callers never update the two maps by hand.
    ///
    /// Version: V1.0
    fn insert_stream(&mut self, entry: StreamEntry) {
        self.signal_to_stream
            .insert(entry.signal_id, entry.stream_id);
        self.streams.insert(entry.stream_id, entry);
    }

    /// ID SRS: SRS-FN-BLESESSION-020
    /// Title: remove_stream_by_signal
    ///
    /// Description: VRConnect shall remove a stream by signal_id, keeping `streams`
    ///              and `signal_to_stream` in sync. No-op if not subscribed.
    ///
    /// Version: V1.0
    fn remove_stream_by_signal(&mut self, signal_id: u16) {
        if let Some(stream_id) = self.signal_to_stream.remove(&signal_id) {
            self.streams.remove(&stream_id);
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-021
    /// Title: clear_streams
    ///
    /// Description: VRConnect shall clear all streams, keeping `streams` and
    ///              `signal_to_stream` in sync.
    ///
    /// Version: V1.0
    fn clear_streams(&mut self) {
        self.signal_to_stream.clear();
        self.streams.clear();
    }

    /// ID SRS: SRS-FN-BLESESSION-003
    /// Title: subscribe
    ///
    /// Description: VRConnect shall allocate a new IDT stream_id for a signal_id on
    ///              first subscription. Subsequent calls with the same signal_id are
    ///              idempotent: the same stream_id is returned without creating a new stream.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier to subscribe (e.g. 0x0101 = HR)
    ///
    /// # Returns
    /// Newly allocated or pre-existing stream_id for this signal
    pub fn subscribe(&mut self, signal_id: u16) -> u16 {
        self.subscribe_with_period(signal_id, None, 0)
    }

    /// ID SRS: SRS-FN-BLESESSION-023
    /// Title: subscribe_with_stream_id
    ///
    /// Description: VRConnect shall subscribe with a caller-chosen stream_id instead of
    ///              auto-allocation. Idempotent: if signal_id is already subscribed, the
    ///              existing stream_id is returned unchanged.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier to subscribe
    /// * `preferred_stream_id` - stream_id to assign on first subscription
    ///
    /// # Returns
    /// Newly allocated (= `preferred_stream_id`) or pre-existing stream_id for this signal
    pub fn subscribe_with_stream_id(&mut self, signal_id: u16, preferred_stream_id: u16) -> u16 {
        self.subscribe_with_period(signal_id, Some(preferred_stream_id), 0)
    }

    /// ID SRS: SRS-FN-BLESESSION-025
    /// Title: subscribe_with_period
    ///
    /// Description: VRConnect shall subscribe a signal_id with an explicit per-stream
    ///              throttle gate (`period_ms`), optionally pinning a caller-chosen
    ///              stream_id. Idempotent: if signal_id is already subscribed, the
    ///              existing stream_id is returned and `period_ms` is updated in place
    ///              on the existing StreamEntry (sequence/dedup/retransmit state is left
    ///              untouched) — a re-subscribe must not silently drop a new throttle
    ///              request. `period_ms = 0` means "no throttling requested": the gate
    ///              in add_data() stays inactive and live throughput is unchanged, which
    ///              is the case for every app deployed before this field existed.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier to subscribe
    /// * `preferred_stream_id` - `Some(id)` to pin the stream_id (as `subscribe_with_stream_id`
    ///   does), `None` to auto-allocate (as `subscribe` does)
    /// * `period_ms` - effective throttle gate for this stream's live BLE output; `0` = off
    ///
    /// # Returns
    /// Newly allocated or pre-existing stream_id for this signal
    pub fn subscribe_with_period(
        &mut self,
        signal_id: u16,
        preferred_stream_id: Option<u16>,
        period_ms: u32,
    ) -> u16 {
        if let Some(&existing) = self.signal_to_stream.get(&signal_id) {
            if let Some(entry) = self.streams.get_mut(&existing) {
                entry.period_ms = period_ms;
            }
            return existing;
        }
        let stream_id = match preferred_stream_id {
            Some(preferred) => {
                if self.next_stream_id <= preferred {
                    self.next_stream_id = preferred + 1;
                }
                preferred
            }
            None => {
                let id = self.next_stream_id;
                self.next_stream_id += 1;
                id
            }
        };
        self.insert_stream(StreamEntry {
            stream_id,
            signal_id,
            source_id: 1,
            last_seq: 0,
            last_t0_ms: None,
            period_ms,
            last_sent_t0_ms: None,
            tx_buffer: VecDeque::new(),
            is_replaying: false,
        });
        stream_id
    }

    /// ID SRS: SRS-FN-BLESESSION-004
    /// Title: unsubscribe
    ///
    /// Description: VRConnect shall remove the stream for a signal_id, discarding
    ///              its retransmit buffer.  A no-op if not subscribed.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier to unsubscribe
    pub fn unsubscribe(&mut self, signal_id: u16) {
        self.remove_stream_by_signal(signal_id);
    }

    /// ID SRS: SRS-FN-BLESESSION-018
    /// Title: unsubscribe_all
    ///
    /// Description: VRConnect shall remove all active streams and signal→stream mappings,
    ///              effectively resetting subscription state to empty.
    ///              Called before processing a new SUBSCRIBE_REQ so the incoming list
    ///              replaces (rather than augments) the current subscriptions.
    ///
    /// Version: V1.0
    pub fn unsubscribe_all(&mut self) {
        self.clear_streams();
        log::info!("All streams cleared (unsubscribe_all)");
    }

    /// ID SRS: SRS-FN-BLESESSION-005
    /// Title: is_subscribed
    ///
    /// Description: VRConnect shall return true if the given signal_id has an active stream.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier
    ///
    /// # Returns
    /// true if an active stream exists for this signal, false otherwise
    pub fn is_subscribed(&self, signal_id: u16) -> bool {
        self.signal_to_stream.contains_key(&signal_id)
    }

    /// ID SRS: SRS-FN-BLESESSION-006
    /// Title: get_stream_id
    ///
    /// Description: VRConnect shall return the IDT stream_id allocated to a signal_id,
    ///              or None if not subscribed.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier
    ///
    /// # Returns
    /// Some(stream_id) if subscribed, None otherwise
    pub fn get_stream_id(&self, signal_id: u16) -> Option<u16> {
        self.signal_to_stream.get(&signal_id).copied()
    }

    /// ID SRS: SRS-FN-BLESESSION-007
    /// Title: add_data
    ///
    /// Description: VRConnect shall produce an IDT DataFrame for the given signal if
    ///              subscribed.  The per-stream sequence counter is incremented and the
    ///              frame is stored in the retransmit buffer (bounded by max_buffer_size).
    ///              If a non-zero `period_ms` throttle was negotiated for this stream, a
    ///              sample arriving before `last_sent_t0_ms + period_ms` is dropped
    ///              *before* the seq counter advances and before it enters `tx_buffer` —
    ///              a throttled sample must never create a gap the ACK/NACK machinery
    ///              would have to recover. This only affects the live BLE stream: the
    ///              caller (`output()` in ble_reliable.rs) always calls
    ///              `record_history()` and journals to the WAL *before* `add_data()`, so
    ///              throttling never reduces what is recorded or replayable.
    ///
    /// Version: V2.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier (e.g. 0x0101 = HR)
    /// * `value`     - Measured float32 value
    /// * `t0_ms`     - Sample timestamp, milliseconds since Unix epoch
    ///
    /// # Returns
    /// Some(DataFrame) ready to notify on Data_OUT, None if signal not subscribed,
    /// deduplicated, or throttled by the stream's `period_ms` gate
    pub fn add_data(&mut self, signal_id: u16, value: f32, t0_ms: u64) -> Option<DataFrame> {
        let stream_id = *self.signal_to_stream.get(&signal_id)?;
        let entry = self.streams.get_mut(&stream_id)?;

        // Cross-message deduplication: VitalRecorder uses a sliding window and may
        // re-send the same timestamp in consecutive Socket.IO messages.
        // Only forward samples that are strictly newer than the last emitted frame.
        if let Some(last) = entry.last_t0_ms {
            if t0_ms <= last {
                log::debug!(
                    "Cross-msg dup skipped: signal=0x{:04X} t0_ms={} <= last={}",
                    signal_id,
                    t0_ms,
                    last
                );
                return None;
            }
        }
        entry.last_t0_ms = Some(t0_ms);

        // Per-stream throttle gate: inactive when period_ms == 0 (no request made —
        // every deployed app today). Placed after the dedup commit above but before
        // last_seq/tx_buffer so a throttled sample burns no sequence number.
        if entry.period_ms > 0 {
            if let Some(last_sent) = entry.last_sent_t0_ms {
                if t0_ms.saturating_sub(last_sent) < entry.period_ms as u64 {
                    log::debug!(
                        "Throttled: signal=0x{:04X} t0_ms={} last_sent={} period_ms={}",
                        signal_id,
                        t0_ms,
                        last_sent,
                        entry.period_ms
                    );
                    return None;
                }
            }
            entry.last_sent_t0_ms = Some(t0_ms);
        }

        entry.last_seq += 1;
        let seq = entry.last_seq;

        let frame = DataFrame::new(self.current_session_id, stream_id, seq, t0_ms, value);

        // Buffer for retransmission (oldest frame evicted when limit reached).
        // [OBS-1] If the ACK channel is frozen, the buffer fills to max_buffer_size
        //         and oldest frames are silently lost.
        //         Each eviction is logged at WARN so medical data loss is never silent.
        entry.tx_buffer.push_back(frame.clone());
        while entry.tx_buffer.len() > self.max_buffer_size {
            if let Some(evicted) = entry.tx_buffer.pop_front() {
                log::warn!(
                    "[BLE] Buffer overflow stream {}: evicted seq {} (cap={}) \
                     — ACK channel may be frozen.",
                    stream_id,
                    evicted.header.seq,
                    self.max_buffer_size
                );
            }
        }

        Some(frame)
    }

    /// ID SRS: SRS-FN-BLESESSION-013
    /// Title: record_history
    ///
    /// Description: VRConnect shall record a (t0_ms, value) sample to the per-signal
    ///              history ring-buffer.  The buffer is bounded by max_history_size;
    ///              the oldest sample is evicted when the limit is reached.
    ///              This is called unconditionally from output(), regardless of subscription
    ///              state, so that history is available even before a client subscribes.
    ///              Duplicate timestamps (t0_ms ≤ last recorded) are silently skipped to
    ///              prevent redundant history entries from repeated calls or flush paths.
    ///
    /// Version: V2.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier
    /// * `value`     - Measured float32 value
    /// * `t0_ms`     - Sample timestamp, milliseconds since Unix epoch
    pub fn record_history(&mut self, signal_id: u16, value: f32, t0_ms: u64) {
        let buf = self.history.entry(signal_id).or_default();
        if let Some(&(last_ts, _)) = buf.back() {
            if t0_ms <= last_ts {
                return;
            }
        }
        buf.push_back((t0_ms, value));
        // Age eviction: remove samples older than max_history_age_ms.
        // Runs before the count cap so sparse signals (e.g. PNI every 5 min)
        // don't accumulate entries spanning multiple days within the size limit.
        if self.max_history_age_ms > 0 {
            let cutoff = t0_ms.saturating_sub(self.max_history_age_ms);
            while buf.front().is_some_and(|&(ts, _)| ts < cutoff) {
                buf.pop_front();
            }
        }
        // Hard count cap (safety net after age eviction).
        while buf.len() > self.max_history_size {
            buf.pop_front();
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-020
    /// Title: flush_tx_to_history
    ///
    /// Description: VRConnect shall flush all frames currently in the retransmit buffers
    ///              into the per-signal history ring-buffers before the session is cleared.
    ///              This is a defensive safety net: since record_history() is called before
    ///              add_data() in the normal output() path, the history should already contain
    ///              these frames.  flush_tx_to_history() guards against any code path that
    ///              bypasses record_history(), and makes the invariant explicit.
    ///              record_history()'s dedup guard prevents duplicate entries.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Number of frames flushed (0 if history was already up to date)
    pub fn flush_tx_to_history(&mut self) -> usize {
        // Collect (signal_id, t0_ms, value) triples first to avoid simultaneous
        // borrow of self.streams (immutable) and self.history (mutable via record_history).
        let pending: Vec<(u16, u64, f32)> = self
            .streams
            .values()
            .flat_map(|entry| {
                entry
                    .tx_buffer
                    .iter()
                    .map(|frame| (entry.signal_id, frame.t0_ms, frame.value))
            })
            .collect();

        let mut flushed = 0usize;
        for (signal_id, t0_ms, value) in pending {
            let prev_len = self.history.get(&signal_id).map(|b| b.len()).unwrap_or(0);
            self.record_history(signal_id, value, t0_ms);
            if self.history.get(&signal_id).map(|b| b.len()).unwrap_or(0) > prev_len {
                flushed += 1;
            }
        }
        flushed
    }

    /// ID SRS: SRS-FN-BLESESSION-014
    /// Title: get_replay_frames
    ///
    /// Description: VRConnect shall return IDT DataFrames for historical samples of a
    ///              given signal, starting from start_time_ms (inclusive).
    ///              If start_time_ms == 0, all buffered history is returned.
    ///              Each returned frame has FLAG_BACKLOG set.
    ///              Returned frames use the provided session_id and stream_id, with
    ///              sequences starting at seq_start and incrementing by 1.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id`   - IDT signal identifier
    /// * `start_time_ms` - Replay window start (epoch ms); 0 = replay all available
    /// * `session_id`  - IDT session identifier for the replay frames
    /// * `stream_id`   - IDT stream identifier for the replay frames
    /// * `seq_start`   - Sequence number of the first replay frame
    ///
    /// # Returns
    /// Vec of DataFrames with FLAG_BACKLOG set, in chronological order
    pub fn get_replay_frames(
        &self,
        signal_id: u16,
        start_time_ms: u64,
        session_id: u16,
        stream_id: u16,
        seq_start: u32,
    ) -> Vec<DataFrame> {
        let Some(buf) = self.history.get(&signal_id) else {
            return vec![];
        };
        let mut frames = Vec::new();
        let mut seq = seq_start;
        for &(t0_ms, value) in buf.iter() {
            if start_time_ms == 0 || t0_ms >= start_time_ms {
                let mut frame = DataFrame::new(session_id, stream_id, seq, t0_ms, value);
                frame.header.flags |= FLAG_BACKLOG;
                frames.push(frame);
                seq = seq.wrapping_add(1);
            }
        }
        frames
    }

    /// ID SRS: SRS-FN-BLESESSION-015
    /// Title: start_replay
    ///
    /// Description: VRConnect shall mark a stream as replaying and return its historical
    ///              DataFrames with FLAG_BACKLOG set.
    ///
    ///              Sequence allocation (F5 + F4, combined fix):
    ///              - The replay frames reserve a contiguous seq block [seq_start, seq_start+N)
    ///                and last_seq is advanced by N *eagerly*. This is required for concurrency:
    ///                live add_data() can run concurrently while the replay burst drains (the
    ///                send loop in ble_reliable.rs does NOT hold the session lock), so live
    ///                frames must get sequence numbers AFTER the reserved block — otherwise a
    ///                live frame and a replay frame would collide on the same seq.
    ///              - All replay frames are pushed into tx_buffer so they are NACK-recoverable.
    ///                If the replay is interrupted (notify failure), the un-sent frames remain
    ///                in tx_buffer; the resulting seq gap is therefore *detectable and
    ///                recoverable* (client NACKs it, server retransmits from tx_buffer) instead
    ///                of irrecoverable. This is what F4 fixes — and it makes the eager reserve
    ///                safe, removing the need for the earlier lazy/commit scheme.
    ///
    ///              tx_buffer is bounded at max_buffer_size: for a backlog larger than the cap,
    ///              only the most recent frames are retained for NACK recovery; older losses are
    ///              recovered by re-subscribing BACKLOG_THEN_LIVE (history pull). Eviction here
    ///              is expected (backlog >> buffer) and is NOT logged per-frame to avoid flooding.
    ///
    /// Version: V2.0
    ///
    /// # Arguments
    /// * `signal_id`     - IDT signal identifier
    /// * `start_time_ms` - Replay start (epoch ms); 0 = replay all history
    ///
    /// # Returns
    /// Vec of replay DataFrames (FLAG_BACKLOG set).  Empty if signal not subscribed
    /// or no history available.
    pub fn start_replay(&mut self, signal_id: u16, start_time_ms: u64) -> Vec<DataFrame> {
        let stream_id = match self.signal_to_stream.get(&signal_id).copied() {
            Some(id) => id,
            None => return vec![],
        };

        let session_id = self.current_session_id;
        let entry = match self.streams.get_mut(&stream_id) {
            Some(e) => e,
            None => return vec![],
        };

        let seq_start = entry.last_seq.wrapping_add(1);
        entry.is_replaying = true;

        // `entry`'s mutable borrow ends here (NLL: last use was the line above) — no
        // explicit drop needed before get_replay_frames() takes an immutable &self borrow.
        let frames =
            self.get_replay_frames(signal_id, start_time_ms, session_id, stream_id, seq_start);

        if let Some(entry) = self.streams.get_mut(&stream_id) {
            // [F5] Reserve the seq block eagerly so concurrent live frames get seq AFTER it.
            entry.last_seq = entry.last_seq.wrapping_add(frames.len() as u32);
            // [F4] Keep replay frames in tx_buffer for NACK recovery (bounded; silent eviction).
            for frame in &frames {
                entry.tx_buffer.push_back(frame.clone());
                if entry.tx_buffer.len() > self.max_buffer_size {
                    entry.tx_buffer.pop_front();
                }
            }
        }

        frames
    }

    /// ID SRS: SRS-FN-BLESESSION-016
    /// Title: finish_replay
    ///
    /// Description: VRConnect shall clear the is_replaying flag for a stream, signalling
    ///              that the historical burst has been fully delivered.
    ///              Subsequent live DATA_FRAMEs were never carrying FLAG_BACKLOG (that flag
    ///              is set exclusively by get_replay_frames(), not by add_data()).
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `stream_id` - IDT stream to mark as no longer replaying
    pub fn finish_replay(&mut self, stream_id: u16) {
        if let Some(entry) = self.streams.get_mut(&stream_id) {
            entry.is_replaying = false;
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-008
    /// Title: handle_ack
    ///
    /// Description: VRConnect shall process a cumulative IDT ACK_FRAME.
    ///   - If a new session_id is detected: reset all stream buffers and sequence counters.
    ///   - Otherwise: purge frames with seq ≤ ack_upto from the named stream's buffer.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `session_id` - Session identifier from the received ACK header
    /// * `stream_id`  - Stream identifier from the received ACK header
    /// * `ack_upto`   - Last contiguously acknowledged sequence number (inclusive)
    pub fn handle_ack(&mut self, session_id: u16, stream_id: u16, ack_upto: u32) {
        if session_id != self.current_session_id {
            // New session detected: reset all buffers (subscriptions preserved)
            self.current_session_id = session_id;
            for entry in self.streams.values_mut() {
                entry.tx_buffer.clear();
                entry.last_seq = 0;
            }
            return;
        }

        // Purge confirmed frames from the targeted stream
        if let Some(entry) = self.streams.get_mut(&stream_id) {
            entry.tx_buffer.retain(|f| f.header.seq > ack_upto);
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-009
    /// Title: handle_nack
    ///
    /// Description: VRConnect shall return frames from the named stream's buffer that
    ///              match the requested sequence numbers, setting FLAG_RETRANSMIT in each
    ///              returned frame.  The buffer itself is NOT modified.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `stream_id` - IDT stream targeted by the NACK
    /// * `seqs`      - Sequence numbers requested for retransmission
    ///
    /// # Returns
    /// Vec of cloned DataFrame with FLAG_RETRANSMIT set; empty if stream unknown
    pub fn handle_nack(&self, stream_id: u16, seqs: &[u32]) -> Vec<DataFrame> {
        let Some(entry) = self.streams.get(&stream_id) else {
            return vec![];
        };
        seqs.iter()
            .filter_map(|&seq| {
                entry
                    .tx_buffer
                    .iter()
                    .find(|f| f.header.seq == seq)
                    .map(|f| {
                        let mut retransmit = f.clone();
                        retransmit.header.flags |= FLAG_RETRANSMIT;
                        retransmit
                    })
            })
            .collect()
    }

    /// ID SRS: SRS-FN-BLESESSION-026
    /// Title: oldest_pending_per_stream
    ///
    /// Description: VRConnect shall return, for every stream with unacknowledged frames,
    ///              a copy of its OLDEST pending frame with FLAG_RETRANSMIT set. The buffer
    ///              itself is NOT modified. Used by supervision_task as a last-chance
    ///              retransmit before declaring the link dead: the oldest frame is the one
    ///              blocking the cumulative ACK, so resending it alone is enough for a live
    ///              Central to ACK everything after it — and it bounds the burst to one
    ///              frame per stream, whatever the buffer depth.
    ///
    /// Version: V1.0
    pub fn oldest_pending_per_stream(&self) -> Vec<DataFrame> {
        self.streams
            .values()
            .filter_map(|e| e.tx_buffer.front())
            .map(|f| {
                let mut retransmit = f.clone();
                retransmit.header.flags |= FLAG_RETRANSMIT;
                retransmit
            })
            .collect()
    }

    /// ID SRS: SRS-FN-BLESESSION-010
    /// Title: reset_session
    ///
    /// Description: VRConnect shall reset all streams to a new session ID, clearing
    ///              retransmit buffers and sequence counters.  Subscriptions are preserved.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `new_session_id` - New IDT session identifier
    pub fn reset_session(&mut self, new_session_id: u16) {
        self.current_session_id = new_session_id;
        for entry in self.streams.values_mut() {
            entry.tx_buffer.clear();
            entry.last_seq = 0;
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-019
    /// Title: on_disconnect
    ///
    /// Description: VRConnect shall fully reset all BLE session state when the Central
    ///              disconnects: all active streams, signal→stream mappings, and the
    ///              stream_id allocator are cleared, and current_session_id is
    ///              auto-incremented (wrapping) so the reconnecting Central starts a
    ///              fresh session with unambiguous frame numbering.
    ///              History ring-buffers are intentionally preserved to support
    ///              BACKLOG_THEN_LIVE replay on the next connection.
    ///              Before clearing, flush_tx_to_history() rescues any unACK'd frames
    ///              not yet in history (defensive: normally record_history is called
    ///              before add_data in the output() path).
    ///
    /// Version: V2.0
    pub fn on_disconnect(&mut self) {
        let flushed = self.flush_tx_to_history();
        if flushed > 0 {
            log::info!(
                "[BLE] flush_tx_to_history: {} frame(s) rescued into history before session reset",
                flushed
            );
        }
        self.clear_streams();
        self.next_stream_id = 1;
        self.current_session_id = self.current_session_id.wrapping_add(1);
        log::info!(
            "BLE session reset on disconnect (new session_id={})",
            self.current_session_id
        );
    }

    /// ID SRS: SRS-FN-BLESESSION-011
    /// Title: get_pending_count
    ///
    /// Description: VRConnect shall return the number of unacknowledged frames in
    ///              the retransmit buffer for the given signal.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier
    ///
    /// # Returns
    /// Number of buffered frames; 0 if not subscribed
    pub fn get_pending_count(&self, signal_id: u16) -> usize {
        self.signal_to_stream
            .get(&signal_id)
            .and_then(|sid| self.streams.get(sid))
            .map(|e| e.tx_buffer.len())
            .unwrap_or(0)
    }

    /// ID SRS: SRS-FN-BLESESSION-012
    /// Title: total_pending
    ///
    /// Description: VRConnect shall return the total number of unacknowledged frames
    ///              across all active streams (used for session statistics).
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Sum of all stream buffer lengths
    pub fn total_pending(&self) -> usize {
        self.streams.values().map(|e| e.tx_buffer.len()).sum()
    }

    /// ID SRS: SRS-FN-BLESESSION-021
    /// Title: serialize_history_to_bytes
    ///
    /// Description: VRConnect shall serialize the history ring buffer to a compact binary
    ///              checkpoint format. Layout: magic(4) + version(4) + timestamp_sec(8) +
    ///              n_signals(4); then per signal: signal_id(2) + n_samples(4); then per
    ///              sample: t0_ms(8) + value_f32(4). All integers are little-endian.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Serialized bytes ready for atomic write to disk
    pub fn serialize_history_to_bytes(&self) -> Vec<u8> {
        const MAGIC: u32 = 0x424C_4548; // "BLEH"
        let timestamp_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let n_signals = self.history.len() as u32;

        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&timestamp_sec.to_le_bytes());
        buf.extend_from_slice(&n_signals.to_le_bytes());

        for (&signal_id, samples) in &self.history {
            buf.extend_from_slice(&signal_id.to_le_bytes());
            buf.extend_from_slice(&(samples.len() as u32).to_le_bytes());
            for &(t0_ms, value) in samples {
                buf.extend_from_slice(&t0_ms.to_le_bytes());
                buf.extend_from_slice(&value.to_le_bytes());
            }
        }
        buf
    }

    /// ID SRS: SRS-FN-BLESESSION-022
    /// Title: load_history_from_bytes
    ///
    /// Description: VRConnect shall deserialize a history checkpoint binary and merge
    ///              the loaded samples into the current history ring buffer via
    ///              record_history (dedup guard prevents duplicate insertion).
    ///              Returns the total number of samples processed, or Err if the
    ///              binary is malformed or uses an unsupported version.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `bytes` - Raw checkpoint bytes produced by serialize_history_to_bytes
    ///
    /// # Returns
    /// Ok(n) — number of samples processed; Err(description) on format error
    pub fn load_history_from_bytes(&mut self, bytes: &[u8]) -> Result<usize, String> {
        const MAGIC: u32 = 0x424C_4548;
        if bytes.len() < 20 {
            return Err(format!("checkpoint too short ({} bytes)", bytes.len()));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(format!("invalid magic 0x{:08X}", magic));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if version != 1 {
            return Err(format!("unsupported checkpoint version {}", version));
        }
        let n_signals = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let mut pos = 20usize;
        let mut total = 0usize;

        for _ in 0..n_signals {
            if pos + 6 > bytes.len() {
                return Err("truncated signal header".into());
            }
            let signal_id = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap());
            let n_samples =
                u32::from_le_bytes(bytes[pos + 2..pos + 6].try_into().unwrap()) as usize;
            pos += 6;
            for _ in 0..n_samples {
                if pos + 12 > bytes.len() {
                    return Err("truncated sample".into());
                }
                let t0_ms = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
                let value = f32::from_le_bytes(bytes[pos + 8..pos + 12].try_into().unwrap());
                pos += 12;
                self.record_history(signal_id, value, t0_ms);
                total += 1;
            }
        }
        Ok(total)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
