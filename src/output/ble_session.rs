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
    ///              creation — replaces what used to be 2 independently-updated
    ///              insert sites (subscribe, subscribe_with_stream_id), each of
    ///              which had to remember to update both maps by hand.
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
        // Idempotent: return existing stream_id unchanged
        if let Some(&existing) = self.signal_to_stream.get(&signal_id) {
            return existing;
        }
        let stream_id = self.next_stream_id;
        self.next_stream_id += 1;
        self.insert_stream(StreamEntry {
            stream_id,
            signal_id,
            source_id: 1,
            last_seq: 0,
            last_t0_ms: None,
            tx_buffer: VecDeque::new(),
            is_replaying: false,
        });
        stream_id
    }

    /// Subscribe with a caller-chosen stream_id instead of auto-allocation.
    /// Idempotent: if signal_id is already subscribed, the existing stream_id is returned.
    pub fn subscribe_with_stream_id(&mut self, signal_id: u16, preferred_stream_id: u16) -> u16 {
        if let Some(&existing) = self.signal_to_stream.get(&signal_id) {
            return existing;
        }
        let stream_id = preferred_stream_id;
        if self.next_stream_id <= stream_id {
            self.next_stream_id = stream_id + 1;
        }
        self.insert_stream(StreamEntry {
            stream_id,
            signal_id,
            source_id: 1,
            last_seq: 0,
            last_t0_ms: None,
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
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier (e.g. 0x0101 = HR)
    /// * `value`     - Measured float32 value
    /// * `t0_ms`     - Sample timestamp, milliseconds since Unix epoch
    ///
    /// # Returns
    /// Some(DataFrame) ready to notify on Data_OUT, None if signal not subscribed
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
mod tests {
    use super::*;
    use crate::domain::ble_protocol::{SignalId, FLAG_RETRANSMIT, IDT_MAGIC, MSG_DATA_FRAME};

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-001
    /// Version: V1.0
    /// Title: Test session creation
    ///
    /// Description: BleSessionState::new shall start with no streams and next_stream_id=1.
    #[test]
    fn test_session_creation() {
        let session = BleSessionState::new(42);
        assert_eq!(session.current_session_id, 42);
        assert!(session.streams.is_empty());
        assert!(session.signal_to_stream.is_empty());
        assert_eq!(session.next_stream_id, 1);
        assert_eq!(session.max_buffer_size, 1000);
    }

    /// ID SRS: SRS-TEST-BLESESSION-002
    /// Version: V1.0
    /// Title: Test subscribe allocates stream_id
    ///
    /// Description: subscribe(0x0101) shall allocate stream_id=1 for the first signal.
    #[test]
    fn test_subscribe_allocates_stream_id() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());
        assert_eq!(stream_id, 1);
        assert!(session.is_subscribed(SignalId::HR.as_u16()));
        assert_eq!(session.streams.len(), 1);
        assert_eq!(session.next_stream_id, 2);
    }

    /// ID SRS: SRS-TEST-BLESESSION-003
    /// Version: V1.0
    /// Title: Test subscribe is idempotent
    ///
    /// Description: Calling subscribe twice for the same signal_id shall return
    ///              the same stream_id without creating a duplicate stream.
    #[test]
    fn test_subscribe_idempotent() {
        let mut session = BleSessionState::new(1);
        let id1 = session.subscribe(SignalId::HR.as_u16());
        let id2 = session.subscribe(SignalId::HR.as_u16());
        assert_eq!(id1, id2);
        assert_eq!(session.streams.len(), 1);
        assert_eq!(session.next_stream_id, 2); // only incremented once
    }

    /// ID SRS: SRS-TEST-BLESESSION-004
    /// Version: V1.0
    /// Title: Test unsubscribe removes stream
    ///
    /// Description: unsubscribe shall remove both the StreamEntry and the signal→stream mapping.
    #[test]
    fn test_unsubscribe_removes_stream() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        assert!(session.is_subscribed(SignalId::HR.as_u16()));

        session.unsubscribe(SignalId::HR.as_u16());
        assert!(!session.is_subscribed(SignalId::HR.as_u16()));
        assert!(session.streams.is_empty());
        assert!(session.signal_to_stream.is_empty());
    }

    // ── add_data ──────────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-005
    /// Version: V1.0
    /// Title: Test add_data returns None when not subscribed
    #[test]
    fn test_add_data_no_subscription() {
        let mut session = BleSessionState::new(1);
        assert!(session
            .add_data(SignalId::HR.as_u16(), 75.0, 1_000_000)
            .is_none());
    }

    /// ID SRS: SRS-TEST-BLESESSION-006
    /// Version: V1.0
    /// Title: Test add_data produces a valid IDT DATA_FRAME
    ///
    /// Description: The returned DataFrame shall carry the correct IDT magic, msg_type,
    ///              session_id, t0_ms, and float32 value.
    #[test]
    fn test_add_data_produces_idt_frame() {
        let mut session = BleSessionState::new(3);
        session.subscribe(SignalId::HR.as_u16());

        let t0_ms: u64 = 1_700_000_000_000;
        let frame = session
            .add_data(SignalId::HR.as_u16(), 72.5, t0_ms)
            .unwrap();

        // Verify wire encoding via to_ble_bytes
        let bytes = frame.to_ble_bytes();
        assert_eq!(u16::from_le_bytes([bytes[0], bytes[1]]), IDT_MAGIC);
        assert_eq!(bytes[3], MSG_DATA_FRAME);

        // Verify struct fields
        assert_eq!(frame.header.session_id, 3);
        assert_eq!(frame.header.seq, 1);
        assert_eq!(frame.t0_ms, t0_ms);
        assert!((frame.value - 72.5f32).abs() < f32::EPSILON);
    }

    /// ID SRS: SRS-TEST-BLESESSION-007
    /// Version: V1.0
    /// Title: Test sequence number increments per stream
    ///
    /// Description: Three successive add_data calls on the same signal shall produce
    ///              seq = 1, 2, 3.
    #[test]
    fn test_add_data_seq_increment() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        let f1 = session.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
        let f2 = session.add_data(SignalId::HR.as_u16(), 71.0, 1000).unwrap();
        let f3 = session.add_data(SignalId::HR.as_u16(), 72.0, 2000).unwrap();

        assert_eq!(f1.header.seq, 1);
        assert_eq!(f2.header.seq, 2);
        assert_eq!(f3.header.seq, 3);
    }

    // ── handle_ack ────────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-008
    /// Version: V1.0
    /// Title: Test handle_ack purges acknowledged frames
    ///
    /// Description: handle_ack(ack_upto=3) on a buffer of 5 frames shall leave
    ///              exactly frames seq 4 and 5.
    #[test]
    fn test_handle_ack_purges_buffer() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        for i in 0u64..5 {
            session.add_data(SignalId::HR.as_u16(), i as f32, i * 1000);
        }
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 5);

        session.handle_ack(1, stream_id, 3);
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 2);

        let entry = session.streams.get(&stream_id).unwrap();
        let seqs: Vec<u32> = entry.tx_buffer.iter().map(|f| f.header.seq).collect();
        assert_eq!(seqs, vec![4, 5]);
    }

    /// ID SRS: SRS-TEST-BLESESSION-009
    /// Version: V1.0
    /// Title: Test handle_ack with new session_id resets all buffers
    ///
    /// Description: If session_id in the ACK differs from current_session_id, all
    ///              stream buffers shall be cleared (subscriptions preserved).
    #[test]
    fn test_handle_ack_session_reset() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        session.add_data(SignalId::HR.as_u16(), 71.0, 1000);
        session.add_data(SignalId::HR.as_u16(), 72.0, 2000);
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 3);

        // New session_id in ACK
        session.handle_ack(99, stream_id, 0);

        assert_eq!(session.current_session_id, 99);
        assert_eq!(session.total_pending(), 0);
        // Subscriptions must survive the reset
        assert!(session.is_subscribed(SignalId::HR.as_u16()));
    }

    // ── handle_nack ───────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-010
    /// Version: V1.0
    /// Title: Test handle_nack returns frames with FLAG_RETRANSMIT
    ///
    /// Description: handle_nack for seq=2 shall return exactly that frame with
    ///              FLAG_RETRANSMIT set, without modifying the buffer.
    #[test]
    fn test_handle_nack_retransmit() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        session.add_data(SignalId::HR.as_u16(), 71.0, 1000);
        session.add_data(SignalId::HR.as_u16(), 72.0, 2000);

        let retransmits = session.handle_nack(stream_id, &[2]);
        assert_eq!(retransmits.len(), 1);
        assert_eq!(retransmits[0].header.seq, 2);
        assert_ne!(
            retransmits[0].header.flags & FLAG_RETRANSMIT,
            0,
            "FLAG_RETRANSMIT must be set"
        );

        // Buffer unchanged — 3 frames still present
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 3);
    }

    // ── Buffer size limit ─────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-011
    /// Version: V1.0
    /// Title: Test buffer size limit evicts oldest frames
    ///
    /// Description: with_buffer_size(3) followed by 5 add_data calls shall retain
    ///              only the 3 most recent frames (seq 3, 4, 5).
    #[test]
    fn test_buffer_size_limit() {
        let mut session = BleSessionState::new(1).with_buffer_size(3);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        for i in 0u64..5 {
            session.add_data(SignalId::HR.as_u16(), i as f32, i * 1000);
        }
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 3);

        let entry = session.streams.get(&stream_id).unwrap();
        assert_eq!(entry.tx_buffer.front().unwrap().header.seq, 3);
        assert_eq!(entry.tx_buffer.back().unwrap().header.seq, 5);
    }

    // ── Multi-signal ──────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-012
    /// Version: V1.0
    /// Title: Test multiple signals get independent streams and sequence counters
    ///
    /// Description: HR and SpO2 shall receive distinct stream_ids and independent
    ///              per-stream sequence numbers.
    #[test]
    fn test_multi_signal_independent_streams() {
        let mut session = BleSessionState::new(1);
        let hr_stream = session.subscribe(SignalId::HR.as_u16());
        let spo2_stream = session.subscribe(SignalId::SpO2.as_u16());

        assert_ne!(hr_stream, spo2_stream);

        let f_hr_1 = session.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
        let f_spo2_1 = session.add_data(SignalId::SpO2.as_u16(), 98.0, 0).unwrap();
        let f_hr_2 = session.add_data(SignalId::HR.as_u16(), 71.0, 1000).unwrap();

        // Independent sequence counters: each starts at 1
        assert_eq!(f_hr_1.header.seq, 1);
        assert_eq!(f_spo2_1.header.seq, 1);
        assert_eq!(f_hr_2.header.seq, 2);

        // Each frame carries its correct stream_id
        assert_eq!(f_hr_1.header.stream_id, hr_stream);
        assert_eq!(f_spo2_1.header.stream_id, spo2_stream);
    }

    /// ID SRS: SRS-TEST-BLESESSION-013
    /// Version: V1.0
    /// Title: Test get_stream_id returns correct stream_id
    #[test]
    fn test_get_stream_id() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::SpO2.as_u16());

        assert_eq!(session.get_stream_id(SignalId::SpO2.as_u16()), Some(1));
        assert_eq!(session.get_stream_id(SignalId::HR.as_u16()), None);
    }

    /// ID SRS: SRS-TEST-BLESESSION-014
    /// Version: V1.0
    /// Title: Test total_pending sums all stream buffers
    ///
    /// Description: 2 HR frames + 1 SpO2 frame → total_pending = 3.
    #[test]
    fn test_total_pending() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.subscribe(SignalId::SpO2.as_u16());

        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        session.add_data(SignalId::HR.as_u16(), 71.0, 1000);
        session.add_data(SignalId::SpO2.as_u16(), 98.0, 0);

        assert_eq!(session.total_pending(), 3);
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 2);
        assert_eq!(session.get_pending_count(SignalId::SpO2.as_u16()), 1);
    }

    // ── subscribe_with_stream_id ───────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-015
    /// Version: V1.0
    /// Title: Test subscribe_with_stream_id assigns the preferred stream_id
    ///
    /// Description: Calling subscribe_with_stream_id(HR, 5) shall allocate stream_id=5,
    ///              and advance next_stream_id to 6.
    #[test]
    fn test_subscribe_with_stream_id_preferred() {
        let mut session = BleSessionState::new(1);
        let sid = session.subscribe_with_stream_id(SignalId::HR.as_u16(), 5);
        assert_eq!(sid, 5);
        assert_eq!(session.get_stream_id(SignalId::HR.as_u16()), Some(5));
        assert_eq!(session.next_stream_id, 6);
    }

    /// ID SRS: SRS-TEST-BLESESSION-016
    /// Version: V1.0
    /// Title: Test subscribe_with_stream_id is idempotent
    ///
    /// Description: A second call with the same signal_id must return the first
    ///              stream_id unchanged, even if a different preferred_id is given.
    #[test]
    fn test_subscribe_with_stream_id_idempotent() {
        let mut session = BleSessionState::new(1);
        let first = session.subscribe_with_stream_id(SignalId::HR.as_u16(), 3);
        let second = session.subscribe_with_stream_id(SignalId::HR.as_u16(), 99);
        assert_eq!(first, second); // second call ignored
        assert_eq!(session.streams.len(), 1);
    }

    // ── FLAG_BACKLOG behaviour ─────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-017
    /// Version: V1.0
    /// Title: Test FLAG_BACKLOG is NOT set on normal live frames
    ///
    /// Description: FLAG_BACKLOG must NOT be set on live DATA_FRAMEs when the stream
    ///              is not in replay mode — even if the retransmit buffer is non-empty.
    #[test]
    fn test_flag_backlog_not_set_on_live_frames() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        // First live frame: no replay in progress → FLAG_BACKLOG must NOT be set
        let f1 = session.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
        assert_eq!(
            f1.header.flags & FLAG_BACKLOG,
            0,
            "First live frame: no replay → FLAG_BACKLOG must NOT be set"
        );

        // Second live frame: buffer is non-empty but NOT replaying → FLAG_BACKLOG must NOT be set
        let f2 = session.add_data(SignalId::HR.as_u16(), 71.0, 1000).unwrap();
        assert_eq!(
            f2.header.flags & FLAG_BACKLOG,
            0,
            "Second live frame: not replaying → FLAG_BACKLOG must NOT be set"
        );
    }

    // ── reset_session ─────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-018
    /// Version: V1.0
    /// Title: Test reset_session clears all buffers and sequence counters
    ///
    /// Description: After reset_session(99), all stream buffers must be empty,
    ///              last_seq must be 0, and subscriptions must be preserved.
    #[test]
    fn test_reset_session() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.subscribe(SignalId::SpO2.as_u16());

        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        session.add_data(SignalId::SpO2.as_u16(), 98.0, 0);
        assert_eq!(session.total_pending(), 2);

        session.reset_session(99);

        assert_eq!(session.current_session_id, 99);
        assert_eq!(session.total_pending(), 0);
        // Subscriptions survive
        assert!(session.is_subscribed(SignalId::HR.as_u16()));
        assert!(session.is_subscribed(SignalId::SpO2.as_u16()));
        // Sequence counters reset
        for entry in session.streams.values() {
            assert_eq!(entry.last_seq, 0);
        }
    }

    // ── handle_nack edge cases ────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-019
    /// Version: V1.0
    /// Title: Test handle_nack returns empty Vec for unknown stream_id
    #[test]
    fn test_handle_nack_unknown_stream() {
        let session = BleSessionState::new(1);
        let result = session.handle_nack(999, &[1, 2, 3]);
        assert!(result.is_empty());
    }

    /// ID SRS: SRS-TEST-BLESESSION-020
    /// Version: V1.0
    /// Title: Test handle_nack for seq not in buffer returns empty Vec
    ///
    /// Description: If the requested seq has already been evicted (buffer capped),
    ///              handle_nack shall silently skip it and return nothing.
    #[test]
    fn test_handle_nack_seq_not_in_buffer() {
        let mut session = BleSessionState::new(1).with_buffer_size(2);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        // Add 3 frames; buffer capped at 2 → seq 1 evicted
        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        session.add_data(SignalId::HR.as_u16(), 71.0, 1000);
        session.add_data(SignalId::HR.as_u16(), 72.0, 2000);

        let result = session.handle_nack(stream_id, &[1]); // seq 1 was evicted
        assert!(result.is_empty(), "Evicted seq must not be retransmitted");
    }

    // ── handle_ack_with_bitmap ────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-021
    /// Version: V1.0
    /// Title: Test handle_ack_with_bitmap purges cumulatively acknowledged frames
    ///
    /// Description: ack_upto=3 with all-zero bitmap must purge seq 1,2,3 and leave
    ///              seq 4,5 in the buffer. No retransmits since bitmap is all zeros.
    #[test]
    fn test_handle_ack_with_bitmap_cumulative_purge() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        for i in 0u64..5 {
            session.add_data(SignalId::HR.as_u16(), i as f32, i * 1000);
        }

        let bitmap = [0u8; 8]; // no out-of-order frames
        let retransmits = session.handle_ack_with_bitmap(1, stream_id, 3, &bitmap);

        assert!(retransmits.is_empty(), "All-zero bitmap → no retransmits");
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 2);
        let entry = session.streams.get(&stream_id).unwrap();
        let seqs: Vec<u32> = entry.tx_buffer.iter().map(|f| f.header.seq).collect();
        assert_eq!(seqs, vec![4, 5]);
    }

    /// ID SRS: SRS-TEST-BLESESSION-022
    /// Version: V1.0
    /// Title: Test handle_ack_with_bitmap detects a hole and returns retransmit
    ///
    /// Description: 5 frames buffered (seq 1-5). ack_upto=1, bitmap has bit1 set
    ///              (seq 3 received) but bit0 clear (seq 2 missing). Only seq 2
    ///              must be returned for retransmission (seq 3 is already received,
    ///              seq 4-5 are beyond the highest acked offset).
    #[test]
    fn test_handle_ack_with_bitmap_hole_retransmit() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        for i in 0u64..5 {
            session.add_data(SignalId::HR.as_u16(), i as f32, i * 1000);
        }

        // ack_upto=1; bit0=seq2 (0=missing), bit1=seq3 (1=received)
        let mut bitmap = [0u8; 8];
        bitmap[0] = 0b0000_0010; // bit1 set → seq 3 received; bit0 clear → seq 2 missing
        let retransmits = session.handle_ack_with_bitmap(1, stream_id, 1, &bitmap);

        assert_eq!(retransmits.len(), 1, "Exactly one hole (seq 2)");
        assert_eq!(retransmits[0].header.seq, 2);
        assert_ne!(
            retransmits[0].header.flags & FLAG_RETRANSMIT,
            0,
            "FLAG_RETRANSMIT must be set"
        );
    }

    /// ID SRS: SRS-TEST-BLESESSION-023
    /// Version: V1.0
    /// Title: Test handle_ack_with_bitmap with all-zero bitmap and no highest_acked_offset
    ///
    /// Description: When bitmap is all zeros (no out-of-order frames confirmed),
    ///              highest_acked_offset is None → no retransmits triggered.
    ///              Buffer frames above ack_upto are treated as in-flight.
    #[test]
    fn test_handle_ack_with_bitmap_in_flight_no_retransmit() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        session.add_data(SignalId::HR.as_u16(), 71.0, 1000);
        session.add_data(SignalId::HR.as_u16(), 72.0, 2000);

        // ack_upto=0, empty bitmap → nothing confirmed above base, frames are in-flight
        let bitmap = [0u8; 8];
        let retransmits = session.handle_ack_with_bitmap(1, stream_id, 0, &bitmap);

        assert!(
            retransmits.is_empty(),
            "In-flight frames must not be retransmitted"
        );
        // All 3 frames still in buffer (ack_upto=0 purges nothing)
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 3);
    }

    /// ID SRS: SRS-TEST-BLESESSION-024
    /// Version: V1.0
    /// Title: Test handle_ack_with_bitmap new session_id resets all streams
    ///
    /// Description: If session_id in the bitmap-ACK differs from current_session_id,
    ///              all stream buffers and sequence counters must be cleared.
    #[test]
    fn test_handle_ack_with_bitmap_new_session_resets() {
        let mut session = BleSessionState::new(1);
        let stream_id = session.subscribe(SignalId::HR.as_u16());

        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        session.add_data(SignalId::HR.as_u16(), 71.0, 1000);
        assert_eq!(session.get_pending_count(SignalId::HR.as_u16()), 2);

        let bitmap = [0u8; 8];
        let retransmits = session.handle_ack_with_bitmap(42, stream_id, 0, &bitmap); // new session_id=42

        assert!(retransmits.is_empty());
        assert_eq!(session.current_session_id, 42);
        assert_eq!(session.total_pending(), 0);
        assert!(session.is_subscribed(SignalId::HR.as_u16()));
    }

    /// ID SRS: SRS-TEST-BLESESSION-025
    /// Version: V1.0
    /// subscribe_with_stream_id does NOT advance next_stream_id when preferred_id < current next
    #[test]
    fn test_subscribe_with_stream_id_lower_than_next_does_not_advance() {
        let mut session = BleSessionState::new(1);
        // Advance next_stream_id to 5 by subscribing four signals
        session.subscribe(0xAA01);
        session.subscribe(0xAA02);
        session.subscribe(0xAA03);
        session.subscribe(0xAA04);
        assert_eq!(session.next_stream_id, 5);

        // Now subscribe HR with a preferred_stream_id lower than next_stream_id
        let sid = session.subscribe_with_stream_id(SignalId::HR.as_u16(), 2);
        // preferred_id=2 already exists so returns its own stream (idempotent)
        // but 2 < 5 so next_stream_id must NOT be advanced
        let _ = sid; // stream_id allocation might reuse 2 if it was the HR stream
        assert_eq!(
            session.next_stream_id, 5,
            "next_stream_id must stay at 5 when preferred_id < current next"
        );
    }

    /// ID SRS: SRS-TEST-BLESESSION-026
    /// Version: V1.0
    /// subscribe_with_stream_id with preferred_id < next does not advance counter (fresh signal)
    #[test]
    fn test_subscribe_with_stream_id_preferred_lower_than_next_fresh() {
        let mut session = BleSessionState::new(1);
        // Subscribe SpO2 first to get stream_id=1, advancing next to 2
        session.subscribe(SignalId::SpO2.as_u16());
        assert_eq!(session.next_stream_id, 2);

        // Now subscribe Temperature with preferred_id=1 (< next_stream_id=2)
        // HR is fresh (not yet subscribed) but preferred_id=1 < next=2 → counter stays at 2
        let sid = session.subscribe_with_stream_id(SignalId::Temperature.as_u16(), 1);
        assert_eq!(sid, 1);
        assert_eq!(session.next_stream_id, 2, "next_stream_id must not regress");
    }

    /// ID SRS: SRS-TEST-BLESESSION-027
    /// Version: V1.0
    /// unsubscribe on a signal_id that was never subscribed is a silent no-op
    #[test]
    fn test_unsubscribe_noop_on_never_subscribed() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        // Unsubscribe SpO2 which was never subscribed — must not panic or change state
        session.unsubscribe(SignalId::SpO2.as_u16());
        assert!(
            session.is_subscribed(SignalId::HR.as_u16()),
            "HR must still be subscribed"
        );
        assert_eq!(session.streams.len(), 1);
    }

    /// ID SRS: SRS-TEST-BLESESSION-028
    /// Version: V1.0
    /// handle_ack with an unknown stream_id is a silent no-op (no panic, no state change)
    #[test]
    fn test_handle_ack_unknown_stream_id_noop() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.add_data(SignalId::HR.as_u16(), 70.0, 0);
        let pending_before = session.total_pending();

        // ACK for stream_id=999 which does not exist
        session.handle_ack(1, 999, 100);

        assert_eq!(
            session.total_pending(),
            pending_before,
            "unknown stream ACK must not change buffer"
        );
    }

    /// ID SRS: SRS-TEST-BLESESSION-029
    /// Version: V1.0
    /// get_pending_count for a signal_id that was never subscribed returns 0
    #[test]
    fn test_get_pending_count_unsubscribed_returns_zero() {
        let session = BleSessionState::new(1);
        assert_eq!(session.get_pending_count(SignalId::Temperature.as_u16()), 0);
    }

    // ── unsubscribe_all ───────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-031
    /// Version: V1.0
    /// Title: unsubscribe_all clears all maps
    ///
    /// Description: After subscribing HR+SpO2 and calling unsubscribe_all(), both
    ///              signal_to_stream and streams must be empty.
    #[test]
    fn test_unsubscribe_all_clears_maps() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.subscribe(SignalId::SpO2.as_u16());
        assert_eq!(session.streams.len(), 2);
        assert_eq!(session.signal_to_stream.len(), 2);

        session.unsubscribe_all();

        assert!(
            session.signal_to_stream.is_empty(),
            "signal_to_stream must be empty after unsubscribe_all"
        );
        assert!(
            session.streams.is_empty(),
            "streams must be empty after unsubscribe_all"
        );
    }

    /// ID SRS: SRS-TEST-BLESESSION-032
    /// Version: V1.0
    /// Title: unsubscribe_all then re-subscribe leaves only the new signal
    ///
    /// Description: Subscribe HR+SpO2, call unsubscribe_all(), then subscribe only HR.
    ///              Exactly one stream (HR) must be active; SpO2 must be gone.
    #[test]
    fn test_unsubscribe_all_then_resubscribe_only_hr() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.subscribe(SignalId::SpO2.as_u16());

        session.unsubscribe_all();
        session.subscribe(SignalId::HR.as_u16());

        assert!(
            session.is_subscribed(SignalId::HR.as_u16()),
            "HR must be subscribed after re-subscribe"
        );
        assert!(
            !session.is_subscribed(SignalId::SpO2.as_u16()),
            "SpO2 must NOT be subscribed after unsubscribe_all + HR-only re-subscribe"
        );
        assert_eq!(session.streams.len(), 1);
        assert_eq!(session.signal_to_stream.len(), 1);
    }

    // ── on_disconnect ─────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-033
    /// Version: V1.0
    /// Title: on_disconnect clears all streams and signal mappings
    ///
    /// Description: After subscribing HR+SpO2 and calling on_disconnect(),
    ///              signal_to_stream and streams must be empty.
    #[test]
    fn test_on_disconnect_clears_streams() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.subscribe(SignalId::SpO2.as_u16());
        session.add_data(SignalId::HR.as_u16(), 70.0, 0);

        session.on_disconnect();

        assert!(
            session.signal_to_stream.is_empty(),
            "signal_to_stream must be empty after on_disconnect"
        );
        assert!(
            session.streams.is_empty(),
            "streams must be empty after on_disconnect"
        );
        assert_eq!(session.total_pending(), 0);
    }

    /// ID SRS: SRS-TEST-BLESESSION-034
    /// Version: V1.0
    /// Title: on_disconnect increments session_id by 1 (wrapping)
    ///
    /// Description: session_id shall be wrapping_add(1) after on_disconnect.
    #[test]
    fn test_on_disconnect_increments_session_id() {
        let mut session = BleSessionState::new(5);
        session.on_disconnect();
        assert_eq!(session.current_session_id, 6);

        // Wrap-around: u16::MAX wraps to 0
        let mut session2 = BleSessionState::new(u16::MAX);
        session2.on_disconnect();
        assert_eq!(session2.current_session_id, 0);
    }

    /// ID SRS: SRS-TEST-BLESESSION-035
    /// Version: V1.0
    /// Title: on_disconnect resets next_stream_id to 1
    ///
    /// Description: After on_disconnect, the stream_id allocator resets so the
    ///              next subscribe call gets stream_id=1 again.
    #[test]
    fn test_on_disconnect_resets_next_stream_id() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.subscribe(SignalId::SpO2.as_u16());
        assert_eq!(session.next_stream_id, 3);

        session.on_disconnect();

        assert_eq!(session.next_stream_id, 1);
        // Re-subscribing after disconnect starts from stream_id=1
        let new_stream = session.subscribe(SignalId::HR.as_u16());
        assert_eq!(new_stream, 1);
    }

    /// ID SRS: SRS-TEST-BLESESSION-036
    /// Version: V1.0
    /// Title: Double on_disconnect is safe (no panic, increments session_id twice)
    ///
    /// Description: Calling on_disconnect twice consecutively must not panic.
    ///              session_id is incremented each time.
    #[test]
    fn test_double_on_disconnect_no_panic() {
        let mut session = BleSessionState::new(10);
        session.subscribe(SignalId::HR.as_u16());

        session.on_disconnect();
        assert_eq!(session.current_session_id, 11);
        assert!(session.streams.is_empty());

        // Second call on already-reset state must be a no-op except session_id increment
        session.on_disconnect();
        assert_eq!(session.current_session_id, 12);
        assert!(session.streams.is_empty());
        assert_eq!(session.next_stream_id, 1);
    }

    /// ID SRS: SRS-TEST-BLESESSION-030
    /// Version: V1.0
    /// handle_ack_with_bitmap with unknown stream_id returns empty Vec (no panic)
    #[test]
    fn test_handle_ack_with_bitmap_unknown_stream_noop() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        session.add_data(SignalId::HR.as_u16(), 70.0, 0);

        let bitmap = [0u8; 8];
        let retransmits = session.handle_ack_with_bitmap(1, 999, 0, &bitmap);
        assert!(
            retransmits.is_empty(),
            "unknown stream bitmap-ACK must return empty Vec"
        );
        assert_eq!(session.total_pending(), 1, "buffer must be unchanged");
    }

    // ── record_history dedup + flush_tx_to_history ───────────────────────────

    /// ID SRS: SRS-TEST-BLESESSION-037
    /// Title: record_history dedup guard skips samples with t0_ms ≤ last recorded
    ///
    /// Description: Calling record_history twice with the same t0_ms (or an older one)
    ///              must not create a duplicate entry in the history buffer.
    ///
    /// Version: V1.0
    #[test]
    fn test_record_history_dedup_skips_duplicate_timestamp() {
        let mut session = BleSessionState::new(1);

        session.record_history(0x0101, 72.0, 1000);
        session.record_history(0x0101, 73.0, 1000); // same t0_ms — must be skipped
        session.record_history(0x0101, 71.0, 500); // older t0_ms — must be skipped
        session.record_history(0x0101, 74.0, 2000); // newer — must be recorded

        let buf = session.history.get(&0x0101).unwrap();
        assert_eq!(buf.len(), 2, "only 2 unique timestamps should be recorded");
        assert_eq!(buf[0], (1000, 72.0));
        assert_eq!(buf[1], (2000, 74.0));
    }

    /// ID SRS: SRS-TEST-BLESESSION-038
    /// Title: flush_tx_to_history rescues frames not yet in history
    ///
    /// Description: When add_data is called without a prior record_history call,
    ///              the frame is in the tx_buffer but not in history. on_disconnect()
    ///              must flush it into history via flush_tx_to_history() so that the
    ///              data survives the session reset and is available for BACKLOG replay.
    ///
    /// Version: V1.0
    #[test]
    fn test_flush_tx_to_history_rescues_unrecorded_frames() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        // add_data directly, without record_history — simulates a code path that
        // bypasses the normal output() ordering.
        session.add_data(SignalId::HR.as_u16(), 75.0, 5000);
        session.add_data(SignalId::HR.as_u16(), 76.0, 6000);

        assert!(
            !session.history.contains_key(&SignalId::HR.as_u16())
                || session.history[&SignalId::HR.as_u16()].is_empty(),
            "history must be empty before flush"
        );

        let flushed = session.flush_tx_to_history();
        assert_eq!(flushed, 2, "both frames must be flushed into history");

        let buf = session.history.get(&SignalId::HR.as_u16()).unwrap();
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], (5000, 75.0));
        assert_eq!(buf[1], (6000, 76.0));
    }

    /// ID SRS: SRS-TEST-BLESESSION-039
    /// Title: flush_tx_to_history is idempotent when history already up to date
    ///
    /// Description: When record_history has been called before add_data (normal path),
    ///              flush_tx_to_history must return 0 (no new entries added) because
    ///              the dedup guard in record_history prevents duplicates.
    ///
    /// Version: V1.0
    #[test]
    fn test_flush_tx_to_history_idempotent_when_already_recorded() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        // Normal output() order: record_history first, then add_data
        session.record_history(SignalId::HR.as_u16(), 75.0, 5000);
        session.add_data(SignalId::HR.as_u16(), 75.0, 5000);

        let flushed = session.flush_tx_to_history();
        assert_eq!(
            flushed, 0,
            "no new entries expected when history already current"
        );

        let buf = session.history.get(&SignalId::HR.as_u16()).unwrap();
        assert_eq!(buf.len(), 1, "history must not have duplicates");
    }

    /// ID SRS: SRS-TEST-BLESESSION-040
    /// Title: on_disconnect calls flush_tx_to_history before clearing streams
    ///
    /// Description: When on_disconnect() is called, any unrecorded frames in the
    ///              tx_buffer must be present in history after the reset, even though
    ///              the streams themselves are cleared.
    ///
    /// Version: V1.0
    #[test]
    fn test_on_disconnect_flushes_tx_before_clearing() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        // Skip record_history to simulate a frame not yet in history
        session.add_data(SignalId::HR.as_u16(), 80.0, 9000);
        assert!(
            !session.history.contains_key(&SignalId::HR.as_u16())
                || session.history[&SignalId::HR.as_u16()].is_empty(),
            "history must be empty before on_disconnect"
        );

        session.on_disconnect();

        // Streams are cleared
        assert!(session.streams.is_empty());
        // But the data is now in history
        let buf = session.history.get(&SignalId::HR.as_u16()).unwrap();
        assert_eq!(buf.len(), 1, "flushed frame must survive session reset");
        assert_eq!(buf[0], (9000, 80.0));
    }

    /// ID SRS: SRS-TEST-BLESESSION-041
    /// Title: serialize/load history checkpoint roundtrip
    ///
    /// Description: Serializing a populated history ring buffer and loading the
    ///              resulting bytes into a fresh session must reproduce all samples
    ///              exactly (same signal_ids, timestamps, values).
    ///
    /// Version: V1.0
    #[test]
    fn test_checkpoint_roundtrip() {
        let mut src = BleSessionState::new(1);
        src.record_history(SignalId::HR.as_u16(), 72.0, 1000);
        src.record_history(SignalId::HR.as_u16(), 73.0, 2000);
        src.record_history(0x0102, 98.0, 1500);

        let bytes = src.serialize_history_to_bytes();
        assert!(bytes.len() > 20, "serialized bytes must exceed header size");

        let mut dst = BleSessionState::new(1);
        let n = dst.load_history_from_bytes(&bytes).unwrap();
        assert_eq!(n, 3, "must load all 3 samples");

        let hr = dst.history.get(&SignalId::HR.as_u16()).unwrap();
        assert_eq!(hr.len(), 2);
        assert_eq!(hr[0], (1000, 72.0));
        assert_eq!(hr[1], (2000, 73.0));

        let spo2 = dst.history.get(&0x0102u16).unwrap();
        assert_eq!(spo2.len(), 1);
        assert_eq!(spo2[0], (1500, 98.0));
    }

    /// ID SRS: SRS-TEST-BLESESSION-042
    /// Title: load_history_from_bytes rejects malformed input
    ///
    /// Description: load_history_from_bytes must return Err on truncated data,
    ///              wrong magic, and unsupported version without panicking.
    ///
    /// Version: V1.0
    #[test]
    fn test_checkpoint_load_errors() {
        let mut session = BleSessionState::new(1);

        // Too short
        assert!(session.load_history_from_bytes(&[0u8; 5]).is_err());

        // Wrong magic
        let mut bad_magic = vec![0u8; 20];
        bad_magic[0] = 0xDE;
        bad_magic[1] = 0xAD;
        assert!(session.load_history_from_bytes(&bad_magic).is_err());

        // Wrong version
        let mut bad_ver = vec![0u8; 20];
        bad_ver[0..4].copy_from_slice(&0x424C_4548u32.to_le_bytes());
        bad_ver[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert!(session.load_history_from_bytes(&bad_ver).is_err());

        // Truncated signal data
        let mut src = BleSessionState::new(1);
        src.record_history(SignalId::HR.as_u16(), 72.0, 1000);
        let full = src.serialize_history_to_bytes();
        // Cut off mid-sample
        let truncated = &full[..full.len() - 4];
        assert!(session.load_history_from_bytes(truncated).is_err());
    }

    // History retention — age eviction & with_history_retention

    /// ID SRS: SRS-TEST-BLESESSION-045
    /// Title: Test record_history evicts samples older than max_history_age_ms
    ///
    /// Description: When a new sample arrives, record_history must evict any existing
    ///              sample whose t0_ms < (new_t0_ms - max_history_age_ms). This prevents
    ///              sparse signals from accumulating multi-day history within the count cap.
    ///
    /// Version: V1.0
    #[test]
    fn test_record_history_age_eviction() {
        // 3 s retention window
        let mut session = BleSessionState::new(1).with_history_retention(3);

        session.record_history(0x0101, 70.0, 0); // t = 0 ms
        session.record_history(0x0101, 71.0, 1_000); // t = 1 000 ms

        // t = 4 000 ms: cutoff = 4000 - 3000 = 1000. Samples with ts < 1000 evicted.
        // t=0 (ts=0 < 1000) → evicted; t=1000 (NOT < 1000) → kept
        session.record_history(0x0101, 74.0, 4_000);

        let buf = session.history.get(&0x0101).unwrap();
        assert_eq!(
            buf.len(),
            2,
            "t=0 ms must be evicted; t=1000 ms and t=4000 ms remain"
        );
        assert_eq!(buf[0].0, 1_000, "oldest remaining must be t=1000 ms");
        assert_eq!(buf[1].0, 4_000, "newest must be t=4000 ms");
    }

    /// ID SRS: SRS-TEST-BLESESSION-046
    /// Title: Test with_history_retention sets both max_history_size and max_history_age_ms
    ///
    /// Description: with_history_retention(N) must set max_history_size = N and
    ///              max_history_age_ms = N * 1000.
    ///
    /// Version: V1.0
    #[test]
    fn test_with_history_retention_sets_both_fields() {
        let session = BleSessionState::new(1).with_history_retention(7200);
        assert_eq!(session.max_history_size, 7200);
        assert_eq!(session.max_history_age_ms, 7_200_000);
    }

    // eager seq reservation / F4: replay in tx_buffer

    /// ID SRS: SRS-TEST-BLESESSION-043
    /// Title: Test start_replay reserves the seq block so concurrent live frames don't collide
    ///
    /// Description: start_replay must reserve a contiguous seq block for the N replay frames
    ///              by advancing last_seq by N eagerly. This guarantees that a live add_data()
    ///              running concurrently while the replay burst drains (the send loop does NOT
    ///              hold the session lock) receives a sequence number AFTER the reserved block,
    ///              never colliding with a replay frame's seq. An interrupted replay leaves its
    ///              un-sent frames in tx_buffer (F4), so the gap is recoverable, not burned.
    ///
    /// Version: V2.0
    #[test]
    fn test_start_replay_reserves_seq_block() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        let stream_id = session.get_stream_id(SignalId::HR.as_u16()).unwrap();

        session.record_history(SignalId::HR.as_u16(), 70.0, 1000);
        session.record_history(SignalId::HR.as_u16(), 71.0, 2000);
        session.record_history(SignalId::HR.as_u16(), 72.0, 3000);

        let frames = session.start_replay(SignalId::HR.as_u16(), 0);
        assert_eq!(frames.len(), 3);
        // Replay frames occupy seq 1, 2, 3
        assert_eq!(frames[0].header.seq, 1);
        assert_eq!(frames[2].header.seq, 3);

        // Eager reservation: last_seq advanced past the whole block
        let entry = session.streams.get(&stream_id).unwrap();
        assert_eq!(
            entry.last_seq, 3,
            "last_seq must reserve the full replay block (eager) so live frames come after"
        );

        // A live frame produced concurrently must get seq 4 — no collision with replay seq 1..3
        let live = session.add_data(SignalId::HR.as_u16(), 73.0, 4000).unwrap();
        assert_eq!(
            live.header.seq, 4,
            "concurrent live frame must follow the reserved block, never collide with replay seq"
        );
        assert_eq!(
            live.header.flags & FLAG_BACKLOG,
            0,
            "live frame must NOT carry FLAG_BACKLOG"
        );
    }

    /// ID SRS: SRS-TEST-BLESESSION-044
    /// Title: Test start_replay populates tx_buffer for NACK recovery (F4)
    ///
    /// Description: All frames returned by start_replay must be present in tx_buffer so that
    ///              handle_nack can retransmit them if the Central reports a loss during the
    ///              backlog burst. An interrupted replay therefore leaves a *recoverable* gap.
    ///
    /// Version: V2.0
    #[test]
    fn test_start_replay_frames_in_tx_buffer() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());
        let stream_id = session.get_stream_id(SignalId::HR.as_u16()).unwrap();

        session.record_history(SignalId::HR.as_u16(), 70.0, 1000);
        session.record_history(SignalId::HR.as_u16(), 71.0, 2000);
        session.record_history(SignalId::HR.as_u16(), 72.0, 3000);

        let frames = session.start_replay(SignalId::HR.as_u16(), 0);
        assert_eq!(frames.len(), 3);

        // F4: all replay frames must be in the retransmit buffer immediately after start_replay
        assert_eq!(
            session.get_pending_count(SignalId::HR.as_u16()),
            3,
            "tx_buffer must hold all replay frames for NACK recovery"
        );

        // NACK for seq=2 must be retransmittable from tx_buffer
        let retransmits = session.handle_nack(stream_id, &[2]);
        assert_eq!(
            retransmits.len(),
            1,
            "handle_nack must find seq=2 in tx_buffer"
        );
        assert_eq!(retransmits[0].header.seq, 2);
        assert_ne!(
            retransmits[0].header.flags & FLAG_RETRANSMIT,
            0,
            "FLAG_RETRANSMIT must be set on retransmitted replay frame"
        );
    }

    /// ID SRS: SRS-TEST-BLESESSION-047
    /// Title: Test tx_buffer keeps only the most recent frames for a backlog larger than the cap
    ///
    /// Description: When the replay backlog exceeds max_buffer_size, start_replay must retain
    ///              the most recent max_buffer_size frames in tx_buffer (FIFO eviction) without
    ///              panicking or logging per-frame. Older losses are recovered by re-subscribing.
    ///
    /// Version: V1.0
    #[test]
    fn test_start_replay_tx_buffer_bounded_for_large_backlog() {
        let mut session = BleSessionState::new(1).with_buffer_size(2);
        session.subscribe(SignalId::HR.as_u16());

        session.record_history(SignalId::HR.as_u16(), 70.0, 1000);
        session.record_history(SignalId::HR.as_u16(), 71.0, 2000);
        session.record_history(SignalId::HR.as_u16(), 72.0, 3000);

        let frames = session.start_replay(SignalId::HR.as_u16(), 0);
        assert_eq!(frames.len(), 3, "all 3 frames are returned for sending");

        // tx_buffer capped at 2 → only the newest 2 (seq 2, 3) retained
        let stream_id = session.get_stream_id(SignalId::HR.as_u16()).unwrap();
        let entry = session.streams.get(&stream_id).unwrap();
        assert_eq!(
            entry.tx_buffer.len(),
            2,
            "tx_buffer must respect max_buffer_size"
        );
        assert_eq!(entry.tx_buffer.front().unwrap().header.seq, 2);
        assert_eq!(entry.tx_buffer.back().unwrap().header.seq, 3);
    }
}
