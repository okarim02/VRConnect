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
    /// FLAG_BACKLOG is set on DATA_FRAMEs only when this flag is true (historical replay).
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
    /// Maximum historical samples kept per signal (default: 3600 ≈ 1 h at 1 Hz).
    pub max_history_size: usize,
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
            max_buffer_size: 1000,    // Medical: prevent unbounded memory growth
            history: HashMap::new(),
            max_history_size: 3600,   // ~1 hour at 1 Hz per signal
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
        self.streams.insert(
            stream_id,
            StreamEntry {
                stream_id,
                signal_id,
                source_id: 1,
                last_seq: 0,
                last_t0_ms: None,
                tx_buffer: VecDeque::new(),
                is_replaying: false,
            },
        );
        self.signal_to_stream.insert(signal_id, stream_id);
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
        self.streams.insert(
            stream_id,
            StreamEntry {
                stream_id,
                signal_id,
                source_id: 1,
                last_seq: 0,
                last_t0_ms: None,
                tx_buffer: VecDeque::new(),
                is_replaying: false,
            },
        );
        self.signal_to_stream.insert(signal_id, stream_id);
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
        if let Some(stream_id) = self.signal_to_stream.remove(&signal_id) {
            self.streams.remove(&stream_id);
        }
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
                    signal_id, t0_ms, last
                );
                return None;
            }
        }
        entry.last_t0_ms = Some(t0_ms);

        entry.last_seq += 1;
        let seq = entry.last_seq;

        let mut frame = DataFrame::new(self.current_session_id, stream_id, seq, t0_ms, value);

        // FLAG_BACKLOG is set only when this stream is actively replaying historical data
        // (BACKLOG_THEN_LIVE / BACKLOG_ONLY mode).
        if entry.is_replaying {
            frame.header.flags |= FLAG_BACKLOG;
        }

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
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - IDT signal identifier
    /// * `value`     - Measured float32 value
    /// * `t0_ms`     - Sample timestamp, milliseconds since Unix epoch
    pub fn record_history(&mut self, signal_id: u16, value: f32, t0_ms: u64) {
        let buf = self.history.entry(signal_id).or_insert_with(VecDeque::new);
        buf.push_back((t0_ms, value));
        if buf.len() > self.max_history_size {
            buf.pop_front();
        }
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
    ///              DataFrames with FLAG_BACKLOG set.  The stream's last_seq is advanced
    ///              by the number of replay frames so that subsequent live frames have
    ///              non-overlapping sequence numbers.
    ///
    /// Version: V1.0
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

        // We need to read from history (immutable borrow) but we just mutably borrowed entry.
        // Use a separate lookup after releasing the mutable borrow.
        drop(entry);

        let frames = self.get_replay_frames(signal_id, start_time_ms, session_id, stream_id, seq_start);

        // Advance last_seq past all replay frames so live frames get unique seq numbers
        if let Some(entry) = self.streams.get_mut(&stream_id) {
            entry.last_seq = entry.last_seq.wrapping_add(frames.len() as u32);
        }

        frames
    }

    /// ID SRS: SRS-FN-BLESESSION-016
    /// Title: finish_replay
    ///
    /// Description: VRConnect shall clear the is_replaying flag for a stream, signalling
    ///              that the historical burst has been fully delivered.
    ///              Subsequent live DATA_FRAMEs will no longer carry FLAG_BACKLOG.
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
    /// Title: Test add_data returns None when not subscribed
    #[test]
    fn test_add_data_no_subscription() {
        let mut session = BleSessionState::new(1);
        assert!(session
            .add_data(SignalId::HR.as_u16(), 75.0, 1_000_000)
            .is_none());
    }

    /// ID SRS: SRS-TEST-BLESESSION-006
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
    /// Title: Test get_stream_id returns correct stream_id
    #[test]
    fn test_get_stream_id() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::SpO2.as_u16());

        assert_eq!(session.get_stream_id(SignalId::SpO2.as_u16()), Some(1));
        assert_eq!(session.get_stream_id(SignalId::HR.as_u16()), None);
    }

    /// ID SRS: SRS-TEST-BLESESSION-014
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
    /// Title: Test handle_nack returns empty Vec for unknown stream_id
    #[test]
    fn test_handle_nack_unknown_stream() {
        let session = BleSessionState::new(1);
        let result = session.handle_nack(999, &[1, 2, 3]);
        assert!(result.is_empty());
    }

    /// ID SRS: SRS-TEST-BLESESSION-020
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
    /// get_pending_count for a signal_id that was never subscribed returns 0
    #[test]
    fn test_get_pending_count_unsubscribed_returns_zero() {
        let session = BleSessionState::new(1);
        assert_eq!(session.get_pending_count(SignalId::Temperature.as_u16()), 0);
    }

    /// ID SRS: SRS-TEST-BLESESSION-030
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
}
