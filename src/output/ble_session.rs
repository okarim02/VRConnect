// /src/output/ble_session.rs
// Module: output.ble_session
// Purpose: Multi-stream Session Engine for the IDT ("ICU Data Transport") BLE protocol — V2.0
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

use crate::domain::ble_protocol::{DataFrame, FLAG_RETRANSMIT};
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
/// Version: V2.0
pub struct StreamEntry {
    /// Allocated IDT stream_id for this signal
    pub stream_id: u16,
    /// IDT signal_id (e.g. 0x0101=HR, 0x0102=SpO2, 0x0103=Temperature)
    pub signal_id: u16,
    /// Source identifier (always 1 for scope signals in V1)
    pub source_id: u8,
    /// Last sent sequence number for this stream (0 = no frame sent yet)
    pub last_seq: u32,
    /// Retransmit buffer: bounded VecDeque of sent-but-unacknowledged frames
    pub tx_buffer: VecDeque<DataFrame>,
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
/// Version: V2.0
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
}

impl BleSessionState {
    /// ID SRS: SRS-FN-BLESESSION-001
    /// Title: new
    ///
    /// Description: VRConnect shall create a new BleSessionState with the given
    ///              session ID and no active streams.
    ///
    /// Version: V2.0
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
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-002
    /// Title: with_buffer_size
    ///
    /// Description: VRConnect shall allow configuring the maximum retransmit buffer
    ///              size per stream. Used for testing and resource-constrained deployments.
    ///
    /// Version: V2.0
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

    /// ID SRS: SRS-FN-BLESESSION-003
    /// Title: subscribe
    ///
    /// Description: VRConnect shall allocate a new IDT stream_id for a signal_id on
    ///              first subscription. Subsequent calls with the same signal_id are
    ///              idempotent: the same stream_id is returned without creating a new stream.
    ///
    /// Version: V2.0
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
                tx_buffer: VecDeque::new(),
            },
        );
        self.signal_to_stream.insert(signal_id, stream_id);
        stream_id
    }

    /// Subscribe with a caller-chosen stream_id instead of auto-allocation.
    /// Used by the TLV path to assign stream IDs that the Flutter app can match
    /// regardless of its endianness assumptions.
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
                tx_buffer: VecDeque::new(),
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
    /// Version: V2.0
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
    /// Version: V2.0
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
    /// Version: V2.0
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
    /// Version: V2.0
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

        entry.last_seq += 1;
        let seq = entry.last_seq;

        let frame = DataFrame::new(self.current_session_id, stream_id, seq, t0_ms, value);

        // Buffer for retransmission (oldest frame evicted when limit reached)
        entry.tx_buffer.push_back(frame.clone());
        while entry.tx_buffer.len() > self.max_buffer_size {
            entry.tx_buffer.pop_front();
        }

        Some(frame)
    }

    /// ID SRS: SRS-FN-BLESESSION-008
    /// Title: handle_ack
    ///
    /// Description: VRConnect shall process a cumulative IDT ACK_FRAME.
    ///   - If a new session_id is detected: reset all stream buffers and sequence counters.
    ///   - Otherwise: purge frames with seq ≤ ack_upto from the named stream's buffer.
    ///
    /// Version: V2.0
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
    /// Version: V2.0
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
    /// Version: V2.0
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
    /// Version: V2.0
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
    /// Version: V2.0
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
}
