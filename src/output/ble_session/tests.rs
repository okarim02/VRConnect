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

// ── throttle gate (period_ms) ────────────────────────────────────────────

/// ID SRS: SRS-TEST-BLESESSION-048
/// Title: add_data with period_ms=0 never throttles (non-regression)
///
/// Description: subscribe_with_period(..., 0) must behave exactly like
///              subscribe() — every strictly-increasing sample is emitted. This is
///              the case for every app deployed before this field existed.
///
/// Version: V1.0
#[test]
fn test_add_data_period_ms_zero_never_throttles() {
    let mut session = BleSessionState::new(1);
    session.subscribe_with_period(SignalId::HR.as_u16(), None, 0);

    let f1 = session.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
    let f2 = session.add_data(SignalId::HR.as_u16(), 71.0, 990).unwrap();
    let f3 = session.add_data(SignalId::HR.as_u16(), 72.0, 1980).unwrap();

    assert_eq!(f1.header.seq, 1);
    assert_eq!(f2.header.seq, 2);
    assert_eq!(f3.header.seq, 3);
}

/// ID SRS: SRS-TEST-BLESESSION-049
/// Title: add_data throttles a sample arriving before period_ms has elapsed
///
/// Description: With period_ms=5000, the first sample is always emitted; a second
///              sample before the 5000 ms mark returns None and must NOT advance
///              last_seq — a throttled sample must never burn a sequence number.
///
/// Version: V1.0
#[test]
fn test_add_data_throttled_sample_returns_none_and_does_not_advance_seq() {
    let mut session = BleSessionState::new(1);
    session.subscribe_with_period(SignalId::HR.as_u16(), None, 5_000);

    let f1 = session.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
    assert_eq!(f1.header.seq, 1);

    let throttled = session.add_data(SignalId::HR.as_u16(), 71.0, 4_000);
    assert!(throttled.is_none());

    let stream_id = session.get_stream_id(SignalId::HR.as_u16()).unwrap();
    assert_eq!(
        session.streams.get(&stream_id).unwrap().last_seq,
        1,
        "a throttled sample must not consume a sequence number"
    );
}

/// ID SRS: SRS-TEST-BLESESSION-050
/// Title: add_data emits again once period_ms has elapsed, and updates last_sent_t0_ms
///
/// Description: A sample at or after last_sent_t0_ms + period_ms is emitted; the
///              throttle gate then re-arms from that new t0_ms.
///
/// Version: V1.0
#[test]
fn test_add_data_emits_at_period_boundary_and_updates_last_sent() {
    let mut session = BleSessionState::new(1);
    session.subscribe_with_period(SignalId::HR.as_u16(), None, 5_000);

    session.add_data(SignalId::HR.as_u16(), 70.0, 0).unwrap();
    assert!(session
        .add_data(SignalId::HR.as_u16(), 71.0, 4_999)
        .is_none());

    let f2 = session
        .add_data(SignalId::HR.as_u16(), 72.0, 5_000)
        .unwrap();
    assert_eq!(f2.header.seq, 2);

    let stream_id = session.get_stream_id(SignalId::HR.as_u16()).unwrap();
    assert_eq!(
        session.streams.get(&stream_id).unwrap().last_sent_t0_ms,
        Some(5_000)
    );

    // Gate re-arms from the new last_sent_t0_ms: 9_999 is still too soon.
    assert!(session
        .add_data(SignalId::HR.as_u16(), 73.0, 9_999)
        .is_none());
    let f3 = session
        .add_data(SignalId::HR.as_u16(), 74.0, 10_000)
        .unwrap();
    assert_eq!(f3.header.seq, 3);
}

/// ID SRS: SRS-TEST-BLESESSION-051
/// Title: cross-message dedup (last_t0_ms) still works independently of the throttle gate
///
/// Description: A duplicate/old t0_ms is rejected by the existing dedup check before
///              the throttle gate is even evaluated, whether or not period_ms is set.
///
/// Version: V1.0
#[test]
fn test_add_data_dedup_independent_of_throttle_gate() {
    let mut session = BleSessionState::new(1);
    session.subscribe_with_period(SignalId::HR.as_u16(), None, 5_000);

    session
        .add_data(SignalId::HR.as_u16(), 70.0, 10_000)
        .unwrap();
    // Same timestamp resent (VitalRecorder sliding-window behavior) — dedup, not throttle.
    assert!(session
        .add_data(SignalId::HR.as_u16(), 70.0, 10_000)
        .is_none());
    // Older timestamp — also dedup.
    assert!(session
        .add_data(SignalId::HR.as_u16(), 70.0, 9_000)
        .is_none());
}

/// ID SRS: SRS-TEST-BLESESSION-052
/// Title: record_history() keeps full resolution regardless of live BLE throttling
///
/// Description: This is the data-integrity guarantee behind the whole feature: only
///              the live BLE stream (add_data) is reduced by period_ms. Six samples
///              recorded to history (as output() always does, before add_data()) at
///              1000 ms spacing, with a 5000 ms live throttle, must yield exactly 2
///              live frames but the full 6-sample backlog via get_replay_frames() —
///              proving a throttled sample stays completely recoverable.
///
/// Version: V1.0
#[test]
fn test_throttled_stream_keeps_full_resolution_in_history_and_replay() {
    let mut session = BleSessionState::new(1);
    session.subscribe_with_period(SignalId::HR.as_u16(), Some(1), 5_000);

    let mut emitted = 0;
    for i in 0u64..=5 {
        let t0_ms = i * 1000;
        // Mirrors output(): record_history() unconditionally, then add_data().
        session.record_history(SignalId::HR.as_u16(), 70.0 + i as f32, t0_ms);
        if session
            .add_data(SignalId::HR.as_u16(), 70.0 + i as f32, t0_ms)
            .is_some()
        {
            emitted += 1;
        }
    }

    assert_eq!(
        emitted, 2,
        "live BLE stream: only t0=0 and t0=5000 pass the gate"
    );

    let replay = session.get_replay_frames(SignalId::HR.as_u16(), 0, 0, 1, 1, 1);
    assert_eq!(
        replay.len(),
        6,
        "history/backlog must retain all 6 samples regardless of live throttling"
    );

    let stream_id = session.get_stream_id(SignalId::HR.as_u16()).unwrap();
    assert_eq!(
        session.streams.get(&stream_id).unwrap().last_seq,
        2,
        "no sequence number was burned by a throttled sample"
    );
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

    let frames = session.start_replay(SignalId::HR.as_u16(), 0, 0);
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

    let frames = session.start_replay(SignalId::HR.as_u16(), 0, 0);
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

/// ID SRS: SRS-TEST-BLESESSION-054
/// Title: throttle cadence survives a re-subscribe (unsubscribe_all) and a session reset
///
/// Description: VRConnect shall keep a requested period_ms across re-subscriptions: after
///              an ACKed sample, unsubscribe_all() + re-subscribe (field: the app reconnects
///              every 10-30 min) or on_disconnect() + re-subscribe must not restart the
///              clock — a sample within the period is still throttled, and the next one
///              goes out exactly one period after the last delivered sample. Covered for
///              both ACK paths (handle_ack and handle_ack_with_bitmap — the latter is the
///              one the live Data_IN handler uses).
///
/// Version: V1.0
#[test]
fn test_throttle_cadence_survives_resubscribe_and_reset() {
    let hr = SignalId::HR.as_u16();
    for (reset_via_disconnect, bitmap_ack) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let mut s = BleSessionState::new(1);
        s.subscribe_with_period(hr, Some(1), 3_600_000);
        assert!(s.add_data(hr, 70.0, 1_000_000).is_some());
        let session = s.current_session_id;
        if bitmap_ack {
            s.handle_ack_with_bitmap(session, 1, 1, &[0u8; 8]);
        } else {
            s.handle_ack(session, 1, 1); // delivered
        }

        if reset_via_disconnect {
            s.on_disconnect();
        } else {
            s.unsubscribe_all();
        }
        s.subscribe_with_period(hr, Some(1), 3_600_000);

        assert!(
            s.add_data(hr, 71.0, 1_000_000 + 600_000).is_none(),
            "10 min after the last delivered sample: still throttled (disconnect={})",
            reset_via_disconnect
        );
        assert!(
            s.add_data(hr, 72.0, 1_000_000 + 3_600_000).is_some(),
            "exactly one period later: sent (disconnect={})",
            reset_via_disconnect
        );
    }
}

/// ID SRS: SRS-TEST-BLESESSION-055
/// Title: an unACKed throttled sample is re-sent immediately after re-subscribe
///
/// Description: If the last emitted sample was still pending (never ACKed) when the
///              stream was cleared — by unsubscribe_all(), unsubscribe() or on_disconnect()
///              — VRConnect shall not keep its cadence: the Central may never have received
///              it, so the first sample after the next subscription must go out at once.
///
/// Version: V1.0
#[test]
fn test_unacked_throttled_sample_resent_after_resubscribe() {
    let hr = SignalId::HR.as_u16();
    for how in ["unsubscribe_all", "unsubscribe", "on_disconnect"] {
        let mut s = BleSessionState::new(1);
        s.subscribe_with_period(hr, Some(1), 3_600_000);
        assert!(s.add_data(hr, 70.0, 1_000_000).is_some()); // never ACKed
        match how {
            "unsubscribe_all" => s.unsubscribe_all(),
            "unsubscribe" => s.unsubscribe(hr),
            _ => s.on_disconnect(),
        }
        s.subscribe_with_period(hr, Some(1), 3_600_000);
        assert!(
            s.add_data(hr, 71.0, 1_000_000 + 600_000).is_some(),
            "unACKed before {}: must be sent immediately after re-subscribe",
            how
        );
    }
}

/// Helper: HR stream throttled to 1 h with one sample at t0=1_000_000 ACKed.
fn hr_hourly_delivered() -> BleSessionState {
    let hr = SignalId::HR.as_u16();
    let mut s = BleSessionState::new(1);
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(s.add_data(hr, 70.0, 1_000_000).is_some());
    let session = s.current_session_id;
    s.handle_ack(session, 1, 1);
    s
}

/// ID SRS: SRS-TEST-BLESESSION-056
/// Title: a new-session ACK clearing tx_buffer is not a delivery
///
/// Description: handle_ack / handle_ack_with_bitmap with a different session_id clear
///              tx_buffer without confirming anything. VRConnect shall not treat that as a
///              delivery: after re-subscribe the undelivered value goes out immediately.
///
/// Version: V1.0
#[test]
fn test_new_session_ack_is_not_a_delivery() {
    let hr = SignalId::HR.as_u16();
    for with_bitmap in [false, true] {
        let mut s = BleSessionState::new(1);
        s.subscribe_with_period(hr, Some(1), 3_600_000);
        assert!(s.add_data(hr, 70.0, 1_000_000).is_some());
        let stale = s.current_session_id.wrapping_add(7);
        if with_bitmap {
            s.handle_ack_with_bitmap(stale, 1, 1, &[0u8; 8]);
        } else {
            s.handle_ack(stale, 1, 1);
        }
        s.unsubscribe_all();
        s.subscribe_with_period(hr, Some(1), 3_600_000);
        assert!(
            s.add_data(hr, 71.0, 1_000_000 + 600_000).is_some(),
            "bitmap={}: never confirmed → must be re-sent at once",
            with_bitmap
        );
    }
}

/// ID SRS: SRS-TEST-BLESESSION-057
/// Title: a backward source-clock jump after re-subscribe does not silence the stream
///
/// Description: If VitalRecorder's timeline goes backwards (restart / clock correction),
///              the carried-over cadence would otherwise throttle until the old timeline is
///              caught up (hours). VRConnect shall treat t0 < last delivery as a reset:
///              send, then throttle normally from the new timeline.
///
/// Version: V1.0
#[test]
fn test_backward_clock_after_resubscribe_sends_and_reseeds() {
    let hr = SignalId::HR.as_u16();
    let delivered = 10_000_000;
    let mut s = BleSessionState::new(1);
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(s.add_data(hr, 70.0, delivered).is_some());
    let session = s.current_session_id;
    s.handle_ack(session, 1, 1);
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    let back = delivered - 7_200_000; // clock jumped back 2 h (> one period)
    assert!(
        s.add_data(hr, 71.0, back).is_some(),
        "clock went back by more than a period: send"
    );
    assert!(
        s.add_data(hr, 72.0, back + 60_000).is_none(),
        "then throttle on the new timeline"
    );
}

/// ID SRS: SRS-TEST-BLESESSION-060
/// Title: sliding-window re-sends right after re-subscribe are throttled, not a clock reset
///
/// Description: A new stream has no dedup history, so VitalRecorder's sliding window can
///              re-send samples slightly older than the last delivery. VRConnect shall
///              throttle them (no out-of-order sample to the app, no drift of the
///              cadence): the next sample goes out exactly one period after the delivery.
///
/// Version: V1.0
#[test]
fn test_window_resend_after_resubscribe_is_throttled() {
    let hr = SignalId::HR.as_u16();
    let mut s = hr_hourly_delivered(); // delivered at 1_000_000
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    for t0 in [997_000, 999_000, 1_000_000, 1_002_000] {
        assert!(
            s.add_data(hr, 71.0, t0).is_none(),
            "window re-send t0={}",
            t0
        );
    }
    assert!(s.add_data(hr, 72.0, 1_000_000 + 3_599_000).is_none());
    assert!(s.add_data(hr, 73.0, 1_000_000 + 3_600_000).is_some());
}

/// ID SRS: SRS-TEST-BLESESSION-061
/// Title: an ACK beyond any issued seq does not record a delivery
///
/// Description: After a re-subscribe the new stream restarts at seq 1 on the same
///              stream_id; a late ACK from the old numbering (ack_upto=50) must not mark the
///              new, unconfirmed seq 1 as delivered — after the next re-subscribe it is
///              re-sent immediately.
///
/// Version: V1.0
#[test]
fn test_stale_ack_beyond_issued_seq_is_not_a_delivery() {
    let hr = SignalId::HR.as_u16();
    let mut s = BleSessionState::new(1);
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(s.add_data(hr, 70.0, 1_000_000).is_some()); // seq 1, unconfirmed
    let session = s.current_session_id;
    s.handle_ack_with_bitmap(session, 1, 50, &[0u8; 8]); // stale, old numbering
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(
        s.add_data(hr, 71.0, 1_000_000 + 60_000).is_some(),
        "never really confirmed → re-sent at once"
    );
}

/// ID SRS: SRS-TEST-BLESESSION-062
/// Title: a new-session reset falls back to the confirmed cadence within a live stream
///
/// Description: When a new-session ACK (or reset_session) clears tx_buffer, the cleared
///              frames were never confirmed; the live stream's gate shall fall back to the
///              last confirmed delivery instead of the last emission, so an unconfirmed
///              value is re-sent without waiting a full period.
///
/// Version: V1.0
#[test]
fn test_new_session_reset_falls_back_to_confirmed_cadence() {
    let hr = SignalId::HR.as_u16();
    for how in 0..3 {
        let mut s = BleSessionState::new(1);
        s.subscribe_with_period(hr, Some(1), 3_600_000);
        assert!(s.add_data(hr, 70.0, 1_000_000).is_some()); // unconfirmed
        let other = s.current_session_id.wrapping_add(3);
        match how {
            0 => s.handle_ack(other, 1, 1),
            1 => {
                s.handle_ack_with_bitmap(other, 1, 1, &[0u8; 8]);
            }
            _ => s.reset_session(other),
        }
        assert!(
            s.add_data(hr, 71.0, 1_000_000 + 60_000).is_some(),
            "reset path {}: unconfirmed value must be re-sent",
            how
        );
    }
}

/// ID SRS: SRS-TEST-BLESESSION-058
/// Title: unthrottled deliveries keep the cadence reference current
///
/// Description: 1 h throttled, then a period of unthrottled (period_ms=0) delivered
///              samples, then 1 h again: the next hourly sample must be measured from the
///              last delivered sample, not from the older throttled one.
///
/// Version: V1.0
#[test]
fn test_unthrottled_deliveries_update_cadence() {
    let hr = SignalId::HR.as_u16();
    let mut s = hr_hourly_delivered(); // delivered at 1_000_000
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 0);
    let t40 = 1_000_000 + 40 * 60_000;
    assert!(s.add_data(hr, 71.0, t40).is_some());
    let session = s.current_session_id;
    s.handle_ack(session, 1, 1);

    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(
        s.add_data(hr, 72.0, 1_000_000 + 60 * 60_000).is_none(),
        "only 20 min after the last delivery: throttled"
    );
    assert!(s.add_data(hr, 73.0, t40 + 3_600_000).is_some());

    // Same stream switched from 0 to 1 h in place (idempotent re-subscribe): the gate
    // must start from the last unthrottled emission.
    let mut s = BleSessionState::new(1);
    s.subscribe_with_period(hr, Some(1), 0);
    assert!(s.add_data(hr, 70.0, 1_000_000).is_some());
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(
        s.add_data(hr, 71.0, 1_000_000 + 600_000).is_none(),
        "in-place switch to 1 h: measured from the last unthrottled emission"
    );
}

/// ID SRS: SRS-TEST-BLESESSION-059
/// Title: pending backlog frames do not move the reference; ACKed ones only raise it
///
/// Description: Pending replay (FLAG_BACKLOG) frames are not deliveries: a re-subscribe keeps
///              the live cadence. Once ACKed they raise the reference via max (never lower
///              it), so the next catch-up does not resend them.
///
/// Version: V1.0
#[test]
fn test_pending_replay_frames_do_not_affect_cadence() {
    let hr = SignalId::HR.as_u16();
    let mut s = hr_hourly_delivered();
    s.record_history(hr, 60.0, 2_000_000);
    s.record_history(hr, 61.0, 3_000_000);
    let replay = s.start_replay(hr, 0, 0);
    assert!(!replay.is_empty());
    assert!(s.total_pending() > 0, "replay frames pending");
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(
        s.add_data(hr, 73.0, 1_000_000 + 300_000).is_none(),
        "pending replay frames must not reset the live cadence"
    );

    // Even once ACKed, replay frames (t0 2_000_000 / 3_000_000) must not move the live
    // reference: otherwise 1_600_000 would look like a backward clock jump and be sent.
    let mut s = hr_hourly_delivered();
    s.record_history(hr, 60.0, 2_000_000);
    s.record_history(hr, 61.0, 3_000_000);
    let replay = s.start_replay(hr, 0, 0);
    let last_seq = replay.last().unwrap().header.seq;
    let session = s.current_session_id;
    s.handle_ack(session, 1, last_seq);
    assert_eq!(s.last_sent_by_signal[&hr], 3_000_000);
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 3_600_000);
    assert!(
        s.add_data(hr, 74.0, 1_000_000 + 600_000).is_none(),
        "live cadence kept despite pending replay frames"
    );
}

/// ID SRS: SRS-TEST-BLESESSION-053
/// Title: oldest_pending_per_stream returns one FLAG_RETRANSMIT frame per busy stream
///
/// Description: VRConnect shall return exactly the oldest unACKed frame of each stream
///              with pending frames (none for idle streams), with FLAG_RETRANSMIT set,
///              and shall leave tx_buffer untouched.
///
/// Version: V1.0
#[test]
fn test_oldest_pending_per_stream() {
    let mut session = BleSessionState::new(1);
    let hr = SignalId::HR.as_u16();
    let spo2 = SignalId::SpO2.as_u16();
    session.subscribe(hr);
    session.subscribe(spo2);
    session.add_data(hr, 70.0, 1000);
    session.add_data(hr, 71.0, 2000);
    session.add_data(hr, 72.0, 3000);

    let frames = session.oldest_pending_per_stream();
    assert_eq!(frames.len(), 1, "idle SpO2 stream contributes nothing");
    assert_eq!(frames[0].header.seq, 1, "oldest pending frame");
    assert_ne!(frames[0].header.flags & FLAG_RETRANSMIT, 0);
    assert_eq!(session.total_pending(), 3, "buffer must not be modified");
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

    let frames = session.start_replay(SignalId::HR.as_u16(), 0, 0);
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

// ── Hourly catch-up replay (downsampled backlog) ──────────────────────────

const CU_BASE: u64 = 1_000_000;
const CU_HOUR: u32 = 3_600_000;

/// Helper: 3 h of 1 Hz HR history starting at CU_BASE, last confirmed delivery = CU_BASE,
/// re-subscribed hourly. Returns the session and the catch-up frames.
fn cu_session_with_catchup() -> (BleSessionState, Vec<DataFrame>) {
    let hr = SignalId::HR.as_u16();
    let mut s = BleSessionState::new(1);
    for i in 0..=10_800u64 {
        s.record_history(hr, 60.0 + (i % 10) as f32, CU_BASE + i * 1000);
    }
    s.last_sent_by_signal.insert(hr, CU_BASE);
    s.subscribe_with_period(hr, Some(1), CU_HOUR);
    let frames = s.start_replay(hr, CU_BASE, CU_HOUR);
    (s, frames)
}

/// ID SRS: SRS-TEST-BLESESSION-063
/// Title: catch-up replay keeps one sample per missed period
///
/// Description: 3 h of 1 Hz history, reference = start, period 1 h → exactly 3 FLAG_BACKLOG
///              frames at +1 h, +2 h, +3 h with consecutive seqs; period 0 = full resolution.
///
/// Version: V1.0
#[test]
fn test_catchup_replay_downsamples_to_one_per_period() {
    let (s, frames) = cu_session_with_catchup();
    let t0s: Vec<u64> = frames.iter().map(|f| f.t0_ms).collect();
    let h = CU_HOUR as u64;
    assert_eq!(t0s, vec![CU_BASE + h, CU_BASE + 2 * h, CU_BASE + 3 * h]);
    assert!(frames.iter().all(|f| f.header.flags & FLAG_BACKLOG != 0));
    let seqs: Vec<u32> = frames.iter().map(|f| f.header.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    assert_eq!(s.streams.get(&1).unwrap().last_seq, 3, "seq block reserved");

    let hr = SignalId::HR.as_u16();
    let full = s.get_replay_frames(hr, CU_BASE, 0, 1, 1, 1);
    assert_eq!(full.len(), 10_801, "min_gap 0 keeps full resolution");
}

/// ID SRS: SRS-TEST-BLESESSION-064
/// Title: no delivery reference means no catch-up gate input
///
/// Description: Without a reference the caller (handle_tlv_subscribe) must not replay; the
///              session exposes this as an absent last_sent_by_signal entry, so a fresh
///              subscription starts live-only with an empty gate.
///
/// Version: V1.0
#[test]
fn test_catchup_without_reference_is_not_requested() {
    let hr = SignalId::HR.as_u16();
    let mut s = BleSessionState::new(1);
    for i in 0..100u64 {
        s.record_history(hr, 70.0, CU_BASE + i * 1000);
    }
    s.subscribe_with_period(hr, Some(1), CU_HOUR);
    assert!(!s.last_sent_by_signal.contains_key(&hr));
    assert!(s.streams.get(&1).unwrap().last_sent_t0_ms.is_none());
    assert_eq!(s.total_pending(), 0, "nothing queued without a reference");
}

/// ID SRS: SRS-TEST-BLESESSION-065
/// Title: ACKed catch-up frames advance the reference; no second catch-up
///
/// Description: After the catch-up frames are cumulatively ACKed, the reference moves to
///              the last replayed t0 (backlog ACKs only raise it) and a re-subscribe
///              produces 0 catch-up frames. Unacked catch-up would be resent.
///
/// Version: V1.0
#[test]
fn test_catchup_acked_not_resent_on_next_resubscribe() {
    let hr = SignalId::HR.as_u16();
    let h = CU_HOUR as u64;

    // Unacked: next re-subscribe repeats the catch-up.
    let (mut s, _) = cu_session_with_catchup();
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), CU_HOUR);
    assert_eq!(s.start_replay(hr, CU_BASE, CU_HOUR).len(), 3);

    // Acked: reference advances, nothing left to catch up.
    let (mut s, _) = cu_session_with_catchup();
    let session = s.current_session_id;
    s.handle_ack(session, 1, 3);
    assert_eq!(s.last_sent_by_signal.get(&hr), Some(&(CU_BASE + 3 * h)));
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), CU_HOUR);
    let since = s.last_sent_by_signal[&hr];
    assert!(s.start_replay(hr, since, CU_HOUR).is_empty());
}

/// ID SRS: SRS-TEST-BLESESSION-066
/// Title: live gate continues from the last replayed sample
///
/// Description: After catch-up the live throttle measures the period from the last replayed
///              t0, so the sample being emitted live is not a duplicate of the backlog.
///
/// Version: V1.0
#[test]
fn test_catchup_live_gate_continues_from_last_replayed() {
    let hr = SignalId::HR.as_u16();
    let (mut s, frames) = cu_session_with_catchup();
    let last = frames.last().unwrap().t0_ms;
    assert_eq!(s.streams.get(&1).unwrap().last_sent_t0_ms, Some(last));
    assert!(s.add_data(hr, 70.0, last + 1000).is_none(), "same period");
    assert!(s.add_data(hr, 71.0, last + 600_000).is_none());
    let live = s.add_data(hr, 72.0, last + CU_HOUR as u64).unwrap();
    assert_eq!(live.header.flags & FLAG_BACKLOG, 0);
    assert_eq!(live.header.seq, 4, "live seq follows the reserved block");
}

/// ID SRS: SRS-TEST-BLESESSION-067
/// Title: catch-up is capped to the most recent frames with contiguous seqs
///
/// Description: 2 h of 1 Hz history at a 60 s period would give 120 frames; only the newest
///              24 are returned, numbered seq_start.. contiguously.
///
/// Version: V1.0
#[test]
fn test_catchup_capped_to_most_recent_frames() {
    let hr = SignalId::HR.as_u16();
    let mut s = BleSessionState::new(1);
    for i in 0..=7_200u64 {
        s.record_history(hr, 60.0, CU_BASE + i * 1000);
    }
    let f = s.get_replay_frames(hr, CU_BASE, 60_000, 1, 1, 5);
    assert_eq!(f.len(), 24);
    assert_eq!(f.last().unwrap().t0_ms, CU_BASE + 7_200_000);
    assert_eq!(f[0].header.seq, 5);
    assert_eq!(f[23].header.seq, 28);
}

/// ID SRS: SRS-TEST-BLESESSION-068
/// Title: second subscribe in the same session resends no catch-up
///
/// Description: SUB1 produces 3 catch-up frames (unACKed); SUB2 (unsubscribe_all +
///              re-register, prior = stream's last_sent_t0_ms) produces 0 frames and no
///              duplicate t0. A real reconnect (on_disconnect, no prior) resends from the
///              confirmed reference. No reference at all -> None.
///
/// Version: V1.0
#[test]
fn test_catchup_second_subscribe_same_session_is_empty() {
    let hr = SignalId::HR.as_u16();
    let (mut s, frames) = cu_session_with_catchup();
    assert_eq!(frames.len(), 3);
    let prior = s.backlog_emitted().get(&hr).copied();
    assert_eq!(prior, Some(CU_BASE + 3 * CU_HOUR as u64));
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), CU_HOUR);
    let (_, f2) = s.catchup_replay(hr, CU_HOUR, prior, 60_000).unwrap();
    assert!(f2.is_empty(), "no duplicate catch-up in the same session");

    // An unACKed LIVE frame is not a catch-up emission: after a same-session re-subscribe
    // it is re-sent at once (gate seeded from the confirmed reference only).
    let (mut s2, _) = cu_session_with_catchup();
    let live_t0 = CU_BASE + 3 * CU_HOUR as u64 + CU_HOUR as u64;
    assert!(s2.add_data(hr, 70.0, live_t0).is_some());
    let prior2 = s2.backlog_emitted().get(&hr).copied();
    s2.unsubscribe_all();
    s2.subscribe_with_period(hr, Some(1), CU_HOUR);
    s2.catchup_replay(hr, CU_HOUR, prior2, 60_000).unwrap();
    assert!(
        s2.add_data(hr, 71.0, live_t0).is_some(),
        "unconfirmed live value re-sent immediately"
    );

    // Real reconnect: streams gone, only the confirmed reference remains.
    s.on_disconnect();
    s.subscribe_with_period(hr, Some(1), CU_HOUR);
    let (_, f3) = s.catchup_replay(hr, CU_HOUR, None, 60_000).unwrap();
    assert_eq!(f3.len(), 3);

    // Too-short period: nothing replayed. No reference: None.
    s.unsubscribe_all();
    s.subscribe_with_period(hr, Some(1), 1000);
    assert!(s
        .catchup_replay(hr, 1000, None, 60_000)
        .unwrap()
        .1
        .is_empty());
    let mut fresh = BleSessionState::new(1);
    fresh.subscribe_with_period(hr, Some(1), CU_HOUR);
    assert!(fresh.catchup_replay(hr, CU_HOUR, None, 60_000).is_none());
}
