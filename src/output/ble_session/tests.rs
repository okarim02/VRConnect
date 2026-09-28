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

    let replay = session.get_replay_frames(SignalId::HR.as_u16(), 0, 1, 1, 1);
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
