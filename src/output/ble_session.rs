// /src/output/ble_session.rs
// Module: output.ble_session
// Purpose: Sliding Window Engine for reliable BLE protocol
//          Isolated from Bluetooth radio, making it safe and testable
//
// The Sliding Window Engine (State Management)

use crate::domain::ble_protocol::{AckFrame, DataFrame, SignalId};
use std::collections::{HashSet, VecDeque};

/// ID SRS: SRS-MOD-BLESESSION-001
/// Title: BleSessionState
///
/// Description: VRConnect shall maintain session state for reliable BLE communication
/// including sequence tracking, transmission buffer management, and subscription handling.
///
/// Version: V1.0
pub struct BleSessionState {
    /// Current session identifier - resets on new session
    pub current_session_id: u16,
    /// Current stream identifier (maps to SignalId)
    pub current_stream_id: u16,
    /// Last sequence number sent
    pub last_seq: u32,
    /// Transmission buffer for sliding window and retransmission
    pub tx_buffer: VecDeque<DataFrame>,
    /// Subscribed signal IDs (e.g., 1=HR, 2=SpO2, 3=Temperature)
    pub subscriptions: HashSet<u16>,
    /// Maximum buffer size to prevent unbounded growth (medical safety)
    pub max_buffer_size: usize,
}

impl BleSessionState {
    /// ID SRS: SRS-FN-BLESESSION-001
    /// Title: new
    ///
    /// Description: VRConnect shall create a new BleSessionState with the given session ID
    /// and default configuration.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `session_id` - Initial session identifier
    ///
    /// # Returns
    /// New BleSessionState instance
    pub fn new(session_id: u16) -> Self {
        Self {
            current_session_id: session_id,
            current_stream_id: 1,
            last_seq: 0,
            tx_buffer: VecDeque::new(),
            subscriptions: HashSet::new(),
            max_buffer_size: 1000, // Medical requirement: prevent unbounded growth
        }
    }

    /// ID SRS: SRS-FN-BLESESSION-002
    /// Title: with_buffer_size
    ///
    /// Description: VRConnect shall allow configuring the maximum buffer size.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `size` - Maximum number of frames to retain in transmission buffer
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
    /// Description: VRConnect shall subscribe a signal ID for transmission.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - Signal ID to subscribe (1=HR, 2=SpO2, 3=Temperature)
    pub fn subscribe(&mut self, signal_id: u16) {
        self.subscriptions.insert(signal_id);
    }

    /// ID SRS: SRS-FN-BLESESSION-004
    /// Title: unsubscribe
    ///
    /// Description: VRConnect shall unsubscribe a signal ID from transmission.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - Signal ID to unsubscribe
    pub fn unsubscribe(&mut self, signal_id: u16) {
        self.subscriptions.remove(&signal_id);
    }

    /// ID SRS: SRS-FN-BLESESSION-005
    /// Title: is_subscribed
    ///
    /// Description: VRConnect shall check if a signal ID is subscribed.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - Signal ID to check
    ///
    /// # Returns
    /// true if subscribed, false otherwise
    pub fn is_subscribed(&self, signal_id: u16) -> bool {
        self.subscriptions.contains(&signal_id)
    }

    /// ID SRS: SRS-FN-BLESESSION-006
    /// Title: handle_ack
    ///
    /// Description: VRConnect shall process acknowledgment frames according to protocol:
    /// - If new session detected: reset state (clear buffer, reset sequence)
    /// - Remove acknowledged frames from tx_buffer (seq_num <= ack_base)
    /// - Check bitmap and retransmit missing frames (bit=0 means missing)
    ///
    /// PDF Logic: "Si nouvelle session détectée : reset ackWindow, stats, lastSeq"
    /// PDF Logic: "stream.txBuffer.removeWhere((seq, _) => seq <= ackBase)"
    /// PDF Logic: "Vérifie bitmap -> Retransmet trames manquantes"
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `ack` - Acknowledgment frame from receiver
    ///
    /// # Returns
    /// Vec of DataFrame to retransmit
    pub fn handle_ack(&mut self, ack: &AckFrame) -> Vec<DataFrame> {
        // Check for new session
        if ack.session_id != self.current_session_id {
            // PDF: "Si nouvelle session détectée : reset ackWindow, stats, lastSeq"
            self.current_session_id = ack.session_id;
            self.tx_buffer.clear();
            self.last_seq = 0;
            return vec![]; // Nothing to retransmit on new session
        }

        // PDF: "stream.txBuffer.removeWhere((seq, _) => seq <= ackBase)"
        // Remove acknowledged frames (seq_num <= ack_base are confirmed received)
        self.tx_buffer.retain(|frame| frame.seq_num > ack.ack_base);

        // PDF: "Vérifie bitmap -> Retransmet trames manquantes"
        // Bitmap: bit=1 means received OK, bit=0 means missing (need retransmission)
        let mut to_retransmit = Vec::new();

        for i in 0..(ack.bitmap_len as usize * 8) {
            let byte_idx = i / 8;
            let bit_idx = i % 8;

            // Bounds check
            if byte_idx >= ack.bitmap.len() {
                break;
            }

            // Check if bit is 0 (0 means missing/retransmission requested)
            let bit_set = (ack.bitmap[byte_idx] >> bit_idx) & 1 == 1;

            if !bit_set {
                // This sequence number is missing, needs retransmission
                let missing_seq = ack.ack_base + 1 + i as u32;

                // Find it in our buffer
                if let Some(frame) = self.tx_buffer.iter().find(|f| f.seq_num == missing_seq) {
                    to_retransmit.push(frame.clone());
                }
            }
        }

        to_retransmit
    }

    /// ID SRS: SRS-FN-BLESESSION-007
    /// Title: add_data
    ///
    /// Description: VRConnect shall add data to the session if subscribed,
    /// creating a new DataFrame with the next sequence number.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal_id` - Signal ID (1=HR, 2=SpO2, 3=Temperature)
    /// * `value` - Float value to encode
    ///
    /// # Returns
    /// Some(DataFrame) if subscribed, None otherwise
    pub fn add_data(&mut self, signal_id: u16, value: f32) -> Option<DataFrame> {
        // Only add data if subscribed to this signal
        if !self.subscriptions.contains(&signal_id) {
            return None;
        }

        // Increment sequence number
        self.last_seq += 1;

        // Encode value as payload (4 bytes, little-endian f32)
        let payload = value.to_le_bytes().to_vec();

        // Create the DataFrame
        let frame = DataFrame {
            session_id: self.current_session_id,
            stream_id: self.current_stream_id,
            seq_num: self.last_seq,
            payload,
        };

        // Add to transmission buffer for potential retransmission
        self.tx_buffer.push_back(frame.clone());

        // Safety: Don't let buffer grow infinitely (Medical requirement)
        while self.tx_buffer.len() > self.max_buffer_size {
            self.tx_buffer.pop_front();
        }

        Some(frame)
    }

    /// ID SRS: SRS-FN-BLESESSION-008
    /// Title: add_data_with_signal
    ///
    /// Description: VRConnect shall add data using SignalId enum for type safety.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `signal` - SignalId enum (HR, SpO2, Temperature)
    /// * `value` - Float value to encode
    ///
    /// # Returns
    /// Some(DataFrame) if subscribed, None otherwise
    pub fn add_data_with_signal(&mut self, signal: SignalId, value: f32) -> Option<DataFrame> {
        self.add_data(signal.as_u16(), value)
    }

    /// ID SRS: SRS-FN-BLESESSION-009
    /// Title: get_pending_count
    ///
    /// Description: VRConnect shall return the number of pending frames in buffer.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Number of frames awaiting acknowledgment
    pub fn get_pending_count(&self) -> usize {
        self.tx_buffer.len()
    }

    /// ID SRS: SRS-FN-BLESESSION-010
    /// Title: clear_buffer
    ///
    /// Description: VRConnect shall clear the transmission buffer.
    ///
    /// Version: V1.0
    pub fn clear_buffer(&mut self) {
        self.tx_buffer.clear();
    }

    /// ID SRS: SRS-FN-BLESESSION-011
    /// Title: reset_session
    ///
    /// Description: VRConnect shall reset the session with a new session ID,
    /// clearing all state.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `new_session_id` - New session identifier
    pub fn reset_session(&mut self, new_session_id: u16) {
        self.current_session_id = new_session_id;
        self.last_seq = 0;
        self.tx_buffer.clear();
    }

    /// ID SRS: SRS-FN-BLESESSION-012
    /// Title: get_frame_by_seq
    ///
    /// Description: VRConnect shall retrieve a specific frame by sequence number
    /// from the transmission buffer.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `seq_num` - Sequence number to find
    ///
    /// # Returns
    /// Some(&DataFrame) if found, None otherwise
    pub fn get_frame_by_seq(&self, seq_num: u32) -> Option<&DataFrame> {
        self.tx_buffer.iter().find(|f| f.seq_num == seq_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ID SRS: SRS-TEST-BLESESSION-001
    /// Title: Test session creation
    ///
    /// Description: VRConnect shall create BleSessionState with correct initial values.
    ///
    /// Version: V1.0
    #[test]
    fn test_session_creation() {
        let session = BleSessionState::new(42);

        assert_eq!(session.current_session_id, 42);
        assert_eq!(session.current_stream_id, 1);
        assert_eq!(session.last_seq, 0);
        assert!(session.tx_buffer.is_empty());
        assert!(session.subscriptions.is_empty());
        assert_eq!(session.max_buffer_size, 1000);
    }

    /// ID SRS: SRS-TEST-BLESESSION-002
    /// Title: Test subscription management
    ///
    /// Description: VRConnect shall correctly manage signal subscriptions.
    ///
    /// Version: V1.0
    #[test]
    fn test_subscription_management() {
        let mut session = BleSessionState::new(1);

        // Subscribe to HR
        session.subscribe(SignalId::HR.as_u16());
        assert!(session.is_subscribed(SignalId::HR.as_u16()));
        assert!(!session.is_subscribed(SignalId::SpO2.as_u16()));

        // Subscribe to SpO2
        session.subscribe(SignalId::SpO2.as_u16());
        assert!(session.is_subscribed(SignalId::SpO2.as_u16()));

        // Unsubscribe HR
        session.unsubscribe(SignalId::HR.as_u16());
        assert!(!session.is_subscribed(SignalId::HR.as_u16()));

        // Test with SignalId enum directly
        session.subscribe(SignalId::Temperature.as_u16());
        assert!(session.is_subscribed(3)); // Temperature = 3
    }

    /// ID SRS: SRS-TEST-BLESESSION-003
    /// Title: Test add_data with subscription
    ///
    /// Description: VRConnect shall only add data for subscribed signals.
    ///
    /// Version: V1.0
    #[test]
    fn test_add_data_with_subscription() {
        let mut session = BleSessionState::new(1);

        // Not subscribed - should return None
        assert!(session.add_data(SignalId::HR.as_u16(), 75.0).is_none());

        // Subscribe and add
        session.subscribe(SignalId::HR.as_u16());
        let frame = session.add_data(SignalId::HR.as_u16(), 75.0);

        assert!(frame.is_some());
        let frame = frame.unwrap();
        assert_eq!(frame.session_id, 1);
        assert_eq!(frame.stream_id, 1);
        assert_eq!(frame.seq_num, 1);

        // Verify payload encoding (f32 little-endian)
        let expected_payload = 75.0f32.to_le_bytes();
        assert_eq!(frame.payload, expected_payload.to_vec());

        // Check buffer size
        assert_eq!(session.get_pending_count(), 1);
    }

    /// ID SRS: SRS-TEST-BLESESSION-004
    /// Title: Test sequence number incrementing
    ///
    /// Description: VRConnect shall increment sequence numbers correctly.
    ///
    /// Version: V1.0
    #[test]
    fn test_sequence_incrementing() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        let frame1 = session.add_data(SignalId::HR.as_u16(), 75.0).unwrap();
        let frame2 = session.add_data(SignalId::HR.as_u16(), 76.0).unwrap();
        let frame3 = session.add_data(SignalId::HR.as_u16(), 77.0).unwrap();

        assert_eq!(frame1.seq_num, 1);
        assert_eq!(frame2.seq_num, 2);
        assert_eq!(frame3.seq_num, 3);
        assert_eq!(session.last_seq, 3);
    }

    /// ID SRS: SRS-TEST-BLESESSION-005
    /// Title: Test buffer size limit
    ///
    /// Description: VRConnect shall enforce maximum buffer size limit.
    ///
    /// Version: V1.0
    #[test]
    fn test_buffer_size_limit() {
        let mut session = BleSessionState::new(1).with_buffer_size(5);
        session.subscribe(SignalId::HR.as_u16());

        // Add 10 frames
        for i in 0..10 {
            session.add_data(SignalId::HR.as_u16(), i as f32);
        }

        // Buffer should only contain last 5 frames (seq 6-10)
        assert_eq!(session.get_pending_count(), 5);

        // Oldest frame should be seq 6
        let oldest = session.tx_buffer.front().unwrap();
        assert_eq!(oldest.seq_num, 6);

        // Newest frame should be seq 10
        let newest = session.tx_buffer.back().unwrap();
        assert_eq!(newest.seq_num, 10);
    }

    /// ID SRS: SRS-TEST-BLESESSION-006
    /// Title: Test handle_ack with new session
    ///
    /// Description: VRConnect shall reset state when new session is detected.
    ///
    /// Version: V1.0
    #[test]
    fn test_handle_ack_new_session() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        // Add some data
        session.add_data(SignalId::HR.as_u16(), 75.0);
        session.add_data(SignalId::HR.as_u16(), 76.0);
        assert_eq!(session.get_pending_count(), 2);
        assert_eq!(session.last_seq, 2);

        // Receive ACK for new session
        let ack = AckFrame::new(2, 1, 0, vec![]);
        let retransmit = session.handle_ack(&ack);

        // State should be reset
        assert_eq!(session.current_session_id, 2);
        assert_eq!(session.get_pending_count(), 0);
        assert_eq!(session.last_seq, 0);
        assert!(retransmit.is_empty());
    }

    /// ID SRS: SRS-TEST-BLESESSION-007
    /// Title: Test handle_ack removes acknowledged frames
    ///
    /// Description: VRConnect shall remove frames with seq <= ack_base from buffer.
    ///
    /// Version: V1.0
    #[test]
    fn test_handle_ack_removes_acked_frames() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        // Add 5 frames (seq 1-5)
        for i in 1..=5 {
            session.add_data(SignalId::HR.as_u16(), i as f32);
        }
        assert_eq!(session.get_pending_count(), 5);

        // ACK with ack_base=3 and empty bitmap (all up to 3 are acked)
        let ack = AckFrame::new(1, 1, 3, vec![]);
        let retransmit = session.handle_ack(&ack);

        // Frames 1-3 should be removed, 4-5 remain
        assert_eq!(session.get_pending_count(), 2);
        assert!(retransmit.is_empty());

        // Verify remaining frames
        let seqs: Vec<u32> = session.tx_buffer.iter().map(|f| f.seq_num).collect();
        assert_eq!(seqs, vec![4, 5]);
    }

    /// ID SRS: SRS-TEST-BLESESSION-008
    /// Title: Test handle_ack retransmission
    ///
    /// Description: VRConnect shall retransmit frames where bitmap bit is 0.
    ///
    /// Version: V1.0
    #[test]
    fn test_handle_ack_retransmission() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        // Add 5 frames (seq 1-5)
        for i in 1..=5 {
            session.add_data(SignalId::HR.as_u16(), i as f32);
        }

        // ACK with ack_base=1 and bitmap where bits 1 and 3 are 0 (seq 2 and 4 missing)
        // Bitmap: bit 0 = seq 2, bit 1 = seq 3, bit 2 = seq 4, bit 3 = seq 5
        // We want bits 1 and 3 to be 0 -> bitmap byte = 0b00000101 = 5
        let ack = AckFrame::new(1, 1, 1, vec![0b00001010]); // bits 1 and 3 are 1, wait no...
                                                            // Let me reconsider: bit 0 set = seq 2 received, bit 1 set = seq 3 received
                                                            // We want seq 2 (bit 0) = 0 and seq 4 (bit 2) = 0
                                                            // bitmap = 0b00001010 = seq 3 and seq 5 received (bits 1 and 3 set)
                                                            // Actually, let's be clearer:
                                                            // - bit 0 (0x01) = seq 2 received OK
                                                            // - bit 1 (0x02) = seq 3 received OK
                                                            // - bit 2 (0x04) = seq 4 received OK
                                                            // - bit 3 (0x08) = seq 5 received OK
                                                            // To mark seq 2 and 4 as missing, we set bits 1 and 3: 0b00001010 = 10
                                                            // Wait, that's wrong. If bit is 1 = received OK, 0 = missing
                                                            // So to have seq 2 (bit 0) and seq 4 (bit 2) MISSING:
                                                            // bits 1 and 3 should be 1: 0b00001010
        let ack = AckFrame::new(1, 1, 1, vec![0b00001010]);

        let retransmit = session.handle_ack(&ack);

        // Should retransmit seq 2 and 4
        assert_eq!(retransmit.len(), 2);
        assert_eq!(retransmit[0].seq_num, 2);
        assert_eq!(retransmit[1].seq_num, 4);

        // Buffer should still have 4 and 5 (2 was retransmitted but not removed)
        // Actually, frames are only removed by ack_base, not by retransmission
        assert_eq!(session.get_pending_count(), 4); // 2, 3, 4, 5 remain (seq 1 removed)
    }

    /// ID SRS: SRS-TEST-BLESESSION-009
    /// Title: Test session reset
    ///
    /// Description: VRConnect shall reset session state correctly.
    ///
    /// Version: V1.0
    #[test]
    fn test_session_reset() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        session.add_data(SignalId::HR.as_u16(), 75.0);
        session.add_data(SignalId::HR.as_u16(), 76.0);

        assert_eq!(session.get_pending_count(), 2);
        assert_eq!(session.last_seq, 2);

        session.reset_session(42);

        assert_eq!(session.current_session_id, 42);
        assert_eq!(session.last_seq, 0);
        assert_eq!(session.get_pending_count(), 0);
        // Subscriptions should be preserved
        assert!(session.is_subscribed(SignalId::HR.as_u16()));
    }

    /// ID SRS: SRS-TEST-BLESESSION-010
    /// Title: Test get_frame_by_seq
    ///
    /// Description: VRConnect shall retrieve frames by sequence number.
    ///
    /// Version: V1.0
    #[test]
    fn test_get_frame_by_seq() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::HR.as_u16());

        session.add_data(SignalId::HR.as_u16(), 75.0);
        session.add_data(SignalId::HR.as_u16(), 76.0);
        session.add_data(SignalId::HR.as_u16(), 77.0);

        assert!(session.get_frame_by_seq(2).is_some());
        assert_eq!(session.get_frame_by_seq(2).unwrap().seq_num, 2);

        assert!(session.get_frame_by_seq(99).is_none());
    }

    /// ID SRS: SRS-TEST-BLESESSION-011
    /// Title: Test add_data_with_signal
    ///
    /// Description: VRConnect shall support SignalId enum for type-safe data addition.
    ///
    /// Version: V1.0
    #[test]
    fn test_add_data_with_signal_enum() {
        let mut session = BleSessionState::new(1);
        session.subscribe(SignalId::SpO2.as_u16());

        let frame = session.add_data_with_signal(SignalId::SpO2, 98.5).unwrap();

        // Verify the payload
        let value = f32::from_le_bytes([
            frame.payload[0],
            frame.payload[1],
            frame.payload[2],
            frame.payload[3],
        ]);
        assert!((value - 98.5).abs() < f32::EPSILON);
    }
}
