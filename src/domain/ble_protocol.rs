// /src/domain/ble_protocol.rs
// Module: domain.ble_protocol
// Purpose: Binary protocol definitions for BLE communication
//          Strictly defined to avoid parsing errors in medical context
//
// Protocol Format:
// - DataFrame: [SessionID(2b) | StreamID(2b) | SeqNum(4b) | Payload(Nb)]
// - AckFrame:  [SessionID(2b) | StreamID(2b) | AckBase(4b) | BitmapLen(1b) | Bitmap(Nb)]

use serde::{Deserialize, Serialize};

/// ID SRS: SRS-MOD-BLEPROTOCOL-001
/// Title: SignalId
///
/// Description: VRConnect shall define hardcoded Signal IDs as per medical
/// protocol specification for HR, SpO2, and Temperature.
///
/// Version: V1.0
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum SignalId {
    /// Heart Rate (ID: 1)
    HR = 1,
    /// Blood Oxygen Saturation (ID: 2)
    SpO2 = 2,
    /// Body Temperature (ID: 3)
    Temperature = 3,
}

impl SignalId {
    /// Convert a SignalId to its u16 representation
    pub fn as_u16(&self) -> u16 {
        *self as u16
    }

    /// Get the SignalId from a u16 value
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(SignalId::HR),
            2 => Some(SignalId::SpO2),
            3 => Some(SignalId::Temperature),
            _ => None,
        }
    }

    /// Get the signal name for logging/display purposes
    pub fn name(&self) -> &'static str {
        match self {
            SignalId::HR => "HR",
            SignalId::SpO2 => "SpO2",
            SignalId::Temperature => "Temperature",
        }
    }
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-002
/// Title: StreamType
///
/// Description: VRConnect shall define stream types for catalog entries.
///
/// Version: V1.0
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamType {
    /// Numerical value stream (single values)
    Num,
    /// Waveform stream (array of values)
    Wav,
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-003
/// Title: CatalogEntry
///
/// Description: VRConnect shall define a catalog entry describing a signal stream
/// with id, name, type, and period.
///
/// Version: V1.0
///
/// Format: (id, name, type, period)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CatalogEntry {
    /// Signal ID (1=HR, 2=SpO2, 3=Temperature)
    pub id: u16,
    /// Human-readable signal name
    pub name: String,
    /// Stream type (Num or Wav)
    pub stream_type: StreamType,
    /// Sampling period in milliseconds
    pub period_ms: u32,
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-004
/// Title: Catalog
///
/// Description: VRConnect shall define a catalog as a list of catalog entries
/// describing available signal streams.
///
/// Version: V1.0
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Catalog {
    pub entries: Vec<CatalogEntry>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add an entry to the catalog
    pub fn add_entry(&mut self, entry: CatalogEntry) {
        self.entries.push(entry);
    }

    /// Get the default catalog with standard medical signals
    pub fn default_medical_catalog() -> Self {
        let mut catalog = Self::new();

        // HR: Heart Rate, numerical, typical period 1000ms (1Hz)
        catalog.add_entry(CatalogEntry {
            id: SignalId::HR.as_u16(),
            name: "HR".to_string(),
            stream_type: StreamType::Num,
            period_ms: 1000,
        });

        // SpO2: Blood oxygen, numerical, typical period 1000ms (1Hz)
        catalog.add_entry(CatalogEntry {
            id: SignalId::SpO2.as_u16(),
            name: "SpO2".to_string(),
            stream_type: StreamType::Num,
            period_ms: 1000,
        });

        // Temperature: body temperature, numerical, typical period 5000ms (0.2Hz)
        catalog.add_entry(CatalogEntry {
            id: SignalId::Temperature.as_u16(),
            name: "Temperature".to_string(),
            stream_type: StreamType::Num,
            period_ms: 5000,
        });

        catalog
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-005
/// Title: DataFrame
///
/// Description: VRConnect shall define a DataFrame structure for binary transmission.
///
/// Format: [SessionID(2b) | StreamID(2b) | SeqNum(4b) | Payload(Nb)]
///
/// Version: V1.0
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataFrame {
    /// Session identifier (2 bytes)
    pub session_id: u16,
    /// Stream identifier (2 bytes) - maps to SignalId
    pub stream_id: u16,
    /// Sequence number (4 bytes) - monotonically increasing
    pub seq_num: u32,
    /// Payload data (variable length)
    pub payload: Vec<u8>,
}

impl DataFrame {
    /// Create a new DataFrame
    pub fn new(session_id: u16, stream_id: u16, seq_num: u32, payload: Vec<u8>) -> Self {
        Self {
            session_id,
            stream_id,
            seq_num,
            payload,
        }
    }

    /// Serialize the DataFrame to bytes using postcard
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    /// Deserialize a DataFrame from bytes using postcard
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    /// Get the header size (fixed portion) in bytes
    pub const fn header_size() -> usize {
        8 // 2 (session_id) + 2 (stream_id) + 4 (seq_num)
    }
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-006
/// Title: AckFrame
///
/// Description: VRConnect shall define an AckFrame structure for reliable delivery.
///
/// Format: [SessionID(2b) | StreamID(2b) | AckBase(4b) | BitmapLen(1b) | Bitmap(Nb)]
///
/// Version: V1.0
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AckFrame {
    /// Session identifier (2 bytes)
    pub session_id: u16,
    /// Stream identifier (2 bytes)
    pub stream_id: u16,
    /// Acknowledgment base sequence number (4 bytes)
    pub ack_base: u32,
    /// Bitmap length in bytes (1 byte)
    pub bitmap_len: u8,
    /// Acknowledgment bitmap - each bit represents a received packet
    pub bitmap: Vec<u8>,
}

impl AckFrame {
    /// Create a new AckFrame
    pub fn new(session_id: u16, stream_id: u16, ack_base: u32, bitmap: Vec<u8>) -> Self {
        let bitmap_len = bitmap.len() as u8;
        Self {
            session_id,
            stream_id,
            ack_base,
            bitmap_len,
            bitmap,
        }
    }

    /// Serialize the AckFrame to bytes using postcard
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    /// Deserialize an AckFrame from bytes using postcard
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    /// Get the header size (fixed portion) in bytes
    pub const fn header_size() -> usize {
        9 // 2 (session_id) + 2 (stream_id) + 4 (ack_base) + 1 (bitmap_len)
    }

    /// Check if a specific sequence number is acknowledged in the bitmap
    /// seq_num should be >= ack_base
    pub fn is_acked(&self, seq_num: u32) -> bool {
        if seq_num < self.ack_base {
            // Already acknowledged
            return true;
        }

        let offset = (seq_num - self.ack_base) as usize;
        if offset >= self.bitmap.len() * 8 {
            // Beyond the bitmap range
            return false;
        }

        let byte_idx = offset / 8;
        let bit_idx = offset % 8;

        if byte_idx >= self.bitmap.len() {
            return false;
        }

        (self.bitmap[byte_idx] >> bit_idx) & 1 == 1
    }

    /// Build an acknowledgment bitmap from a set of received sequence numbers
    pub fn build_bitmap(ack_base: u32, received_seqs: &[u32]) -> Vec<u8> {
        if received_seqs.is_empty() {
            return Vec::new();
        }

        // Find the maximum offset needed
        let max_offset = received_seqs
            .iter()
            .filter(|&&seq| seq >= ack_base)
            .map(|&seq| (seq - ack_base) as usize)
            .max()
            .unwrap_or(0);

        // Calculate required bitmap size (1 bit per packet, rounded up)
        let bitmap_size = (max_offset + 8) / 8;
        let mut bitmap = vec![0u8; bitmap_size];

        for &seq in received_seqs {
            if seq >= ack_base {
                let offset = (seq - ack_base) as usize;
                let byte_idx = offset / 8;
                let bit_idx = offset % 8;
                if byte_idx < bitmap.len() {
                    bitmap[byte_idx] |= 1 << bit_idx;
                }
            }
        }

        bitmap
    }
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-007
/// Title: ProtocolFrame
///
/// Description: VRConnect shall define a union type for protocol frames.
///
/// Version: V1.0
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProtocolFrame {
    Data(DataFrame),
    Ack(AckFrame),
}

impl ProtocolFrame {
    /// Serialize the ProtocolFrame to bytes using postcard
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    /// Deserialize a ProtocolFrame from bytes using postcard
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ID SRS: SRS-TEST-BLEPROTOCOL-001
    /// Title: Test SignalId conversion
    ///
    /// Description: VRConnect shall correctly convert SignalId to/from u16.
    ///
    /// Version: V1.0
    #[test]
    fn test_signal_id_conversion() {
        assert_eq!(SignalId::HR.as_u16(), 1);
        assert_eq!(SignalId::SpO2.as_u16(), 2);
        assert_eq!(SignalId::Temperature.as_u16(), 3);

        assert_eq!(SignalId::from_u16(1), Some(SignalId::HR));
        assert_eq!(SignalId::from_u16(2), Some(SignalId::SpO2));
        assert_eq!(SignalId::from_u16(3), Some(SignalId::Temperature));
        assert_eq!(SignalId::from_u16(0), None);
        assert_eq!(SignalId::from_u16(99), None);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-002
    /// Title: Test DataFrame serialization
    ///
    /// Description: VRConnect shall correctly serialize and deserialize DataFrame.
    ///
    /// Version: V1.0
    #[test]
    fn test_data_frame_serialization() {
        let payload = vec![0x01, 0x02, 0x03, 0x04];
        let frame = DataFrame::new(1, SignalId::HR.as_u16(), 100, payload.clone());

        let bytes = frame.to_bytes().expect("Failed to serialize DataFrame");
        let decoded = DataFrame::from_bytes(&bytes).expect("Failed to deserialize DataFrame");

        assert_eq!(decoded.session_id, 1);
        assert_eq!(decoded.stream_id, SignalId::HR.as_u16());
        assert_eq!(decoded.seq_num, 100);
        assert_eq!(decoded.payload, payload);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-003
    /// Title: Test AckFrame serialization
    ///
    /// Description: VRConnect shall correctly serialize and deserialize AckFrame.
    ///
    /// Version: V1.0
    #[test]
    fn test_ack_frame_serialization() {
        let bitmap = vec![0b10101010, 0b00001111];
        let frame = AckFrame::new(1, SignalId::SpO2.as_u16(), 50, bitmap);

        let bytes = frame.to_bytes().expect("Failed to serialize AckFrame");
        let decoded = AckFrame::from_bytes(&bytes).expect("Failed to deserialize AckFrame");

        assert_eq!(decoded.session_id, 1);
        assert_eq!(decoded.stream_id, SignalId::SpO2.as_u16());
        assert_eq!(decoded.ack_base, 50);
        assert_eq!(decoded.bitmap_len, 2);
        assert_eq!(decoded.bitmap, vec![0b10101010, 0b00001111]);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-004
    /// Title: Test AckFrame acknowledgment checking
    ///
    /// Description: VRConnect shall correctly check if sequence numbers are acknowledged.
    ///
    /// Version: V1.0
    #[test]
    fn test_ack_frame_is_acked() {
        // Bitmap: bit 0 = 1, bit 1 = 0, bit 2 = 1, bit 3 = 0...
        let bitmap = vec![0b00000101]; // ack_base + 0 and + 2 are acked
        let ack_frame = AckFrame::new(1, 1, 100, bitmap);

        // Below ack_base should be considered acked
        assert!(ack_frame.is_acked(99));
        assert!(ack_frame.is_acked(100)); // bit 0 = 1
        assert!(!ack_frame.is_acked(101)); // bit 1 = 0
        assert!(ack_frame.is_acked(102)); // bit 2 = 1
        assert!(!ack_frame.is_acked(103)); // bit 3 = 0
        assert!(!ack_frame.is_acked(200)); // beyond bitmap
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-005
    /// Title: Test AckFrame bitmap building
    ///
    /// Description: VRConnect shall correctly build acknowledgment bitmaps.
    ///
    /// Version: V1.0
    #[test]
    fn test_ack_frame_build_bitmap() {
        let received = vec![100, 102, 103, 107];
        let bitmap = AckFrame::build_bitmap(100, &received);

        // Bitmap should be: bit 0=1, bit 2=1, bit 3=1, bit 7=1
        // Which is: 0b10001101 = 0x8D (but LSB first, so reversed)
        // Actually: bit 0 (seq 100) = 1, bit 1 (seq 101) = 0, bit 2 (seq 102) = 1, bit 3 (seq 103) = 1
        // bit 4 (seq 104) = 0, bit 5 (seq 105) = 0, bit 6 (seq 106) = 0, bit 7 (seq 107) = 1
        // Result: 0b10001101 = 141
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap[0], 0b10001101);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-006
    /// Title: Test default medical catalog
    ///
    /// Description: VRConnect shall provide a default catalog with standard signals.
    ///
    /// Version: V1.0
    #[test]
    fn test_default_medical_catalog() {
        let catalog = Catalog::default_medical_catalog();

        assert_eq!(catalog.entries.len(), 3);

        // Check HR entry
        let hr_entry = &catalog.entries[0];
        assert_eq!(hr_entry.id, SignalId::HR.as_u16());
        assert_eq!(hr_entry.name, "HR");
        assert!(matches!(hr_entry.stream_type, StreamType::Num));
        assert_eq!(hr_entry.period_ms, 1000);

        // Check SpO2 entry
        let spo2_entry = &catalog.entries[1];
        assert_eq!(spo2_entry.id, SignalId::SpO2.as_u16());
        assert_eq!(spo2_entry.name, "SpO2");

        // Check Temperature entry
        let temp_entry = &catalog.entries[2];
        assert_eq!(temp_entry.id, SignalId::Temperature.as_u16());
        assert_eq!(temp_entry.name, "Temperature");
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-007
    /// Title: Test ProtocolFrame enum
    ///
    /// Description: VRConnect shall correctly serialize and deserialize ProtocolFrame enum.
    ///
    /// Version: V1.0
    #[test]
    fn test_protocol_frame_enum() {
        let data_frame = DataFrame::new(1, 1, 10, vec![1, 2, 3]);
        let protocol_frame = ProtocolFrame::Data(data_frame);

        let bytes = protocol_frame
            .to_bytes()
            .expect("Failed to serialize ProtocolFrame");
        let decoded =
            ProtocolFrame::from_bytes(&bytes).expect("Failed to deserialize ProtocolFrame");

        match decoded {
            ProtocolFrame::Data(df) => {
                assert_eq!(df.session_id, 1);
                assert_eq!(df.seq_num, 10);
            }
            _ => panic!("Expected DataFrame variant"),
        }
    }
}
