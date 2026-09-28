// /src/domain/ble_protocol.rs
// Module: domain.ble_protocol
// Purpose: IDT ("ICU Data Transport") BLE binary protocol — V1.1
//
// All IDT frames: [Header(13b) | Payload(Nb) | CRC32C(4b)]
// DATA_FRAME:  [Header(13b) | t0ms(8b) | count(1b) | payloadLen(2b) | dt_ms(2b) | value(4b) | CRC32C(4b)] = 34 bytes
// ACK_FRAME:   [Header(13b) | ack_upto(4b) | bitmap_len(1b) | bitmap(8b) | CRC32C(4b)] = 30 bytes
// All values: little-endian
//
// msg_type values:
//   0x01 = SUBSCRIBE_REQ  (Write → Subscribe char)
//   0x02 = SUBSCRIBE_RSP  (Notify → Data_OUT char)
//   0x10 = DATA_FRAME     (Notify → Data_OUT char)
//   0x20 = ACK_FRAME      (Write → Data_IN char)
//   0x21 = NACK_FRAME     (Write → Data_IN char)

// ─────────────────────────────────────────────────────────────────────────────
// IDT constants
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-001
/// Version: V1.0
/// Magic number identifying every IDT frame (bytes [0..2] LE = 0x7A 0xD1)
pub const IDT_MAGIC: u16 = 0xD17A;

/// Protocol version
pub const IDT_VERSION: u8 = 0x01;

// msg_type constants
pub const MSG_SUBSCRIBE_REQ: u8 = 0x01;
pub const MSG_SUBSCRIBE_RSP: u8 = 0x02;
pub const MSG_DATA_FRAME: u8 = 0x10; // IDT v1.1 DATA_FRAME msg_type (MyPredi does not check this field)
pub const MSG_ACK_FRAME: u8 = 0x20;
pub const MSG_NACK_FRAME: u8 = 0x21;

// flags bits
pub const FLAG_RETRANSMIT: u8 = 0x01; // bit0: frame is a retransmission
pub const FLAG_BACKLOG: u8 = 0x02; // bit1: historical replay in progress

// value_type codes (used in Catalog)
pub const VALUE_TYPE_FLOAT32: u8 = 3;
pub const VALUE_TYPE_UINT16: u8 = 6;

// unit_code values
pub const UNIT_BPM: u8 = 1;
pub const UNIT_PCT: u8 = 2;
pub const UNIT_MMHG: u8 = 3;
pub const UNIT_DEGC: u8 = 4;
pub const UNIT_HPA: u8 = 5;
pub const UNIT_MM: u8 = 6;

// subscribe op codes
pub const SUB_OP_SUBSCRIBE: u8 = 1;
pub const SUB_OP_UNSUBSCRIBE: u8 = 2;

// ─────────────────────────────────────────────────────────────────────────────
// SignalId — IDT signal identifiers
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-002
/// Version: V1.0
/// IDT signal identifiers per PDF signal allocation table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SignalId {
    HR = 0x0101,
    SpO2 = 0x0102,
    Temperature = 0x0103,
    SBP = 0x0201,
    DBP = 0x0202,
    MBP = 0x0203,
    StII = 0x0301,
    StV = 0x0302,
    StAvl = 0x0303,
    Spv = 0x0401,
    Ppv = 0x0402,
    AmbPres = 0x0501,
}

impl SignalId {
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            // IDT compound IDs (spec "Proposition de protocole BLE")
            0x0101 => Some(SignalId::HR),
            0x0102 => Some(SignalId::SpO2),
            0x0103 => Some(SignalId::Temperature),
            0x0201 => Some(SignalId::SBP),
            0x0202 => Some(SignalId::DBP),
            0x0203 => Some(SignalId::MBP),
            0x0301 => Some(SignalId::StII),
            0x0302 => Some(SignalId::StV),
            0x0303 => Some(SignalId::StAvl),
            0x0401 => Some(SignalId::Spv),
            0x0402 => Some(SignalId::Ppv),
            0x0501 => Some(SignalId::AmbPres),
            // Legacy simple IDs (spec "Proposition de protocole BLE.pdf" / older Central implementations)
            1 => Some(SignalId::HR),
            2 => Some(SignalId::SpO2),
            3 => Some(SignalId::Temperature),
            _ => None,
        }
    }

    /// Catalog name (used in BLE Catalog characteristic)
    pub fn name(self) -> &'static str {
        match self {
            SignalId::HR => "HR",
            SignalId::SpO2 => "PLETH_SPO2",
            SignalId::Temperature => "BT1_TEMP",
            SignalId::SBP => "SBP",
            SignalId::DBP => "DBP",
            SignalId::MBP => "MBP",
            SignalId::StII => "ST_II",
            SignalId::StV => "ST_V",
            SignalId::StAvl => "ST_AVL",
            SignalId::Spv => "SPV",
            SignalId::Ppv => "PPV",
            SignalId::AmbPres => "AMB_PRES",
        }
    }

    /// source_id = 1 (scope) for all current signals
    pub fn source_id(self) -> u8 {
        1
    }

    pub fn value_type(self) -> u8 {
        VALUE_TYPE_FLOAT32
    }

    pub fn unit_code(self) -> u8 {
        match self {
            SignalId::HR => UNIT_BPM,
            SignalId::SpO2 => UNIT_PCT,
            SignalId::Temperature => UNIT_DEGC,
            SignalId::SBP | SignalId::DBP | SignalId::MBP => UNIT_MMHG,
            SignalId::StII | SignalId::StV | SignalId::StAvl => UNIT_MM,
            SignalId::Spv | SignalId::Ppv => UNIT_PCT,
            SignalId::AmbPres => UNIT_HPA,
        }
    }

    /// String unit label sent in the BLE Catalog (matches protocol field `unit: string`).
    pub fn unit_str(self) -> &'static str {
        match self {
            SignalId::HR => "bpm",
            SignalId::SpO2 => "%",
            SignalId::Temperature => "\u{00B0}C", // °C
            SignalId::SBP | SignalId::DBP | SignalId::MBP => "mmHg",
            SignalId::StII | SignalId::StV | SignalId::StAvl => "mm",
            SignalId::Spv | SignalId::Ppv => "%",
            SignalId::AmbPres => "hPa",
        }
    }

    /// Sample kind per protocol: 0=instantaneous, 1=waveform, 2=calculated, 3=event.
    /// All current medical signals are instantaneous readings.
    pub fn sample_kind(self) -> u8 {
        0 // instantaneous
    }

    pub fn nominal_period_ms(self) -> u32 {
        match self {
            SignalId::HR => 1000,
            SignalId::SpO2 => 1000,
            SignalId::Temperature => 2000,
            // Discontinuous signals (NIBP cuff / ambient sensor)
            SignalId::SBP | SignalId::DBP | SignalId::MBP => 300_000,
            // ST segments and pressure variability — spec 1.5 Hz
            SignalId::StII | SignalId::StV | SignalId::StAvl => 667,
            SignalId::Spv | SignalId::Ppv => 667,
            SignalId::AmbPres => 10_000,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IdtHeader — 13-byte common header for all IDT frames
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-003
/// Version: V1.0
/// IDT frame header — exactly 13 bytes, little-endian.
/// Matches app's decodeHeader() which reads magic→version→msg_type→flags→
/// session_id→stream_id→seq and then immediately reads t0ms (no payload_len field).
///
/// Byte layout:
/// [0..2]  magic       u16 LE  = 0xD17A
/// [2]     version     u8      = 0x01
/// [3]     msg_type    u8
/// [4]     flags       u8
/// [5..7]  session_id  u16 LE
/// [7..9]  stream_id   u16 LE
/// [9..13] seq         u32 LE
#[derive(Debug, Clone, PartialEq)]
pub struct IdtHeader {
    pub magic: u16,
    pub version: u8,
    pub msg_type: u8,
    pub flags: u8,
    pub session_id: u16,
    pub stream_id: u16,
    pub seq: u32,
}

impl IdtHeader {
    pub const SIZE: usize = 13;

    pub fn new_data(session_id: u16, stream_id: u16, seq: u32) -> Self {
        Self {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_DATA_FRAME,
            flags: 0,
            session_id,
            stream_id,
            seq,
        }
    }

    /// Serialize to exactly 13 bytes
    pub fn to_bytes(&self) -> [u8; 13] {
        let mut b = [0u8; 13];
        b[0..2].copy_from_slice(&self.magic.to_le_bytes());
        b[2] = self.version;
        b[3] = self.msg_type;
        b[4] = self.flags;
        b[5..7].copy_from_slice(&self.session_id.to_le_bytes());
        b[7..9].copy_from_slice(&self.stream_id.to_le_bytes());
        b[9..13].copy_from_slice(&self.seq.to_le_bytes());
        b
    }

    /// Deserialize from bytes. Returns None if too short or magic mismatch.
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < Self::SIZE {
            return None;
        }
        let magic = u16::from_le_bytes([b[0], b[1]]);
        if magic != IDT_MAGIC {
            return None;
        }
        Some(Self {
            magic,
            version: b[2],
            msg_type: b[3],
            flags: b[4],
            session_id: u16::from_le_bytes([b[5], b[6]]),
            stream_id: u16::from_le_bytes([b[7], b[8]]),
            seq: u32::from_le_bytes([b[9], b[10], b[11], b[12]]),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataFrame — IDT DATA_FRAME (msg_type=0x10), count=1, float32
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-004
/// Version: V1.0
/// IDT DATA_FRAME for a single float32 sample (count=1).
///
/// Wire format (34 bytes total):
/// [Header(13b)] [t0_ms(8b)] [count=1(1b)] [payloadLen=6(2b)] [dt_ms=0(2b)] [value(4b)] [CRC32C(4b)]
///
/// Byte offsets:
/// [0..12]  Header    13 bytes (IDT header)
/// [13..20] t0_ms     u64 LE (milliseconds since Unix epoch)
/// [21]     count     u8  = 1
/// [22,23]  payloadLen u16 LE = 6 (size of dt_ms+value per sample)
/// [24,25]  dt_ms     u16 LE = 0 (delta from t0_ms for this sample)
/// [26..29] value     f32 LE
/// [30..33] CRC32C    u32 LE  crc32c of bytes [0..30]
/// Total: 34 bytes
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrame {
    pub header: IdtHeader,
    pub t0_ms: u64,
    pub value: f32,
}

impl DataFrame {
    /// Total frame size: header(13) + t0ms(8) + count(1) + payloadLen(2) + dt_ms(2) + value(4) + CRC32C(4) = 34
    pub const TOTAL_LEN: usize = 34;
    /// Byte count of payload before the CRC32C tail (the region over which CRC is computed)
    const BODY_LEN: usize = 30;
    /// Per-sample payload size written into the payloadLen field: dt_ms(2) + value(4) = 6
    const SAMPLE_PAYLOAD_LEN: u16 = 6;

    pub fn new(session_id: u16, stream_id: u16, seq: u32, t0_ms: u64, value: f32) -> Self {
        Self {
            header: IdtHeader::new_data(session_id, stream_id, seq),
            t0_ms,
            value,
        }
    }

    /// Serialize to 34 bytes. CRC32C of bytes [0..30] appended at [30..34].
    pub fn to_ble_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::TOTAL_LEN);
        buf.extend_from_slice(&self.header.to_bytes()); // [0..12]  13 bytes
        buf.extend_from_slice(&self.t0_ms.to_le_bytes()); // [13..20]  8 bytes
        buf.push(1u8); // [21]      count = 1
        buf.extend_from_slice(&Self::SAMPLE_PAYLOAD_LEN.to_le_bytes()); // [22,23]  payloadLen = 6
        buf.extend_from_slice(&0u16.to_le_bytes()); // [24,25]   dt_ms = 0
        buf.extend_from_slice(&self.value.to_le_bytes()); // [26..29]  4 bytes
                                                          // [30..33] CRC32C over the preceding 30 bytes
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Returns true if `bytes` is a well-formed DATA_FRAME with a valid CRC32C tail.
    /// Checks that bytes.len() >= TOTAL_LEN and crc32c(bytes[0..30]) == bytes[30..34].
    ///
    /// ID SRS: SRS-FN-BLEPROTOCOL-011
    /// Version: V1.0
    pub fn verify_crc(bytes: &[u8]) -> bool {
        if bytes.len() < Self::TOTAL_LEN {
            return false;
        }
        let expected = crc32c::crc32c(&bytes[..Self::BODY_LEN]);
        let actual = u32::from_le_bytes([bytes[30], bytes[31], bytes[32], bytes[33]]);
        expected == actual
    }

    /// Deserialize from bytes. Returns None on length, magic, or CRC mismatch.
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < Self::TOTAL_LEN {
            return None;
        }
        // Verify CRC32C before parsing — returns None on mismatch (does not panic)
        if !Self::verify_crc(b) {
            return None;
        }
        let header = IdtHeader::from_bytes(&b[0..IdtHeader::SIZE])?;
        if header.msg_type != MSG_DATA_FRAME {
            return None;
        }
        let t0_ms = u64::from_le_bytes([b[13], b[14], b[15], b[16], b[17], b[18], b[19], b[20]]);
        // b[21] = count (should be 1)
        // b[22,23] = payloadLen (should be 6)
        // b[24,25] = dt_ms (should be 0)
        let value = f32::from_le_bytes([b[26], b[27], b[28], b[29]]);
        Some(Self {
            header,
            t0_ms,
            value,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AckFrame — IDT ACK_FRAME (msg_type=0x20), cumulative acknowledgment
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-005
/// Version: V1.0
/// Cumulative + selective ACK from the BLE Central.
///
/// IDT wire format (30 bytes):
/// [Header(13b)]  IDT header — session_id and stream_id carried here
/// [13..17]  ack_upto    u32 LE  (last contiguously acknowledged seq)
/// [17]      bitmap_len  u8      (= 8 bytes = 64 bits)
/// [18..26]  bitmap      8 bytes (SACK: bit i = 1 ↔ seq (ack_upto+1+i) received)
/// [26..30]  CRC32C      u32 LE  (over bytes [0..26])
/// Total: 30 bytes
#[derive(Debug, Clone, PartialEq)]
pub struct AckFrame {
    pub session_id: u16,
    pub stream_id: u16,
    /// Last contiguously acknowledged seq (cumulative ACK base)
    pub ack_upto: u32,
    /// 64-bit selective-ACK bitmap: bit i = 1 means seq (ack_upto+1+i) was received
    pub bitmap: [u8; 8],
}

impl AckFrame {
    /// Total wire size: header(13) + ack_upto(4) + bitmap_len(1) + bitmap(8) + CRC32C(4) = 30
    pub const TOTAL_LEN: usize = 30;
    /// Byte count before the CRC32C tail (region over which CRC is computed)
    const BODY_LEN: usize = 26;

    /// Deserialize from bytes received on Data_IN characteristic (IDT format).
    /// Verifies IDT magic, msg_type=0x20, and CRC32C.
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < Self::TOTAL_LEN {
            return None;
        }
        let header = IdtHeader::from_bytes(&b[0..IdtHeader::SIZE])?;
        if header.msg_type != MSG_ACK_FRAME {
            return None;
        }
        let expected_crc = crc32c::crc32c(&b[..Self::BODY_LEN]);
        let actual_crc = u32::from_le_bytes([b[26], b[27], b[28], b[29]]);
        if expected_crc != actual_crc {
            return None;
        }
        let ack_upto = u32::from_le_bytes([b[13], b[14], b[15], b[16]]);
        let bitmap_len = b[17] as usize;
        let mut bitmap = [0u8; 8];
        let copy_len = bitmap_len.min(8).min(b.len().saturating_sub(18));
        bitmap[..copy_len].copy_from_slice(&b[18..18 + copy_len]);
        Some(Self {
            session_id: header.session_id,
            stream_id: header.stream_id,
            ack_upto,
            bitmap,
        })
    }

    /// Deserialize from a Flutter custom ACK (17 bytes, no IDT magic).
    ///
    /// Flutter's `sendAck()` wire format:
    /// ```text
    /// [session_id(2b LE)] [stream_id(2b LE)] [ack_upto(4b LE)] [bitmap_len(1b)] [bitmap(8b)]
    /// ```
    /// Total = 17 bytes. No IDT magic, no CRC.
    ///
    /// Returns `None` if the buffer is too short or accidentally has IDT magic
    /// (in which case `from_ble_bytes` should be used instead).
    ///
    /// ID SRS: SRS-FN-BLEPROTOCOL-014
    /// Version: V1.0
    pub fn from_flutter_bytes(b: &[u8]) -> Option<Self> {
        const FLUTTER_LEN: usize = 17;
        const FLUTTER_MIN: usize = 9; // session(2)+stream(2)+ack_upto(4)+bitmap_len(1)
        if b.len() < FLUTTER_MIN {
            return None;
        }
        // Guard: must not have IDT magic — if it does, use from_ble_bytes instead
        if has_idt_magic(b) {
            return None;
        }
        let session_id = u16::from_le_bytes([b[0], b[1]]);
        let stream_id = u16::from_le_bytes([b[2], b[3]]);
        let ack_upto = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
        let bitmap_len = b[8] as usize;
        let mut bitmap = [0u8; 8];
        let available = b.len().saturating_sub(9);
        let copy_len = bitmap_len.min(8).min(available);
        if copy_len > 0 {
            bitmap[..copy_len].copy_from_slice(&b[9..9 + copy_len]);
        }
        // Warn if the frame is shorter than the canonical 17 bytes
        if b.len() < FLUTTER_LEN {
            log::debug!(
                "Flutter ACK: short frame ({} bytes, expected {}), bitmap may be incomplete",
                b.len(),
                FLUTTER_LEN
            );
        }
        Some(Self {
            session_id,
            stream_id,
            ack_upto,
            bitmap,
        })
    }

    /// Parse MyPredi ACK format: IDT-like 24-byte header + 17-byte payload + CRC32C = 45 bytes.
    ///
    /// MyPredi uses the same 24-byte header structure for all frames (including ACK),
    /// unlike the IDT spec which uses a 13-byte header for ACK_FRAME.
    ///
    /// Wire layout:
    /// ```text
    /// [0..2]   magic=0xD17A  [2] version  [3] msgType=0x20  [4] flags
    /// [5..7]   sessionId     [7..9] streamId  [9..13] seq=ackBase
    /// [13..21] t0ms          [21] count=0  [22..24] payloadLen=17
    /// [24..26] sessionId (payload)  [26..28] streamId (payload)
    /// [28..32] ackBase       [32] bitmapLen=8  [33..41] bitmap
    /// [41..45] CRC32C
    /// ```
    pub fn from_mypredi_bytes(b: &[u8]) -> Option<Self> {
        const HEADER_LEN: usize = 24;
        const PAYLOAD_LEN: usize = 17; // sessionId(2)+streamId(2)+ackBase(4)+bitmapLen(1)+bitmap(8)
        const CRC_LEN: usize = 4;
        const TOTAL: usize = HEADER_LEN + PAYLOAD_LEN + CRC_LEN; // 45

        if b.len() < TOTAL {
            return None;
        }
        if !has_idt_magic(b) {
            return None;
        }
        if b[3] != MSG_ACK_FRAME {
            return None;
        }

        // Verify CRC32C over everything except the trailing 4-byte CRC
        let crc_offset = b.len() - CRC_LEN;
        let expected = crc32c::crc32c(&b[..crc_offset]);
        let actual = u32::from_le_bytes(b[crc_offset..crc_offset + 4].try_into().ok()?);
        if expected != actual {
            log::debug!("MyPredi ACK: CRC32C mismatch — ignored");
            return None;
        }

        // Parse payload at offset 24
        let p = HEADER_LEN;
        let session_id = u16::from_le_bytes([b[p], b[p + 1]]);
        let stream_id = u16::from_le_bytes([b[p + 2], b[p + 3]]);
        let ack_upto = u32::from_le_bytes([b[p + 4], b[p + 5], b[p + 6], b[p + 7]]);
        let bitmap_len = b[p + 8] as usize;
        let mut bitmap = [0u8; 8];
        let copy_len = bitmap_len.min(8).min(b.len().saturating_sub(p + 9));
        if copy_len > 0 {
            bitmap[..copy_len].copy_from_slice(&b[p + 9..p + 9 + copy_len]);
        }

        Some(Self {
            session_id,
            stream_id,
            ack_upto,
            bitmap,
        })
    }

    /// Returns true if `seq` is acknowledged — either cumulatively (seq ≤ ack_upto)
    /// or selectively (bit set in bitmap for seq in [ack_upto+1 .. ack_upto+64]).
    pub fn is_acked(&self, seq: u32) -> bool {
        if seq <= self.ack_upto {
            return true;
        }
        let offset = seq.wrapping_sub(self.ack_upto).wrapping_sub(1) as usize;
        if offset >= 64 {
            return false;
        }
        (self.bitmap[offset / 8] >> (offset % 8)) & 1 == 1
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NackFrame — IDT NACK_FRAME (msg_type=0x21), explicit retransmit request
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-006
/// Version: V1.0
/// Explicit NACK from the BLE Central requesting retransmission of specific frames.
///
/// Wire format:
/// [Header(15b)] [n(1b)] [reason(1b)] [seq_list(4b×n)] [CRC32C(4b)]
#[derive(Debug, Clone, PartialEq)]
pub struct NackFrame {
    pub header: IdtHeader,
    /// 1=CRC_FAIL, 2=MISSING, 3=PARSE_FAIL
    pub reason: u8,
    pub seq_list: Vec<u32>,
}

impl NackFrame {
    /// Deserialize from bytes received on Data_IN characteristic.
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        // Minimum: header(15) + n(1) + reason(1) + crc(4) = 21 bytes
        if b.len() < IdtHeader::SIZE + 2 + 4 {
            return None;
        }
        let header = IdtHeader::from_bytes(&b[0..IdtHeader::SIZE])?;
        if header.msg_type != MSG_NACK_FRAME {
            return None;
        }
        let n = b[IdtHeader::SIZE] as usize;
        let reason = b[IdtHeader::SIZE + 1];
        let crc_offset = IdtHeader::SIZE + 2 + n * 4;
        if b.len() < crc_offset + 4 {
            return None;
        }
        // Verify CRC32C over header + payload (excluding CRC)
        let expected_crc = crc32c::crc32c(&b[..crc_offset]);
        let actual_crc = u32::from_le_bytes([
            b[crc_offset],
            b[crc_offset + 1],
            b[crc_offset + 2],
            b[crc_offset + 3],
        ]);
        if expected_crc != actual_crc {
            return None;
        }
        let mut seq_list = Vec::with_capacity(n);
        for i in 0..n {
            let off = IdtHeader::SIZE + 2 + i * 4;
            seq_list.push(u32::from_le_bytes([
                b[off],
                b[off + 1],
                b[off + 2],
                b[off + 3],
            ]));
        }
        Some(Self {
            header,
            reason,
            seq_list,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SubscribeReq — IDT SUBSCRIBE_REQ (msg_type=0x01)
// ─────────────────────────────────────────────────────────────────────────────

/// One item in a SUBSCRIBE_REQ frame (17 bytes each)
///
/// Byte layout per item:
/// [0]     source_id       u8
/// [1..3]  signal_id       u16 LE
/// [3]     mode            u8  (0=LIVE, 1=BACKLOG_THEN_LIVE)
/// [4..8]  period_ms       u32 LE
/// [8]     batch_max       u8
/// [9..17] start_time_ms   u64 LE
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeItem {
    pub source_id: u8,
    pub signal_id: u16,
    pub mode: u8,
    pub period_ms: u32,
    pub batch_max: u8,
    pub start_time_ms: u64,
}

impl SubscribeItem {
    pub const SIZE: usize = 17;
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-007
/// Version: V1.0
/// SUBSCRIBE_REQ frame from the BLE Central.
///
/// Payload: [req_id(2b)] [op(1b)] [n(1b)] [items[n × 17b]]
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeReq {
    pub header: IdtHeader,
    pub req_id: u16,
    pub op: u8,
    pub items: Vec<SubscribeItem>,
}

impl SubscribeReq {
    /// Deserialize from bytes received on Subscribe characteristic.
    ///
    /// Supports non-standard SubscribeItem sizes by brute-forcing candidate strides
    /// (17..=30 bytes/item) and accepting the first that yields a valid CRC32C.
    /// Only the first `SubscribeItem::SIZE` (17) bytes of each item are decoded;
    /// any extra bytes per item are treated as padding and ignored.
    /// This handles Central implementations that use a non-standard item layout.
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        // Minimum: header(15) + req_id(2) + op(1) + n(1) + crc(4) = 23 bytes
        if b.len() < IdtHeader::SIZE + 4 + 4 {
            log::debug!(
                "SubscribeReq: too short ({} bytes, need ≥ {})",
                b.len(),
                IdtHeader::SIZE + 8
            );
            return None;
        }
        let header = IdtHeader::from_bytes(&b[0..IdtHeader::SIZE])?;
        if header.msg_type != MSG_SUBSCRIBE_REQ {
            log::debug!(
                "SubscribeReq: wrong msg_type=0x{:02X} (expected 0x01)",
                header.msg_type
            );
            return None;
        }
        let req_id = u16::from_le_bytes([b[IdtHeader::SIZE], b[IdtHeader::SIZE + 1]]);
        let op = b[IdtHeader::SIZE + 2];
        let n = b[IdtHeader::SIZE + 3] as usize;

        // Detect the actual item stride by finding which size makes CRC validate.
        // Standard size (17) is tried first; 23 is tried second (known non-standard variant).
        let stride = Self::detect_item_stride(b, n)?;

        // Parse items: read only the first SubscribeItem::SIZE bytes of each stride
        let mut items = Vec::with_capacity(n);
        for i in 0..n {
            let off = IdtHeader::SIZE + 4 + i * stride;
            if off + SubscribeItem::SIZE > b.len() {
                log::debug!("SubscribeReq: item[{}] out of bounds", i);
                return None;
            }
            items.push(SubscribeItem {
                source_id: b[off],
                signal_id: u16::from_le_bytes([b[off + 1], b[off + 2]]),
                mode: b[off + 3],
                period_ms: u32::from_le_bytes([b[off + 4], b[off + 5], b[off + 6], b[off + 7]]),
                batch_max: b[off + 8],
                start_time_ms: u64::from_le_bytes([
                    b[off + 9],
                    b[off + 10],
                    b[off + 11],
                    b[off + 12],
                    b[off + 13],
                    b[off + 14],
                    b[off + 15],
                    b[off + 16],
                ]),
            });
        }
        Some(Self {
            header,
            req_id,
            op,
            items,
        })
    }

    /// Find the item stride (bytes/item) that yields a valid CRC32C for the given buffer.
    /// Tries standard size first, then common alternatives up to 30 bytes/item.
    fn detect_item_stride(b: &[u8], n: usize) -> Option<usize> {
        // fixed overhead: header(16) + req_id(2)+op(1)+n(1) + CRC(4)
        const FIXED: usize = IdtHeader::SIZE + 4 + 4;

        if n == 0 {
            // No items — verify CRC at fixed header position
            if b.len() < FIXED {
                return None;
            }
            let crc_off = FIXED - 4;
            let exp = crc32c::crc32c(&b[..crc_off]);
            let got =
                u32::from_le_bytes([b[crc_off], b[crc_off + 1], b[crc_off + 2], b[crc_off + 3]]);
            return if exp == got { Some(0) } else { None };
        }

        // Priority order: standard (17 bytes/item) first, then common non-standard sizes
        for &candidate in &[
            SubscribeItem::SIZE,
            23usize,
            18,
            19,
            20,
            21,
            22,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
        ] {
            let payload_end = IdtHeader::SIZE + 4 + n * candidate;
            if b.len() < payload_end + 4 {
                continue; // frame too short for this candidate
            }
            let exp_crc = crc32c::crc32c(&b[..payload_end]);
            let got_crc = u32::from_le_bytes([
                b[payload_end],
                b[payload_end + 1],
                b[payload_end + 2],
                b[payload_end + 3],
            ]);
            if exp_crc == got_crc {
                if candidate != SubscribeItem::SIZE {
                    log::warn!(
                        "SubscribeReq: non-standard item stride={} bytes (expected {}) — \
                         parsing first {} bytes/item, ignoring {} extra bytes/item",
                        candidate,
                        SubscribeItem::SIZE,
                        SubscribeItem::SIZE,
                        candidate - SubscribeItem::SIZE
                    );
                }
                return Some(candidate);
            }
        }
        log::debug!(
            "SubscribeReq: no item stride yields valid CRC (buf={} bytes, n={} items)",
            b.len(),
            n
        );
        None
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SubscribeRsp — IDT SUBSCRIBE_RSP (msg_type=0x02)
// ─────────────────────────────────────────────────────────────────────────────

/// One result item in a SUBSCRIBE_RSP frame (10 bytes each)
///
/// Byte layout per result:
/// [0]    source_id             u8
/// [1..3] signal_id             u16 LE
/// [3..5] stream_id             u16 LE  (assigned by VRConnect)
/// [5..9] effective_period_ms   u32 LE
/// [9]    effective_batch_max   u8
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeRspItem {
    pub source_id: u8,
    pub signal_id: u16,
    pub stream_id: u16,
    pub effective_period_ms: u32,
    pub effective_batch_max: u8,
}

impl SubscribeRspItem {
    pub const SIZE: usize = 10;
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-008
/// Version: V1.0
/// SUBSCRIBE_RSP frame sent by VRConnect on Data_OUT characteristic.
///
/// Payload: [req_id(2b)] [status(1b)] [n(1b)] [results[n × 10b]]
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeRsp {
    pub session_id: u16,
    pub req_id: u16,
    /// 0 = OK, 1 = ERR
    pub status: u8,
    pub results: Vec<SubscribeRspItem>,
}

impl SubscribeRsp {
    /// Serialize to bytes for sending via Data_OUT Notify.
    ///
    /// Uses the **13-byte compact IDT header** — same layout as DATA_FRAME:
    ///   magic(2) | version(1) | msg_type(1) | flags(1)
    ///   | session_id(2) | stream_id(2) | seq(4)
    /// Then: payload | CRC32C(4)
    ///
    /// NOTE: if Flutter's `decodeHeader()` is updated to the 16-byte spec header
    /// (with `header_len` at byte [5]), switch to that format and update the test.
    pub fn to_ble_bytes(&self) -> Vec<u8> {
        let n = self.results.len();
        let mut buf = Vec::with_capacity(13 + 4 + n * SubscribeRspItem::SIZE + 4);

        // 13-byte compact header
        buf.extend_from_slice(&IDT_MAGIC.to_le_bytes()); // [0-1]  magic
        buf.push(IDT_VERSION); // [2]    version
        buf.push(MSG_SUBSCRIBE_RSP); // [3]    msg_type = 0x02
        buf.push(0u8); // [4]    flags
        buf.extend_from_slice(&self.session_id.to_le_bytes()); // [5-6]  session_id
        buf.extend_from_slice(&0u16.to_le_bytes()); // [7-8]  stream_id = 0
        buf.extend_from_slice(&0u32.to_le_bytes()); // [9-12] seq = 0

        // Payload
        buf.extend_from_slice(&self.req_id.to_le_bytes());
        buf.push(self.status);
        buf.push(n as u8);
        for r in &self.results {
            buf.push(r.source_id);
            buf.extend_from_slice(&r.signal_id.to_le_bytes());
            buf.extend_from_slice(&r.stream_id.to_le_bytes());
            buf.extend_from_slice(&r.effective_period_ms.to_le_bytes());
            buf.push(r.effective_batch_max);
        }

        // CRC32C on header+payload
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Serialize to the full 24-byte IDT frame format expected by MyPredi/Flutter Central v2.
    ///
    /// Flutter v2 `_processBuffer()` routes `msgType=0x02` to `_handleSubscribeResponse(payload)`,
    /// which parses the TLV payload (bytes [24..24+payloadLen]) to populate `activeStreams`.
    /// `_initStreams()` is now commented out — `activeStreams` is empty until RSP is received.
    ///
    /// Frame layout (mirrors Flutter's `buildFrame()` used for DATA_FRAMEs):
    /// ```
    /// [0-1]   magic=0xD17A
    /// [2]     ver=0x01
    /// [3]     msgType=0x02 (SUBSCRIBE_RSP)
    /// [4]     flags=0
    /// [5-6]   session_id (LE)
    /// [7-8]   stream_id=0 (LE)
    /// [9-12]  seq=0 (LE)
    /// [13-20] t0ms=0 (LE)
    /// [21]    count=0
    /// [22-23] payloadLen (LE)
    /// [24..N] TLV payload: tlv(0x01,reqId) + tlv(0x02,status) + tlv(0x03,stream)×n
    /// [N..N+4] CRC32C of [0..N]
    /// ```
    /// Each stream TLV(0x03) contains: tlv(0x01,streamId) + tlv(0x02,sourceId) +
    ///   tlv(0x03,signalId) + tlv(0x04,periodMs) + tlv(0x05,batchMax).
    pub fn to_mypredi_ble_bytes(&self) -> Vec<u8> {
        fn tlv(t: u8, value: &[u8]) -> Vec<u8> {
            let len = value.len();
            let mut out = vec![t, (len & 0xFF) as u8, ((len >> 8) & 0xFF) as u8];
            out.extend_from_slice(value);
            out
        }

        // Build TLV payload (no outer 0x21 wrapper — Flutter reads directly from frame payload)
        let mut payload: Vec<u8> = Vec::new();
        payload.extend(tlv(0x01, &self.req_id.to_le_bytes()));
        payload.extend(tlv(0x02, &[self.status]));
        for r in &self.results {
            let mut sp: Vec<u8> = Vec::new();
            sp.extend(tlv(0x01, &r.stream_id.to_le_bytes()));
            sp.extend(tlv(0x02, &[r.source_id]));
            sp.extend(tlv(0x03, &r.signal_id.to_le_bytes()));
            sp.extend(tlv(0x04, &r.effective_period_ms.to_le_bytes()));
            sp.extend(tlv(0x05, &[r.effective_batch_max]));
            payload.extend(tlv(0x03, &sp));
        }

        let payload_len = payload.len() as u16;

        // 24-byte IDT header (identical layout to DATA_FRAME header)
        let mut buf: Vec<u8> = Vec::with_capacity(24 + payload.len() + 4);
        buf.extend_from_slice(&IDT_MAGIC.to_le_bytes()); // [0-1]
        buf.push(IDT_VERSION); // [2]
        buf.push(MSG_SUBSCRIBE_RSP); // [3] = 0x02
        buf.push(0u8); // [4]  flags
        buf.extend_from_slice(&self.session_id.to_le_bytes()); // [5-6]
        buf.extend_from_slice(&0u16.to_le_bytes()); // [7-8]  stream_id=0
        buf.extend_from_slice(&0u32.to_le_bytes()); // [9-12] seq=0
        buf.extend_from_slice(&0u64.to_le_bytes()); // [13-20] t0ms=0
        buf.push(0u8); // [21] count=0
        buf.extend_from_slice(&payload_len.to_le_bytes()); // [22-23]
        buf.extend_from_slice(&payload); // [24..24+payloadLen]

        // CRC32C of entire header+payload
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Serialize to the TLV format expected by the Flutter/MyPredi Central app.
    ///
    /// Wire format (mirrors `buildSubscribeRsp()` in the Flutter peripheral simulator):
    /// ```
    /// tlv(0x21, [
    ///   tlv(0x01, req_id_2b_le),
    ///   tlv(0x02, [status]),
    ///   tlv(0x03, [                    ← one per stream
    ///     tlv(0x01, stream_id_2b_le),
    ///     tlv(0x02, [source_id]),
    ///     tlv(0x03, signal_id_2b_le),  ← simple ID (1/2/3), not compound
    ///     tlv(0x04, period_ms_4b_le),
    ///     tlv(0x05, [batch_max]),
    ///   ]),
    ///   ...
    /// ])
    /// ```
    /// Each TLV field is encoded as `[type(1b), len_lo(1b), len_hi(1b), ...value]`.
    /// Signal IDs are the full compound IDT ID (2b LE) — e.g. 0x0101, 0x0201, 0x0501.
    pub fn to_flutter_tlv_bytes(&self) -> Vec<u8> {
        fn tlv(t: u8, value: &[u8]) -> Vec<u8> {
            let len = value.len();
            let mut out = vec![t, (len & 0xFF) as u8, ((len >> 8) & 0xFF) as u8];
            out.extend_from_slice(value);
            out
        }

        let mut payload: Vec<u8> = Vec::new();

        payload.extend(tlv(0x01, &self.req_id.to_le_bytes()));
        payload.extend(tlv(0x02, &[self.status]));

        for r in &self.results {
            let mut sp: Vec<u8> = Vec::new();
            sp.extend(tlv(0x01, &r.stream_id.to_le_bytes()));
            sp.extend(tlv(0x02, &[r.source_id]));
            // Full compound signal_id (2b LE) — mirrors what Flutter sent in SUBSCRIBE_REQ.
            // 0x01xx signals keep backward compat; 0x02xx/0x05xx need the full ID (lower byte alone conflicts).
            sp.extend(tlv(0x03, &r.signal_id.to_le_bytes()));
            sp.extend(tlv(0x04, &r.effective_period_ms.to_le_bytes()));
            sp.extend(tlv(0x05, &[r.effective_batch_max]));
            payload.extend(tlv(0x03, &sp));
        }

        tlv(0x21, &payload)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Catalog — IDT signal catalog (TLV binary format)
// ─────────────────────────────────────────────────────────────────────────────

/// One entry in the Catalog characteristic (binary TLV, variable length).
///
/// Byte layout per entry (protocol field order):
/// [0..2]  signal_id         u16 LE
/// [2]     source_id         u8
/// [3]     name_len          u8
/// [4..]   name              UTF-8 bytes (e.g. "PLETH_SPO2")
/// [+0]    unit_len          u8
/// [+1..]  unit              UTF-8 bytes (e.g. "%", "mmHg", "°C")
/// [+0]    value_type        u8  (3=float32, 6=uint16)
/// [+1]    sample_kind       u8  (0=instantaneous, 1=waveform, 2=calculated, 3=event)
/// [+2..6] nominal_period_ms u32 LE
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogEntry {
    pub source_id: u8,
    pub signal_id: u16,
    pub value_type: u8,
    /// String unit label: "bpm", "%", "°C", "mmHg", "hPa"
    pub unit: String,
    /// 0=instantaneous, 1=waveform, 2=calculated, 3=event
    pub sample_kind: u8,
    pub nominal_period_ms: u32,
    pub name: String,
}

/// ID SRS: SRS-MOD-BLEPROTOCOL-009
/// Version: V1.0
/// Available signal catalog, read by the BLE Central at connection time.
#[derive(Debug, Clone)]
pub struct Catalog {
    pub entries: Vec<CatalogEntry>,
}

impl Catalog {
    /// Default medical catalog: HR, SpO2, Temperature, SBP, DBP, MBP, AmbPres (all float32, source=scope)
    pub fn default_medical_catalog() -> Self {
        Self {
            entries: [
                SignalId::HR,
                SignalId::SpO2,
                SignalId::Temperature,
                SignalId::SBP,
                SignalId::DBP,
                SignalId::MBP,
                SignalId::StII,
                SignalId::StV,
                SignalId::StAvl,
                SignalId::Spv,
                SignalId::Ppv,
                SignalId::AmbPres,
            ]
            .iter()
            .map(|&sig| CatalogEntry {
                source_id: sig.source_id(),
                signal_id: sig.as_u16(),
                value_type: sig.value_type(),
                unit: sig.unit_str().to_string(),
                sample_kind: sig.sample_kind(),
                nominal_period_ms: sig.nominal_period_ms(),
                name: sig.name().to_string(),
            })
            .collect(),
        }
    }

    /// Serialize to binary for the Catalog GATT characteristic (Read).
    ///
    /// Layout per entry (protocol spec v1 p.20):
    ///   source_id(1) | signal_id(2 LE) | value_type(1) | unit_code(1) | nominal_period_ms(4 LE) | name_len(1) | name(N)
    ///
    /// unit_code: 1=bpm, 2=%, 3=mmHg, 4=°C, 5=mV, 6=mm, 255=custom
    /// value_type: 1=int32, 2=uint32, 3=float32, 4=string, 5=int16, 6=uint16
    pub fn to_ble_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for e in &self.entries {
            let unit_code = SignalId::from_u16(e.signal_id)
                .map(|s| s.unit_code())
                .unwrap_or(255);
            buf.push(e.source_id);
            buf.extend_from_slice(&e.signal_id.to_le_bytes());
            buf.push(e.value_type);
            buf.push(unit_code);
            buf.extend_from_slice(&e.nominal_period_ms.to_le_bytes());
            let name_bytes = e.name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
        }
        buf
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// InboundFrame — dispatcher for frames received from the BLE Central
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-010
/// Version: V1.0
/// Dispatches inbound BLE writes by msg_type.
pub enum InboundFrame {
    Ack(AckFrame),
    Nack(NackFrame),
    SubscribeReq(SubscribeReq),
}

impl InboundFrame {
    /// Parse an inbound IDT frame from the BLE Central.
    ///
    /// All inbound frames carry IDT magic. Dispatch is on msg_type:
    ///   0x20 = ACK_FRAME      → InboundFrame::Ack
    ///   0x21 = NACK_FRAME     → InboundFrame::Nack
    ///   0x01 = SUBSCRIBE_REQ  → InboundFrame::SubscribeReq
    ///   other                 → None
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < IdtHeader::SIZE {
            return None;
        }
        let magic = u16::from_le_bytes([b[0], b[1]]);
        if magic != IDT_MAGIC {
            return None;
        }
        match b[3] {
            MSG_ACK_FRAME => AckFrame::from_ble_bytes(b).map(InboundFrame::Ack),
            MSG_NACK_FRAME => NackFrame::from_ble_bytes(b).map(InboundFrame::Nack),
            MSG_SUBSCRIBE_REQ => SubscribeReq::from_ble_bytes(b).map(InboundFrame::SubscribeReq),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// has_idt_magic — inline magic check helper
// ─────────────────────────────────────────────────────────────────────────────

/// Returns `true` if `bytes` is at least 2 bytes long and bytes[0..2] encodes
/// IDT_MAGIC (0xD17A) in little-endian order.
///
/// Used to route incoming BLE writes without constructing a full IdtHeader.
///
/// ID SRS: SRS-FN-BLEPROTOCOL-012
/// Version: V1.0
pub fn has_idt_magic(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && u16::from_le_bytes([bytes[0], bytes[1]]) == IDT_MAGIC
}

// ─────────────────────────────────────────────────────────────────────────────
// SignalMeta + SignalRegistry — extensible signal catalog
// ─────────────────────────────────────────────────────────────────────────────

/// Metadata for a single registered signal.
///
/// ID SRS: SRS-MOD-BLEPROTOCOL-012
/// Version: V1.0
#[derive(Debug, Clone, PartialEq)]
pub struct SignalMeta {
    pub signal_id: u16,
    pub source_id: u8,
    pub name: String,
    /// Value encoding — VALUE_TYPE_FLOAT32 (3) for all V1 medical signals
    pub value_type: u8,
    /// String unit label: "bpm", "%", "°C", "mmHg", "hPa"
    pub unit: String,
    /// 0=instantaneous, 1=waveform, 2=calculated, 3=event
    pub sample_kind: u8,
    pub nominal_period_ms: u32,
}

/// Extensible registry of known BLE signals.
///
/// Replaces compile-time `SignalId` enum for catalog/subscribe/output lookups.
/// Coexists with `SignalId` enum for backward-compatible data-pipeline usage.
/// `SignalRegistry::with_defaults()` pre-registers HR, SpO2, Temperature.
/// Call `register()` to add further signals at startup.
///
/// ID SRS: SRS-MOD-BLEPROTOCOL-013
/// Version: V1.0
pub struct SignalRegistry {
    signals: std::collections::HashMap<u16, SignalMeta>,
}

impl Default for SignalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            signals: std::collections::HashMap::new(),
        }
    }

    /// Pre-register all twelve medical signals.
    pub fn with_defaults() -> Self {
        let mut r = Self::new();
        for sig in [
            SignalId::HR,
            SignalId::SpO2,
            SignalId::Temperature,
            SignalId::SBP,
            SignalId::DBP,
            SignalId::MBP,
            SignalId::StII,
            SignalId::StV,
            SignalId::StAvl,
            SignalId::Spv,
            SignalId::Ppv,
            SignalId::AmbPres,
        ] {
            r.register(SignalMeta {
                signal_id: sig.as_u16(),
                source_id: sig.source_id(),
                name: sig.name().to_string(),
                value_type: sig.value_type(),
                unit: sig.unit_str().to_string(),
                sample_kind: sig.sample_kind(),
                nominal_period_ms: sig.nominal_period_ms(),
            });
        }
        r
    }

    /// Register a signal. If `signal_id` is already present, the entry is replaced.
    pub fn register(&mut self, meta: SignalMeta) {
        self.signals.insert(meta.signal_id, meta);
    }

    /// Look up a signal by its canonical IDT signal_id (e.g. 0x0101).
    pub fn get(&self, signal_id: u16) -> Option<&SignalMeta> {
        self.signals.get(&signal_id)
    }

    /// Normalize a raw signal ID (legacy 1/2/3 or IDT 0x0101–0x01FF) to the
    /// canonical signal_id stored in this registry.  Returns `None` if the ID is
    /// unknown.
    pub fn normalize_id(&self, raw: u16) -> Option<u16> {
        // Fast path: direct lookup (handles IDT compound IDs and any custom IDs)
        if self.signals.contains_key(&raw) {
            return Some(raw);
        }
        // Legacy path: delegate to SignalId enum for the three V1 simple IDs (1/2/3)
        SignalId::from_u16(raw)
            .map(|s| s.as_u16())
            .filter(|id| self.signals.contains_key(id))
    }

    /// Returns `true` if `raw` resolves (via `normalize_id`) to a registered signal.
    pub fn contains_normalized(&self, raw: u16) -> bool {
        self.normalize_id(raw).is_some()
    }

    /// Returns all registered canonical signal IDs, sorted ascending.
    pub fn all_signal_ids(&self) -> Vec<u16> {
        let mut ids: Vec<u16> = self.signals.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Build a `Catalog` from all registered signals (sorted by signal_id).
    pub fn build_catalog(&self) -> Catalog {
        let entries = self
            .all_signal_ids()
            .into_iter()
            .filter_map(|id| self.signals.get(&id))
            .map(|m| CatalogEntry {
                source_id: m.source_id,
                signal_id: m.signal_id,
                value_type: m.value_type,
                unit: m.unit.clone(),
                sample_kind: m.sample_kind,
                nominal_period_ms: m.nominal_period_ms,
                name: m.name.clone(),
            })
            .collect();
        Catalog { entries }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// parse_tlv_subscribe_req — Flutter TLV SUBSCRIBE_REQ fallback parser
// ─────────────────────────────────────────────────────────────────────────────

/// One parsed SUBSCRIBE_REQ item: `(signal_id, requested_period_ms)`. `None` for the
/// period means "not requested" (absent nested TLV, or the sentinel value `0` that
/// every deployed app sends).
pub type SubscribeReqEntry = (u16, Option<u32>);

/// Parse a Flutter-custom TLV SUBSCRIBE_REQ (byte[0]=0x20, no IDT magic).
///
/// Flutter sends this format instead of an IDT-framed SUBSCRIBE_REQ:
/// ```text
/// [marker=0x20(1b)] [total_len(2b LE)] [version(1b)] [flags/op(2b)] [req_id(2b LE)]
/// [items: n × { tag=0x03(1b) len(2b LE) [nested TLVs] }]
/// ```
/// Item scanning is **length-driven**: each item's `len` field is read and the cursor
/// advances by `3 + len`, rather than assuming a fixed 24-byte value. Deployed apps send
/// `len=24`; this also tolerates a future item carrying additional nested TLVs.
///
/// Each item's value is itself a sequence of nested TLVs `[tag(1b)][len(2b LE)][value]`.
/// Two are read: tag `0x02` (2 bytes) → `signal_id` (u16 LE), tag `0x04` (4 bytes) →
/// `period_ms` (u32 LE). `period_ms == 0` (what every deployed app sends today) is normalized to `None` here so callers only ever see "requested" or "not
/// requested".
///
/// Returns `Some((req_id, entries))` on success, `None` if the frame is not a valid
/// Flutter TLV subscribe (wrong marker, too short, or all signal_ids are zero).
///
/// Flutter sends this format although the IDT spec requires a full IDT-framed
/// SUBSCRIBE_REQ. Both are accepted; TLV is tried as fallback.
///
/// ID SRS: SRS-FN-BLEPROTOCOL-013
/// Version: V2.0
pub fn parse_tlv_subscribe_req(data: &[u8]) -> Option<(u16, Vec<SubscribeReqEntry>)> {
    // Byte[0] must be 0x20 (TLV SUBSCRIBE_CMD marker); minimum: 8-byte header + 1 item.
    // The smallest legal item is tag(1)+len(2) with len=0, i.e. 3 bytes.
    if data.len() < 8 + 3 || data[0] != 0x20 {
        return None;
    }

    let req_id = u16::from_le_bytes([data[6], data[7]]);

    // Items start immediately after the 8-byte header:
    // marker(1) + total_len(2) + version(1) + flags(2) + req_id(2) = 8 bytes
    let mut pos = 8;
    let mut entries = Vec::new();

    while pos + 3 <= data.len() {
        // Each item: tag=0x03, len_u16_le = length of the nested-TLV value that follows.
        if data[pos] != 0x03 {
            break;
        }
        let item_len = u16::from_le_bytes([data[pos + 1], data[pos + 2]]) as usize;
        let value_start = pos + 3;
        let value_end = match value_start.checked_add(item_len) {
            Some(end) if end <= data.len() => end,
            _ => break,
        };

        let mut signal_id: Option<u16> = None;
        let mut period_ms: Option<u32> = None;

        // Walk the nested TLVs inside [value_start..value_end).
        let mut npos = value_start;
        while npos + 3 <= value_end {
            let tag = data[npos];
            let nlen = u16::from_le_bytes([data[npos + 1], data[npos + 2]]) as usize;
            let nvalue_start = npos + 3;
            let nvalue_end = match nvalue_start.checked_add(nlen) {
                Some(end) if end <= value_end => end,
                _ => break,
            };
            match tag {
                0x02 if nlen == 2 => {
                    signal_id = Some(u16::from_le_bytes([
                        data[nvalue_start],
                        data[nvalue_start + 1],
                    ]));
                }
                0x04 if nlen == 4 => {
                    period_ms = Some(u32::from_le_bytes([
                        data[nvalue_start],
                        data[nvalue_start + 1],
                        data[nvalue_start + 2],
                        data[nvalue_start + 3],
                    ]));
                }
                _ => {}
            }
            npos = nvalue_end;
        }

        if let Some(sid) = signal_id {
            if sid > 0 {
                // 0 = "not requested" sentinel (matches every deployed app today).
                let requested_period = period_ms.filter(|&p| p > 0);
                entries.push((sid, requested_period));
            }
        }

        pos = value_end;
    }

    if entries.is_empty() {
        None
    } else {
        Some((req_id, entries))
    }
}

/// Parse an IDT-wrapped MyPredi TLV SUBSCRIBE_REQ.
///
/// Some legacy Flutter/MyPredi centrals send an IDT-like envelope with
/// `IDT_MAGIC` and `MSG_SUBSCRIBE_REQ` before the actual TLV payload.
/// The real TLV section begins at offset 24 and ends 4 bytes before the end.
/// Returns `Some((req_id, entries))` if the embedded TLV payload is valid.
pub fn parse_idt_wrapped_tlv_subscribe_req(data: &[u8]) -> Option<(u16, Vec<SubscribeReqEntry>)> {
    if data.get(3).copied() != Some(MSG_SUBSCRIBE_REQ) || data.len() <= 28 {
        return None;
    }
    let tlv_payload = &data[24..data.len().saturating_sub(4)];
    parse_tlv_subscribe_req(tlv_payload)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
