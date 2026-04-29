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

// subscribe op codes
pub const SUB_OP_SUBSCRIBE: u8 = 1;
pub const SUB_OP_UNSUBSCRIBE: u8 = 2;

// ─────────────────────────────────────────────────────────────────────────────
// SignalId — IDT signal identifiers
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-002
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
            SignalId::AmbPres => 10_000,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IdtHeader — 13-byte common header for all IDT frames
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-003
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
/// IDT DATA_FRAME for a single float32 sample (count=1).
///
/// Wire format (34 bytes total — [TODO-1 resolved] CRC32C appended):
/// [Header(13b)] [t0_ms(8b)] [count=1(1b)] [payloadLen=6(2b)] [dt_ms=0(2b)] [value(4b)] [CRC32C(4b)]
///
/// Byte offsets:
/// [0..12]  Header    13 bytes (IDT header)
/// [13..20] t0_ms     u64 LE (milliseconds since Unix epoch)
/// [21]     count     u8  = 1
/// [22,23]  payloadLen u16 LE = 6 (size of dt_ms+value per sample)
/// [24,25]  dt_ms     u16 LE = 0 (delta from t0_ms for this sample)
/// [26..29] value     f32 LE
/// [30..33] CRC32C    u32 LE  crc32c of bytes [0..30]   ← [TODO-1 resolved]
/// Total: 34 bytes
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrame {
    pub header: IdtHeader,
    pub t0_ms: u64,
    pub value: f32,
}

impl DataFrame {
    /// Total frame size: header(13) + t0ms(8) + count(1) + payloadLen(2) + dt_ms(2) + value(4) + CRC32C(4) = 34
    /// [TODO-1 resolved] CRC32C appended; was 30 bytes (DEV-1).
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
    /// [TODO-1 resolved] CRC32C appended.
    pub fn to_ble_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::TOTAL_LEN);
        buf.extend_from_slice(&self.header.to_bytes()); // [0..12]  13 bytes
        buf.extend_from_slice(&self.t0_ms.to_le_bytes()); // [13..20]  8 bytes
        buf.push(1u8); // [21]      count = 1
        buf.extend_from_slice(&Self::SAMPLE_PAYLOAD_LEN.to_le_bytes()); // [22,23]  payloadLen = 6
        buf.extend_from_slice(&0u16.to_le_bytes()); // [24,25]   dt_ms = 0
        buf.extend_from_slice(&self.value.to_le_bytes()); // [26..29]  4 bytes
                                                          // [30..33] CRC32C over the preceding 30 bytes [TODO-1 resolved]
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

    /// Deserialize from a Flutter custom ACK (17 bytes, no IDT magic) — [DEV-2].
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
        buf.extend_from_slice(&IDT_MAGIC.to_le_bytes());      // [0-1]  magic
        buf.push(IDT_VERSION);                                 // [2]    version
        buf.push(MSG_SUBSCRIBE_RSP);                           // [3]    msg_type = 0x02
        buf.push(0u8);                                         // [4]    flags
        buf.extend_from_slice(&self.session_id.to_le_bytes()); // [5-6]  session_id
        buf.extend_from_slice(&0u16.to_le_bytes());            // [7-8]  stream_id = 0
        buf.extend_from_slice(&0u32.to_le_bytes());            // [9-12] seq = 0

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
        buf.extend_from_slice(&IDT_MAGIC.to_le_bytes());          // [0-1]
        buf.push(IDT_VERSION);                                     // [2]
        buf.push(MSG_SUBSCRIBE_RSP);                               // [3] = 0x02
        buf.push(0u8);                                             // [4]  flags
        buf.extend_from_slice(&self.session_id.to_le_bytes());     // [5-6]
        buf.extend_from_slice(&0u16.to_le_bytes());                // [7-8]  stream_id=0
        buf.extend_from_slice(&0u32.to_le_bytes());                // [9-12] seq=0
        buf.extend_from_slice(&0u64.to_le_bytes());                // [13-20] t0ms=0
        buf.push(0u8);                                             // [21] count=0
        buf.extend_from_slice(&payload_len.to_le_bytes());         // [22-23]
        buf.extend_from_slice(&payload);                           // [24..24+payloadLen]

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

impl SignalRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            signals: std::collections::HashMap::new(),
        }
    }

    /// Pre-register the seven V1 medical signals: HR (0x0101), SpO2 (0x0102),
    /// Temperature (0x0103), SBP (0x0201), DBP (0x0202), MBP (0x0203), AmbPres (0x0501).
    pub fn with_defaults() -> Self {
        let mut r = Self::new();
        for sig in [
            SignalId::HR,
            SignalId::SpO2,
            SignalId::Temperature,
            SignalId::SBP,
            SignalId::DBP,
            SignalId::MBP,
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

/// Parse a Flutter-custom TLV SUBSCRIBE_REQ (byte[0]=0x20, no IDT magic).
///
/// Flutter sends this format instead of an IDT-framed SUBSCRIBE_REQ:
/// ```text
/// [marker=0x20(1b)] [total_len(2b LE)] [version(1b)] [flags/op(2b)] [req_id(2b LE)] [n(1b)]
/// [items: n × { tag=0x03(1b) len=24(2b LE) [nested TLVs] }]
/// ```
/// Each item contains a 2-byte LE signal_id at item_base+10 (inside the nested TLV).
///
/// Returns `Some((req_id, signal_ids))` on success, `None` if the frame is not a valid
/// Flutter TLV subscribe (wrong marker, too short, or all signal_ids are zero).
///
/// [DEV-3] cross-reference: Flutter sends this format; IDT spec requires a full
/// IDT-framed SUBSCRIBE_REQ. Both are accepted; TLV is tried as fallback.
///
/// ID SRS: SRS-FN-BLEPROTOCOL-013
/// Version: V1.0
pub fn parse_tlv_subscribe_req(data: &[u8]) -> Option<(u16, Vec<u16>)> {
    // Byte[0] must be 0x20 (TLV SUBSCRIBE_CMD marker); minimum: 9-byte header + 1 item (27b)
    // Minimum: 8-byte header + at least one 27-byte item
    if data.len() < 8 + 27 || data[0] != 0x20 {
        return None;
    }

    let req_id = u16::from_le_bytes([data[6], data[7]]);

    // Items start immediately after the 8-byte header:
    // marker(1) + total_len(2) + version(1) + flags(2) + req_id(2) = 8 bytes
    let mut pos = 8;
    let mut signal_ids = Vec::new();

    while pos + 27 <= data.len() {
        // Each item: tag=0x03, len_u16_le=24 (0x18 0x00)
        if data[pos] == 0x03 && data[pos + 1] == 0x18 && data[pos + 2] == 0x00 {
            // signal_id (LE u16) is at item_base + 10 (inside nested TLV tag=2, len=2)
            let signal_id = u16::from_le_bytes([data[pos + 10], data[pos + 11]]);
            if signal_id > 0 {
                signal_ids.push(signal_id);
            }
            pos += 27; // 3 (item tag+len) + 24 (item value)
        } else {
            break;
        }
    }

    if signal_ids.is_empty() {
        None
    } else {
        Some((req_id, signal_ids))
    }
}

/// Parse an IDT-wrapped MyPredi TLV SUBSCRIBE_REQ.
///
/// Some legacy Flutter/MyPredi centrals send an IDT-like envelope with
/// `IDT_MAGIC` and `MSG_SUBSCRIBE_REQ` before the actual TLV payload.
/// The real TLV section begins at offset 24 and ends 4 bytes before the end.
/// Returns `Some((req_id, signal_ids))` if the embedded TLV payload is valid.
pub fn parse_idt_wrapped_tlv_subscribe_req(data: &[u8]) -> Option<(u16, Vec<u16>)> {
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
mod tests {
    use super::*;

    // ── Helper: build a valid IDT ACK_FRAME byte buffer ──────────────────────
    // IDT ACK_FRAME: [Header(13b)][ack_upto(4b)][bitmap_len=8(1b)][bitmap(8b)][CRC32C(4b)] = 30b
    fn make_ack_frame_bytes(session_id: u16, stream_id: u16, ack_upto: u32) -> Vec<u8> {
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_ACK_FRAME,
            flags: 0,
            session_id,
            stream_id,
            seq: 0,
        };
        let mut buf = Vec::with_capacity(AckFrame::TOTAL_LEN);
        buf.extend_from_slice(&header.to_bytes()); // [0..12]  13 bytes
        buf.extend_from_slice(&ack_upto.to_le_bytes()); // [13..16]  4 bytes
        buf.push(8u8); // [17]      bitmap_len = 8
        buf.extend_from_slice(&0u64.to_le_bytes()); // [18..25]  bitmap = all zeros
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes()); // [26..29]  CRC32C
        buf
    }

    // ── Helper: build a valid NACK_FRAME byte buffer ──────────────────────────
    fn make_nack_frame_bytes(session_id: u16, stream_id: u16, reason: u8, seqs: &[u32]) -> Vec<u8> {
        let n = seqs.len();
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_NACK_FRAME,
            flags: 0,
            session_id,
            stream_id,
            seq: 0,
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(&header.to_bytes());
        buf.push(n as u8);
        buf.push(reason);
        for &seq in seqs {
            buf.extend_from_slice(&seq.to_le_bytes());
        }
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    // ── Helper: build a valid SUBSCRIBE_REQ byte buffer ──────────────────────
    fn make_subscribe_req_bytes(
        session_id: u16,
        req_id: u16,
        op: u8,
        items: &[(u8, u16)],
    ) -> Vec<u8> {
        let n = items.len();
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_SUBSCRIBE_REQ,
            flags: 0,
            session_id,
            stream_id: 0,
            seq: 0,
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(&req_id.to_le_bytes());
        buf.push(op);
        buf.push(n as u8);
        for &(source_id, signal_id) in items {
            buf.push(source_id);
            buf.extend_from_slice(&signal_id.to_le_bytes());
            buf.push(0u8); // mode = LIVE
            buf.extend_from_slice(&1000u32.to_le_bytes()); // period_ms
            buf.push(1u8); // batch_max
            buf.extend_from_slice(&0u64.to_le_bytes()); // start_time_ms
        }
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    // ── SignalId tests ────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-001
    #[test]
    fn test_signal_id_new_values() {
        assert_eq!(SignalId::HR.as_u16(), 0x0101);
        assert_eq!(SignalId::SpO2.as_u16(), 0x0102);
        assert_eq!(SignalId::Temperature.as_u16(), 0x0103);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-002
    #[test]
    fn test_signal_id_from_u16_roundtrip() {
        // IDT compound IDs (primary spec)
        assert_eq!(SignalId::from_u16(0x0101), Some(SignalId::HR));
        assert_eq!(SignalId::from_u16(0x0102), Some(SignalId::SpO2));
        assert_eq!(SignalId::from_u16(0x0103), Some(SignalId::Temperature));
        assert_eq!(SignalId::from_u16(0x0201), Some(SignalId::SBP));
        assert_eq!(SignalId::from_u16(0x0202), Some(SignalId::DBP));
        assert_eq!(SignalId::from_u16(0x0203), Some(SignalId::MBP));
        assert_eq!(SignalId::from_u16(0x0501), Some(SignalId::AmbPres));
        // Legacy simple IDs (I.pdf / older Central) — must also be accepted
        assert_eq!(SignalId::from_u16(1), Some(SignalId::HR));
        assert_eq!(SignalId::from_u16(2), Some(SignalId::SpO2));
        assert_eq!(SignalId::from_u16(3), Some(SignalId::Temperature));
        // Unknown IDs must be rejected
        assert_eq!(SignalId::from_u16(0), None);
        assert_eq!(SignalId::from_u16(4), None);
        assert_eq!(SignalId::from_u16(0x0200), None);
        assert_eq!(SignalId::from_u16(0x0500), None);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-003
    #[test]
    fn test_signal_id_metadata() {
        assert_eq!(SignalId::HR.name(), "HR");
        assert_eq!(SignalId::SpO2.name(), "PLETH_SPO2");
        assert_eq!(SignalId::Temperature.name(), "BT1_TEMP");
        assert_eq!(SignalId::SBP.name(), "SBP");
        assert_eq!(SignalId::DBP.name(), "DBP");
        assert_eq!(SignalId::MBP.name(), "MBP");
        assert_eq!(SignalId::AmbPres.name(), "AMB_PRES");
        assert_eq!(SignalId::HR.unit_code(), UNIT_BPM);
        assert_eq!(SignalId::SpO2.unit_code(), UNIT_PCT);
        assert_eq!(SignalId::Temperature.unit_code(), UNIT_DEGC);
        assert_eq!(SignalId::SBP.unit_code(), UNIT_MMHG);
        assert_eq!(SignalId::DBP.unit_code(), UNIT_MMHG);
        assert_eq!(SignalId::MBP.unit_code(), UNIT_MMHG);
        assert_eq!(SignalId::AmbPres.unit_code(), UNIT_HPA);
        assert_eq!(SignalId::HR.nominal_period_ms(), 1000);
        assert_eq!(SignalId::Temperature.nominal_period_ms(), 2000);
        assert_eq!(SignalId::SBP.nominal_period_ms(), 300_000);
        assert_eq!(SignalId::AmbPres.nominal_period_ms(), 10_000);
        assert_eq!(SignalId::HR.source_id(), 1);
        assert_eq!(SignalId::HR.value_type(), VALUE_TYPE_FLOAT32);
    }

    // ── IdtHeader tests ───────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-004
    #[test]
    fn test_idt_header_byte_layout() {
        let h = IdtHeader::new_data(42, 7, 100);
        let b = h.to_bytes();
        assert_eq!(b.len(), 13);
        // magic at [0..2] = 0xD17A → LE: 0x7A 0xD1
        assert_eq!(b[0], 0x7A);
        assert_eq!(b[1], 0xD1);
        assert_eq!(b[2], IDT_VERSION);
        assert_eq!(b[3], MSG_DATA_FRAME);
        assert_eq!(b[4], 0); // flags
        assert_eq!(u16::from_le_bytes([b[5], b[6]]), 42); // session_id
        assert_eq!(u16::from_le_bytes([b[7], b[8]]), 7); // stream_id
        assert_eq!(u32::from_le_bytes([b[9], b[10], b[11], b[12]]), 100); // seq
                                                                          // No payload_len field in 13-byte header
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-005
    #[test]
    fn test_idt_header_magic_mismatch() {
        let mut b = IdtHeader::new_data(1, 1, 1).to_bytes();
        b[0] = 0xFF; // corrupt magic
        assert!(IdtHeader::from_bytes(&b).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-006
    #[test]
    fn test_idt_header_roundtrip() {
        let original = IdtHeader::new_data(5, 3, 999);
        let parsed = IdtHeader::from_bytes(&original.to_bytes()).unwrap();
        assert_eq!(original, parsed);
    }

    // ── DataFrame tests ───────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-007
    #[test]
    fn test_data_frame_total_length() {
        let frame = DataFrame::new(1, 1, 1, 0, 65.0);
        assert_eq!(frame.to_ble_bytes().len(), DataFrame::TOTAL_LEN);
        assert_eq!(DataFrame::TOTAL_LEN, 34); // [TODO-1 resolved] +4 CRC32C
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-008
    #[test]
    fn test_data_frame_byte_layout() {
        let t0_ms: u64 = 1_700_000_000_000;
        let value: f32 = 72.5;
        let bytes = DataFrame::new(1, 2, 10, t0_ms, value).to_ble_bytes();

        // Header occupies [0..13] — 13-byte header (no payload_len field)
        assert_eq!(u16::from_le_bytes([bytes[0], bytes[1]]), IDT_MAGIC);
        assert_eq!(bytes[3], MSG_DATA_FRAME);

        // t0_ms at [13..21] (immediately after the 13-byte header)
        let parsed_t0 = u64::from_le_bytes([
            bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20],
        ]);
        assert_eq!(parsed_t0, t0_ms);

        // count at [21] = 1
        assert_eq!(bytes[21], 1u8);

        // payloadLen at [22,23] = 6 (size of dt_ms+value per sample)
        assert_eq!(u16::from_le_bytes([bytes[22], bytes[23]]), 6u16);

        // dt_ms at [24,25] = 0
        assert_eq!(u16::from_le_bytes([bytes[24], bytes[25]]), 0u16);

        // value at [26..30]
        let parsed_val = f32::from_le_bytes([bytes[26], bytes[27], bytes[28], bytes[29]]);
        assert!((parsed_val - value).abs() < f32::EPSILON);

        // CRC32C at [30..34] — [TODO-1 resolved]
        assert_eq!(bytes.len(), 34);
        assert_eq!(DataFrame::verify_crc(&bytes), true);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-009
    /// Data frame with corrupted magic returns None from from_ble_bytes
    #[test]
    fn test_data_frame_bad_magic() {
        let mut bytes = DataFrame::new(1, 1, 1, 0, 65.0).to_ble_bytes();
        bytes[0] = 0xFF; // corrupt magic
        assert!(DataFrame::from_ble_bytes(&bytes).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-010
    #[test]
    fn test_data_frame_roundtrip() {
        let original = DataFrame::new(3, 2, 42, 1_700_000_000_123, 98.6);
        let bytes = original.to_ble_bytes();
        let parsed = DataFrame::from_ble_bytes(&bytes).unwrap();
        assert_eq!(parsed.header.session_id, 3);
        assert_eq!(parsed.header.stream_id, 2);
        assert_eq!(parsed.header.seq, 42);
        assert_eq!(parsed.t0_ms, 1_700_000_000_123);
        assert!((parsed.value - 98.6f32).abs() < f32::EPSILON);
    }

    // ── AckFrame tests ────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-011
    /// ACK_FRAME total wire length is 30 bytes (IDT header + payload + CRC32C)
    #[test]
    fn test_ack_frame_total_len() {
        assert_eq!(AckFrame::TOTAL_LEN, 30);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-012
    #[test]
    fn test_ack_frame_parse() {
        let bytes = make_ack_frame_bytes(1, 2, 99);
        let ack = AckFrame::from_ble_bytes(&bytes).unwrap();
        assert_eq!(ack.session_id, 1);
        assert_eq!(ack.stream_id, 2);
        assert_eq!(ack.ack_upto, 99);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-013
    /// ACK_FRAME that is too short returns None
    #[test]
    fn test_ack_frame_too_short() {
        let bytes = vec![0u8; AckFrame::TOTAL_LEN - 1]; // 29 bytes, need 30
        assert!(AckFrame::from_ble_bytes(&bytes).is_none());
    }

    // ── NackFrame tests ───────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-014
    #[test]
    fn test_nack_frame_parse() {
        let seqs = [3u32, 7u32];
        let bytes = make_nack_frame_bytes(1, 2, 2, &seqs);
        let nack = NackFrame::from_ble_bytes(&bytes).unwrap();
        assert_eq!(nack.reason, 2);
        assert_eq!(nack.seq_list, vec![3, 7]);
        assert_eq!(nack.header.stream_id, 2);
    }

    // ── SubscribeReq tests ────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-015
    #[test]
    fn test_subscribe_req_parse_subscribe() {
        let items = [(1u8, 0x0101u16)]; // source=1, HR
        let bytes = make_subscribe_req_bytes(1, 42, SUB_OP_SUBSCRIBE, &items);
        let req = SubscribeReq::from_ble_bytes(&bytes).unwrap();
        assert_eq!(req.req_id, 42);
        assert_eq!(req.op, SUB_OP_SUBSCRIBE);
        assert_eq!(req.items.len(), 1);
        assert_eq!(req.items[0].source_id, 1);
        assert_eq!(req.items[0].signal_id, 0x0101);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-016
    #[test]
    fn test_subscribe_req_parse_unsubscribe() {
        let items = [(1u8, 0x0102u16)]; // source=1, SpO2
        let bytes = make_subscribe_req_bytes(1, 7, SUB_OP_UNSUBSCRIBE, &items);
        let req = SubscribeReq::from_ble_bytes(&bytes).unwrap();
        assert_eq!(req.op, SUB_OP_UNSUBSCRIBE);
        assert_eq!(req.items[0].signal_id, 0x0102);
    }

    // ── SubscribeRsp tests ────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-017
    #[test]
    fn test_subscribe_rsp_bytes() {
        let rsp = SubscribeRsp {
            session_id: 1,
            req_id: 42,
            status: 0,
            results: vec![SubscribeRspItem {
                source_id: 1,
                signal_id: 0x0101,
                stream_id: 1,
                effective_period_ms: 1000,
                effective_batch_max: 1,
            }],
        };
        let bytes = rsp.to_ble_bytes();
        // 13-byte compact header: magic ✓, msg_type=0x02 at [3], session_id at [5-6]
        assert_eq!(u16::from_le_bytes([bytes[0], bytes[1]]), IDT_MAGIC);
        assert_eq!(bytes[3], MSG_SUBSCRIBE_RSP);
        // Size = header(13) + req_id(2)+status(1)+n(1) + result(10) + crc(4) = 31
        assert_eq!(bytes.len(), 13 + 4 + 10 + 4);
    }

    // ── Catalog tests ─────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-018
    #[test]
    fn test_catalog_default_medical() {
        let catalog = Catalog::default_medical_catalog();
        assert_eq!(catalog.entries.len(), 7);
        assert_eq!(catalog.entries[0].signal_id, 0x0101);
        assert_eq!(catalog.entries[1].signal_id, 0x0102);
        assert_eq!(catalog.entries[2].signal_id, 0x0103);
        assert_eq!(catalog.entries[3].signal_id, 0x0201);
        assert_eq!(catalog.entries[4].signal_id, 0x0202);
        assert_eq!(catalog.entries[5].signal_id, 0x0203);
        assert_eq!(catalog.entries[6].signal_id, 0x0501);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-019
    #[test]
    fn test_catalog_to_ble_bytes() {
        let catalog = Catalog::default_medical_catalog();
        let bytes = catalog.to_ble_bytes();
        assert!(!bytes.is_empty());
        // First entry (HR) — spec v1 layout (p.20):
        // source_id(1) | signal_id(2 LE) | value_type(1) | unit_code(1) | period_ms(4 LE) | name_len(1) | name(N)
        assert_eq!(bytes[0], 1u8); // source_id = 1 (scope)
        assert_eq!(bytes[1], 0x01); // signal_id low byte  (0x0101 LE)
        assert_eq!(bytes[2], 0x01); // signal_id high byte
        assert_eq!(bytes[3], VALUE_TYPE_FLOAT32); // value_type = 3
        assert_eq!(bytes[4], UNIT_BPM); // unit_code = 1 (bpm)
        assert_eq!(u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]), 1000); // period_ms
        let name_len = bytes[9] as usize;
        assert_eq!(&bytes[10..10 + name_len], b"HR");
    }

    // ── InboundFrame dispatch tests ───────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-020
    /// IDT ACK_FRAME is routed to InboundFrame::Ack
    #[test]
    fn test_inbound_frame_dispatch_ack() {
        let bytes = make_ack_frame_bytes(1, 1, 5);
        match InboundFrame::from_ble_bytes(&bytes) {
            Some(InboundFrame::Ack(ack)) => {
                assert_eq!(ack.session_id, 1);
                assert_eq!(ack.stream_id, 1);
                assert_eq!(ack.ack_upto, 5);
            }
            _ => panic!("Expected InboundFrame::Ack"),
        }
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-021
    #[test]
    fn test_inbound_frame_dispatch_nack() {
        let bytes = make_nack_frame_bytes(1, 1, 2, &[3, 5]);
        match InboundFrame::from_ble_bytes(&bytes) {
            Some(InboundFrame::Nack(nack)) => assert_eq!(nack.seq_list, vec![3, 5]),
            _ => panic!("Expected InboundFrame::Nack"),
        }
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-022
    #[test]
    fn test_inbound_frame_dispatch_subscribe_req() {
        let bytes = make_subscribe_req_bytes(1, 1, SUB_OP_SUBSCRIBE, &[(1, 0x0102)]);
        match InboundFrame::from_ble_bytes(&bytes) {
            Some(InboundFrame::SubscribeReq(req)) => {
                assert_eq!(req.items[0].signal_id, 0x0102)
            }
            _ => panic!("Expected InboundFrame::SubscribeReq"),
        }
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-023
    #[test]
    fn test_inbound_frame_unknown_type() {
        // Build a buffer with valid magic but unknown msg_type at byte[3]
        // Use a 35-byte buffer with magic at start
        let mut bytes = vec![0u8; 35];
        bytes[0] = 0x7A; // magic LE
        bytes[1] = 0xD1;
        bytes[3] = 0xFF; // unknown msg_type
        assert!(InboundFrame::from_ble_bytes(&bytes).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-024
    /// A buffer shorter than IdtHeader::SIZE returns None from InboundFrame
    #[test]
    fn test_inbound_frame_too_short_for_ack() {
        // Any buffer shorter than the 13-byte IDT header returns None
        let bytes = vec![0x7Au8, 0xD1, 0x01]; // IDT magic + 1 byte, too short for header
        assert!(InboundFrame::from_ble_bytes(&bytes).is_none());
    }

    // ── AckFrame::is_acked tests ──────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-025
    /// is_acked returns true for seq ≤ ack_upto (cumulative path)
    #[test]
    fn test_ack_frame_is_acked_cumulative() {
        let ack = AckFrame {
            session_id: 1,
            stream_id: 1,
            ack_upto: 10,
            bitmap: [0u8; 8],
        };
        assert!(ack.is_acked(1));
        assert!(ack.is_acked(10));
        assert!(!ack.is_acked(11));
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-026
    /// is_acked returns true for seq covered by a set bitmap bit (selective ACK path)
    #[test]
    fn test_ack_frame_is_acked_bitmap() {
        // ack_upto = 5; bitmap bit0 = seq 6 received, bit2 = seq 8 received
        let mut bitmap = [0u8; 8];
        bitmap[0] = 0b0000_0101; // bits 0 and 2 set → seq 6 and 8 acknowledged
        let ack = AckFrame {
            session_id: 1,
            stream_id: 1,
            ack_upto: 5,
            bitmap,
        };
        assert!(ack.is_acked(6)); // bit0 set
        assert!(!ack.is_acked(7)); // bit1 clear → not acked
        assert!(ack.is_acked(8)); // bit2 set
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-027
    /// is_acked returns false for seq beyond the 64-bit bitmap window
    #[test]
    fn test_ack_frame_is_acked_beyond_window() {
        let ack = AckFrame {
            session_id: 1,
            stream_id: 1,
            ack_upto: 0,
            bitmap: [0xFF; 8], // all 64 bits set → seq 1..64 acked
        };
        assert!(ack.is_acked(64)); // last bit in window
        assert!(!ack.is_acked(65)); // one beyond window
    }

    // ── AckFrame bitmap parsing ───────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-028
    /// AckFrame::from_ble_bytes correctly reads an 8-byte non-zero bitmap
    #[test]
    fn test_ack_frame_parse_with_bitmap() {
        let mut buf = make_ack_frame_bytes(2, 3, 7);
        // Bitmap starts at byte 18 in IDT format (after 13b header + 4b ack_upto + 1b bitmap_len)
        // Recompute CRC after modifying the bitmap byte
        buf[18] = 0x01; // bit0 set → seq (ack_upto+1+0) = seq 8 received
        let crc = crc32c::crc32c(&buf[..AckFrame::TOTAL_LEN - 4]);
        let crc_bytes = crc.to_le_bytes();
        let len = buf.len();
        buf[len - 4..].copy_from_slice(&crc_bytes);
        let ack = AckFrame::from_ble_bytes(&buf).unwrap();
        assert_eq!(ack.ack_upto, 7);
        assert_eq!(ack.bitmap[0], 0x01);
        assert!(ack.is_acked(8)); // bit0 of bitmap → seq 8
        assert!(!ack.is_acked(9)); // bit1 clear
    }

    // ── IdtHeader / DataFrame length guards ───────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-029
    /// IdtHeader::from_bytes returns None if buffer is shorter than 13 bytes
    #[test]
    fn test_idt_header_too_short() {
        let b = vec![0x7A, 0xD1, 0x01]; // only 3 bytes
        assert!(IdtHeader::from_bytes(&b).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-030
    /// DataFrame::from_ble_bytes returns None if buffer is shorter than TOTAL_LEN (34) bytes
    #[test]
    fn test_data_frame_too_short() {
        let b = vec![0u8; DataFrame::TOTAL_LEN - 1]; // 33 bytes
        assert!(DataFrame::from_ble_bytes(&b).is_none());
    }

    // ── NackFrame CRC guard ───────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-031
    /// NackFrame::from_ble_bytes returns None when the CRC is corrupted
    #[test]
    fn test_nack_frame_bad_crc() {
        let mut bytes = make_nack_frame_bytes(1, 1, 2, &[5]);
        // Flip the last byte of the CRC
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(NackFrame::from_ble_bytes(&bytes).is_none());
    }

    // ── Additional coverage tests ──────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-035
    /// InboundFrame::from_ble_bytes returns None for buffers shorter than IdtHeader::SIZE
    #[test]
    fn test_inbound_frame_single_byte_returns_none() {
        assert!(InboundFrame::from_ble_bytes(&[]).is_none());
        assert!(InboundFrame::from_ble_bytes(&[0x7A]).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-036
    /// InboundFrame returns None when IDT magic matches but buffer is shorter than IdtHeader::SIZE
    #[test]
    fn test_inbound_frame_idt_magic_too_short() {
        // 4 bytes: magic (0x7A 0xD1) + two padding bytes — length < IdtHeader::SIZE (13)
        let bytes = vec![0x7Au8, 0xD1, 0x01, 0x21]; // IDT_MAGIC LE + partial header
        assert!(InboundFrame::from_ble_bytes(&bytes).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-037
    /// SubscribeReq::from_ble_bytes with n=0 items parses successfully and returns empty items vec
    #[test]
    fn test_subscribe_req_parse_zero_items() {
        let buf = make_subscribe_req_bytes(1, 7, SUB_OP_SUBSCRIBE, &[]);
        match SubscribeReq::from_ble_bytes(&buf) {
            Some(req) => {
                assert_eq!(req.op, SUB_OP_SUBSCRIBE);
                assert!(req.items.is_empty(), "n=0 must yield empty items vec");
            }
            None => panic!("Expected Some(SubscribeReq) with n=0"),
        }
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-038
    /// SubscribeReq::from_ble_bytes returns None when CRC is corrupted (no valid stride found)
    #[test]
    fn test_subscribe_req_bad_crc_returns_none() {
        let mut buf =
            make_subscribe_req_bytes(1, 1, SUB_OP_SUBSCRIBE, &[(1, SignalId::HR.as_u16())]);
        // Corrupt last CRC byte so detect_item_stride finds no valid stride
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert!(SubscribeReq::from_ble_bytes(&buf).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-040
    /// NackFrame::from_ble_bytes with n=0 (empty seq_list) parses cleanly
    #[test]
    fn test_nack_frame_zero_seqs() {
        let buf = make_nack_frame_bytes(1, 2, 2, &[]); // n=0, reason=MISSING
        let frame = NackFrame::from_ble_bytes(&buf).expect("n=0 NackFrame must parse");
        assert_eq!(frame.reason, 2);
        assert!(frame.seq_list.is_empty());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-041
    /// AckFrame::is_acked correctly checks bitmap bits in bytes beyond byte 0 (offsets 8–15)
    #[test]
    fn test_ack_frame_is_acked_bitmap_high_bytes() {
        // ack_upto=0; set bit 8 (byte1, bit0) → seq 9 received
        // and bit 15 (byte1, bit7) → seq 16 received
        let mut bitmap = [0u8; 8];
        bitmap[1] = 0b1000_0001; // bit8 (offset=8) and bit15 (offset=15) set
        let ack = AckFrame {
            session_id: 1,
            stream_id: 1,
            ack_upto: 0,
            bitmap,
        };
        assert!(ack.is_acked(9), "bit 8 in byte1 → seq 9 must be acked");
        assert!(ack.is_acked(16), "bit 15 in byte1 → seq 16 must be acked");
        assert!(!ack.is_acked(10), "bit 9 clear → seq 10 not acked");
    }

    // ── DataFrame CRC32C tests ─────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-042
    /// to_ble_bytes() appends CRC32C of the first 30 bytes at positions [30..34]
    #[test]
    fn test_dataframe_crc_appended() {
        let frame = DataFrame::new(1, 1, 1, 0, 65.0);
        let bytes = frame.to_ble_bytes();
        assert_eq!(bytes.len(), 34);
        let expected = crc32c::crc32c(&bytes[..30]);
        let actual = u32::from_le_bytes([bytes[30], bytes[31], bytes[32], bytes[33]]);
        assert_eq!(
            actual, expected,
            "CRC32C at [30..34] must equal crc32c(bytes[0..30])"
        );
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-043
    /// verify_crc returns true for a valid frame and false after any byte is corrupted
    #[test]
    fn test_dataframe_verify_crc_pass_and_fail() {
        let bytes = DataFrame::new(1, 1, 1, 0, 65.0).to_ble_bytes();
        assert!(
            DataFrame::verify_crc(&bytes),
            "valid frame must pass CRC check"
        );
        // Corrupt a byte in the header
        let mut corrupted = bytes.clone();
        corrupted[5] ^= 0xFF;
        assert!(
            !DataFrame::verify_crc(&corrupted),
            "corrupted frame must fail CRC check"
        );
        // Too-short buffer must fail
        assert!(
            !DataFrame::verify_crc(&bytes[..33]),
            "short buffer must fail CRC check"
        );
    }

    // ── has_idt_magic tests ────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-044
    /// has_idt_magic returns true only for buffers starting with 0x7A 0xD1 (IDT_MAGIC LE)
    #[test]
    fn test_has_idt_magic() {
        // Valid magic (0xD17A LE = [0x7A, 0xD1, ...])
        assert!(has_idt_magic(&[0x7A, 0xD1, 0x00]));
        assert!(has_idt_magic(&[0x7A, 0xD1]));
        // Wrong magic
        assert!(!has_idt_magic(&[0x20, 0x00]));
        assert!(!has_idt_magic(&[0xD1, 0x7A])); // bytes swapped (big-endian) — rejected
                                                // Too short
        assert!(!has_idt_magic(&[]));
        assert!(!has_idt_magic(&[0x7A]));
    }

    // ── SignalRegistry tests ───────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-045
    /// SignalRegistry::with_defaults registers exactly the three V1 medical signals
    #[test]
    fn test_signal_registry_default_has_three_signals() {
        let r = SignalRegistry::with_defaults();
        assert!(r.get(0x0101).is_some(), "HR must be registered");
        assert!(r.get(0x0102).is_some(), "SpO2 must be registered");
        assert!(r.get(0x0103).is_some(), "Temperature must be registered");
        assert!(r.get(0x0201).is_some(), "SBP must be registered");
        assert!(r.get(0x0202).is_some(), "DBP must be registered");
        assert!(r.get(0x0203).is_some(), "MBP must be registered");
        assert!(r.get(0x0501).is_some(), "AmbPres must be registered");
        assert_eq!(r.all_signal_ids().len(), 7);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-046
    /// Registering an extra signal is reflected in all_signal_ids and build_catalog
    #[test]
    fn test_signal_registry_register_extra_signal() {
        let mut r = SignalRegistry::with_defaults();
        r.register(SignalMeta {
            signal_id: 0x0204,
            source_id: 2,
            name: "IBP_SBP".to_string(),
            value_type: VALUE_TYPE_FLOAT32,
            unit: "mmHg".to_string(),
            sample_kind: 0,
            nominal_period_ms: 1000,
        });
        assert_eq!(r.all_signal_ids().len(), 8);
        assert!(r.get(0x0204).is_some());
        let catalog = r.build_catalog();
        assert_eq!(catalog.entries.len(), 8);
        // Sorted by signal_id: 0x0101, 0x0102, 0x0103, 0x0201, 0x0202, 0x0203, 0x0204, 0x0501
        assert_eq!(catalog.entries[6].signal_id, 0x0204);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-047
    /// normalize_id resolves legacy 1/2/3 to IDT compound IDs and unknown IDs to None
    #[test]
    fn test_signal_registry_normalize_legacy_ids() {
        let r = SignalRegistry::with_defaults();
        assert_eq!(r.normalize_id(1), Some(0x0101));
        assert_eq!(r.normalize_id(2), Some(0x0102));
        assert_eq!(r.normalize_id(3), Some(0x0103));
        assert_eq!(r.normalize_id(0x0201), Some(0x0201)); // SBP canonical
        assert_eq!(r.normalize_id(0x0202), Some(0x0202)); // DBP canonical
        assert_eq!(r.normalize_id(0x0203), Some(0x0203)); // MBP canonical
        assert_eq!(r.normalize_id(0x0501), Some(0x0501)); // AmbPres canonical
        assert_eq!(r.normalize_id(0x0101), Some(0x0101)); // HR canonical — direct hit
        assert_eq!(r.normalize_id(0x9999), None); // unknown
        assert_eq!(r.normalize_id(0), None); // zero — rejected
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-048
    /// build_catalog from registry matches Catalog::default_medical_catalog for the three defaults
    #[test]
    fn test_signal_registry_build_catalog_matches_default_medical() {
        let r = SignalRegistry::with_defaults();
        let from_registry = r.build_catalog();
        let hardcoded = Catalog::default_medical_catalog();
        assert_eq!(from_registry.entries.len(), hardcoded.entries.len());
        for (a, b) in from_registry.entries.iter().zip(hardcoded.entries.iter()) {
            assert_eq!(a.signal_id, b.signal_id);
            assert_eq!(a.name, b.name);
            assert_eq!(a.nominal_period_ms, b.nominal_period_ms);
            assert_eq!(a.unit, b.unit);
        }
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-049
    /// contains_normalized returns true for all resolvable IDs (canonical + legacy) and false
    /// for unknown/zero IDs
    #[test]
    fn test_signal_registry_contains_normalized() {
        let r = SignalRegistry::with_defaults();
        // Canonical IDT compound IDs — direct HashMap hit
        assert!(r.contains_normalized(0x0101), "HR canonical must resolve");
        assert!(r.contains_normalized(0x0102), "SpO2 canonical must resolve");
        assert!(r.contains_normalized(0x0103), "Temp canonical must resolve");
        assert!(r.contains_normalized(0x0201), "SBP canonical must resolve");
        assert!(r.contains_normalized(0x0202), "DBP canonical must resolve");
        assert!(r.contains_normalized(0x0203), "MBP canonical must resolve");
        assert!(
            r.contains_normalized(0x0501),
            "AmbPres canonical must resolve"
        );
        // Legacy simple IDs — SignalId fallback + filter (0x01xx only)
        assert!(r.contains_normalized(1), "legacy HR id=1 must resolve");
        assert!(r.contains_normalized(2), "legacy SpO2 id=2 must resolve");
        assert!(r.contains_normalized(3), "legacy Temp id=3 must resolve");
        // Unknown IDs must not resolve
        assert!(!r.contains_normalized(0), "zero must not resolve");
        assert!(
            !r.contains_normalized(0x9999),
            "unknown IDT ID must not resolve"
        );
        assert!(
            !r.contains_normalized(99),
            "unknown legacy simple ID must not resolve"
        );
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-050
    /// SignalRegistry::new() creates an empty registry; all lookups return None/false/empty
    #[test]
    fn test_signal_registry_new_is_empty() {
        let r = SignalRegistry::new();
        assert!(
            r.all_signal_ids().is_empty(),
            "fresh registry must have no signal IDs"
        );
        assert!(
            r.get(0x0101).is_none(),
            "get on empty registry must be None"
        );
        // normalize_id falls back to SignalId enum, but the .filter() gates on the registry
        // — so even legacy IDs return None when the registry has no entries
        assert_eq!(
            r.normalize_id(1),
            None,
            "legacy id=1 must not resolve in empty registry"
        );
        assert!(
            !r.contains_normalized(1),
            "contains_normalized must be false in empty registry"
        );
        assert_eq!(
            r.build_catalog().entries.len(),
            0,
            "catalog from empty registry must be empty"
        );
    }

    // ── parse_tlv_subscribe_req ───────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-020
    /// parse_tlv_subscribe_req parses the exact 89-byte payload captured from the Flutter app
    #[test]
    fn test_parse_tlv_subscribe_req_real_flutter_bytes() {
        let bytes: Vec<u8> = vec![
            0x20, 0x56, 0x00, 0x01, 0x02, 0x00, 0x2A, 0x00, // header (8b)
            0x03, 0x18, 0x00, // item 1 tag+len
            0x01, 0x01, 0x00, 0x01, // nested TLV: source_id=1
            0x02, 0x02, 0x00, 0x01, 0x00, // nested TLV: signal_id=1 (HR)
            0x03, 0x01, 0x00, 0x00, // nested TLV: mode=0
            0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, // nested TLV: period_ms=0
            0x05, 0x01, 0x00, 0x01, // nested TLV: batch_max=1
            0x03, 0x18, 0x00, // item 2 tag+len
            0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x02, 0x00, // signal_id=2 (SpO2)
            0x03, 0x01, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00,
            0x01, 0x03, 0x18, 0x00, // item 3 tag+len
            0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x03, 0x00, // signal_id=3 (Temperature)
            0x03, 0x01, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00,
            0x01,
        ];
        assert_eq!(bytes.len(), 89);
        let (req_id, signal_ids) = parse_tlv_subscribe_req(&bytes).unwrap();
        assert_eq!(req_id, 42);
        assert_eq!(signal_ids, vec![1u16, 2u16, 3u16]);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-021
    /// parse_tlv_subscribe_req returns None for wrong marker byte
    #[test]
    fn test_parse_tlv_subscribe_req_wrong_marker() {
        let mut bytes = vec![0u8; 8 + 27];
        bytes[0] = 0x7A; // not 0x20
        assert!(parse_tlv_subscribe_req(&bytes).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-023
    /// parse_idt_wrapped_tlv_subscribe_req accepts a MyPredi TLV SUBSCRIBE_REQ wrapped in an IDT envelope.
    #[test]
    fn test_parse_idt_wrapped_tlv_subscribe_req() {
        let mut bytes = vec![0u8; 24];
        bytes[0..2].copy_from_slice(&IDT_MAGIC.to_le_bytes());
        bytes[2] = IDT_VERSION;
        bytes[3] = MSG_SUBSCRIBE_REQ;
        bytes.extend_from_slice(&[
            0x20, 0x56, 0x00, 0x01, 0x02, 0x00, 0x2A, 0x00, // tlv header
            0x03, 0x18, 0x00, 0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x01, 0x00, 0x03, 0x01,
            0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01, 0x03,
            0x18, 0x00, 0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x02, 0x00, 0x03, 0x01, 0x00,
            0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01, 0x03, 0x18,
            0x00, 0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x03, 0x00, 0x03, 0x01, 0x00, 0x00,
            0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01,
        ]);
        bytes.extend_from_slice(&[0u8; 4]);

        assert_eq!(
            parse_idt_wrapped_tlv_subscribe_req(&bytes),
            Some((0x002A, vec![1u16, 2u16, 3u16]))
        );
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-022
    /// parse_tlv_subscribe_req returns None for a buffer that is too short
    #[test]
    fn test_parse_tlv_subscribe_req_too_short() {
        let bytes = vec![0x20u8; 30]; // 8 + 22 < 8 + 27
        assert!(parse_tlv_subscribe_req(&bytes).is_none());
    }
}
