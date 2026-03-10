// /src/domain/ble_protocol.rs
// Module: domain.ble_protocol
// Purpose: IDT ("ICU Data Transport") BLE binary protocol — V2.0
//          Full IDT frame format per "Proposition de protocole BLE.pdf"
//
// All frames: [Header(15b) | Payload(Nb) | CRC32C(4b)]
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
pub const MSG_DATA_FRAME: u8 = 0x10;
pub const MSG_ACK_FRAME: u8 = 0x20;
pub const MSG_NACK_FRAME: u8 = 0x21;

// flags bits
pub const FLAG_BACKLOG: u8 = 0b0000_0001;
pub const FLAG_RETRANSMIT: u8 = 0b0000_0010;

// value_type codes (used in Catalog)
pub const VALUE_TYPE_FLOAT32: u8 = 3;
pub const VALUE_TYPE_UINT16: u8 = 6;

// unit_code values
pub const UNIT_BPM: u8 = 1;
pub const UNIT_PCT: u8 = 2;
pub const UNIT_MMHG: u8 = 3;
pub const UNIT_DEGC: u8 = 4;

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
            // Legacy simple IDs (spec "I.pdf" / some Flutter client implementations)
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
        }
    }

    pub fn nominal_period_ms(self) -> u32 {
        match self {
            SignalId::HR => 1000,
            SignalId::SpO2 => 1000,
            SignalId::Temperature => 2000,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IdtHeader — 15-byte common header for all IDT frames
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-003
/// IDT frame header — exactly 15 bytes, little-endian
///
/// Byte layout:
/// [0..2]   magic       u16 LE  = 0xD17A
/// [2]      version     u8      = 0x01
/// [3]      msg_type    u8
/// [4]      flags       u8
/// [5..7]   session_id  u16 LE
/// [7..9]   stream_id   u16 LE
/// [9..13]  seq         u32 LE
/// [13..15] payload_len u16 LE
#[derive(Debug, Clone, PartialEq)]
pub struct IdtHeader {
    pub magic: u16,
    pub version: u8,
    pub msg_type: u8,
    pub flags: u8,
    pub session_id: u16,
    pub stream_id: u16,
    pub seq: u32,
    pub payload_len: u16,
}

impl IdtHeader {
    pub const SIZE: usize = 15;

    pub fn new_data(session_id: u16, stream_id: u16, seq: u32, payload_len: u16) -> Self {
        Self {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_DATA_FRAME,
            flags: 0,
            session_id,
            stream_id,
            seq,
            payload_len,
        }
    }

    /// Serialize to exactly 15 bytes
    pub fn to_bytes(&self) -> [u8; 15] {
        let mut b = [0u8; 15];
        b[0..2].copy_from_slice(&self.magic.to_le_bytes());
        b[2] = self.version;
        b[3] = self.msg_type;
        b[4] = self.flags;
        b[5..7].copy_from_slice(&self.session_id.to_le_bytes());
        b[7..9].copy_from_slice(&self.stream_id.to_le_bytes());
        b[9..13].copy_from_slice(&self.seq.to_le_bytes());
        b[13..15].copy_from_slice(&self.payload_len.to_le_bytes());
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
            payload_len: u16::from_le_bytes([b[13], b[14]]),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataFrame — IDT DATA_FRAME (msg_type=0x10), count=1, float32
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-004
/// IDT DATA_FRAME for a single float32 sample (count=1).
///
/// Wire format (34 bytes total):
/// [Header(15b)] [t0_ms(8b)] [count=1(1b)] [dt_ms=0(2b)] [value(4b)] [CRC32C(4b)]
///
/// Payload = 15 bytes: t0_ms(8) + count(1) + dt_ms[0](2) + value(4)
/// CRC32C computed over bytes [0..30] (header + payload, excluding the 4 CRC bytes)
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrame {
    pub header: IdtHeader,
    pub t0_ms: u64,
    pub value: f32,
}

impl DataFrame {
    /// Payload size in bytes for count=1, float32
    pub const PAYLOAD_LEN: usize = 15; // t0_ms(8) + count(1) + dt_ms(2) + value(4)
    /// Total frame size: header(15) + payload(15) + CRC32C(4)
    pub const TOTAL_LEN: usize = 34;

    pub fn new(session_id: u16, stream_id: u16, seq: u32, t0_ms: u64, value: f32) -> Self {
        Self {
            header: IdtHeader::new_data(
                session_id,
                stream_id,
                seq,
                Self::PAYLOAD_LEN as u16,
            ),
            t0_ms,
            value,
        }
    }

    /// Serialize to 34 bytes. CRC32C covers bytes [0..30].
    pub fn to_ble_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::TOTAL_LEN);
        buf.extend_from_slice(&self.header.to_bytes()); // 15 bytes
        buf.extend_from_slice(&self.t0_ms.to_le_bytes()); // 8 bytes
        buf.push(1u8); // count = 1
        buf.extend_from_slice(&0u16.to_le_bytes()); // dt_ms[0] = 0
        buf.extend_from_slice(&self.value.to_le_bytes()); // 4 bytes
        // Append CRC32C over all bytes so far
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Deserialize from bytes. Returns None on length/magic/CRC mismatch.
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < Self::TOTAL_LEN {
            return None;
        }
        // Verify CRC32C over bytes [0..30]
        let expected_crc = crc32c::crc32c(&b[..Self::TOTAL_LEN - 4]);
        let actual_crc = u32::from_le_bytes([b[30], b[31], b[32], b[33]]);
        if expected_crc != actual_crc {
            return None;
        }
        let header = IdtHeader::from_bytes(&b[0..IdtHeader::SIZE])?;
        if header.msg_type != MSG_DATA_FRAME {
            return None;
        }
        let t0_ms = u64::from_le_bytes([
            b[15], b[16], b[17], b[18], b[19], b[20], b[21], b[22],
        ]);
        // b[23] = count (should be 1)
        // b[24..26] = dt_ms[0] (should be 0)
        let value = f32::from_le_bytes([b[26], b[27], b[28], b[29]]);
        Some(Self { header, t0_ms, value })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AckFrame — IDT ACK_FRAME (msg_type=0x20), cumulative acknowledgment
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-005
/// Cumulative ACK from the BLE Central (client).
///
/// Wire format (23 bytes total):
/// [Header(15b)] [ack_upto(4b)] [CRC32C(4b)]
#[derive(Debug, Clone, PartialEq)]
pub struct AckFrame {
    pub header: IdtHeader,
    pub ack_upto: u32,
}

impl AckFrame {
    pub const PAYLOAD_LEN: usize = 4;
    pub const TOTAL_LEN: usize = 23; // header(15) + payload(4) + CRC(4)

    /// Deserialize from bytes received on Data_IN characteristic.
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < Self::TOTAL_LEN {
            return None;
        }
        // Verify CRC32C over bytes [0..19]
        let expected_crc = crc32c::crc32c(&b[..Self::TOTAL_LEN - 4]);
        let actual_crc = u32::from_le_bytes([b[19], b[20], b[21], b[22]]);
        if expected_crc != actual_crc {
            return None;
        }
        let header = IdtHeader::from_bytes(&b[0..IdtHeader::SIZE])?;
        if header.msg_type != MSG_ACK_FRAME {
            return None;
        }
        let ack_upto = u32::from_le_bytes([b[15], b[16], b[17], b[18]]);
        Some(Self { header, ack_upto })
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
            seq_list.push(u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]));
        }
        Some(Self { header, reason, seq_list })
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
    /// This handles Flutter app implementations that use a 23-byte item layout.
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
        // Standard size (17) is tried first; 23 is tried second (known Flutter variant).
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
        Some(Self { header, req_id, op, items })
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
            let got = u32::from_le_bytes([b[crc_off], b[crc_off+1], b[crc_off+2], b[crc_off+3]]);
            return if exp == got { Some(0) } else { None };
        }

        // Priority order: standard first, then the known 23-byte Flutter variant
        for &candidate in &[SubscribeItem::SIZE, 23usize, 18, 19, 20, 21, 22, 24, 25, 26, 27, 28, 29, 30] {
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
    /// Format: [Header(15b)] [req_id(2b)] [status(1b)] [n(1b)] [results(n×10b)] [CRC32C(4b)]
    pub fn to_ble_bytes(&self) -> Vec<u8> {
        let n = self.results.len();
        let payload_len = 4 + n * SubscribeRspItem::SIZE; // req_id(2)+status(1)+n(1)+results
        let total = IdtHeader::SIZE + payload_len + 4;
        let mut buf = Vec::with_capacity(total);

        // Build header
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_SUBSCRIBE_RSP,
            flags: 0,
            session_id: self.session_id,
            stream_id: 0,
            seq: 0,
            payload_len: payload_len as u16,
        };
        buf.extend_from_slice(&header.to_bytes());

        // Build payload
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

        // Append CRC32C
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Catalog — IDT signal catalog (TLV binary format)
// ─────────────────────────────────────────────────────────────────────────────

/// One entry in the Catalog characteristic (TLV format, variable length)
///
/// Byte layout per entry:
/// [0]    source_id          u8
/// [1..3] signal_id          u16 LE
/// [3]    value_type         u8  (3=float32, 6=uint16)
/// [4]    unit_code          u8  (1=bpm, 2=%, 3=mmHg, 4=°C)
/// [5..9] nominal_period_ms  u32 LE
/// [9]    name_len           u8
/// [10..] name               UTF-8 bytes
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogEntry {
    pub source_id: u8,
    pub signal_id: u16,
    pub value_type: u8,
    pub unit_code: u8,
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
    /// Default medical catalog: HR, SpO2, Temperature (all float32, source=scope)
    pub fn default_medical_catalog() -> Self {
        Self {
            entries: vec![
                CatalogEntry {
                    source_id: SignalId::HR.source_id(),
                    signal_id: SignalId::HR.as_u16(),
                    value_type: SignalId::HR.value_type(),
                    unit_code: SignalId::HR.unit_code(),
                    nominal_period_ms: SignalId::HR.nominal_period_ms(),
                    name: SignalId::HR.name().to_string(),
                },
                CatalogEntry {
                    source_id: SignalId::SpO2.source_id(),
                    signal_id: SignalId::SpO2.as_u16(),
                    value_type: SignalId::SpO2.value_type(),
                    unit_code: SignalId::SpO2.unit_code(),
                    nominal_period_ms: SignalId::SpO2.nominal_period_ms(),
                    name: SignalId::SpO2.name().to_string(),
                },
                CatalogEntry {
                    source_id: SignalId::Temperature.source_id(),
                    signal_id: SignalId::Temperature.as_u16(),
                    value_type: SignalId::Temperature.value_type(),
                    unit_code: SignalId::Temperature.unit_code(),
                    nominal_period_ms: SignalId::Temperature.nominal_period_ms(),
                    name: SignalId::Temperature.name().to_string(),
                },
            ],
        }
    }

    /// Serialize to TLV binary for the Catalog GATT characteristic (Read).
    pub fn to_ble_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for e in &self.entries {
            buf.push(e.source_id);
            buf.extend_from_slice(&e.signal_id.to_le_bytes());
            buf.push(e.value_type);
            buf.push(e.unit_code);
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
    /// Parse an inbound frame. Dispatches on byte[3] = msg_type.
    pub fn from_ble_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < IdtHeader::SIZE {
            return None;
        }
        // Verify magic first (bytes 0..2)
        let magic = u16::from_le_bytes([b[0], b[1]]);
        if magic != IDT_MAGIC {
            return None;
        }
        let msg_type = b[3];
        match msg_type {
            MSG_ACK_FRAME => AckFrame::from_ble_bytes(b).map(InboundFrame::Ack),
            MSG_NACK_FRAME => NackFrame::from_ble_bytes(b).map(InboundFrame::Nack),
            MSG_SUBSCRIBE_REQ => SubscribeReq::from_ble_bytes(b).map(InboundFrame::SubscribeReq),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TLV Subscribe parser — Flutter app wire format (non-IDT)
// ─────────────────────────────────────────────────────────────────────────────

/// ID SRS: SRS-MOD-BLEPROTOCOL-011
/// Parse the Flutter app's TLV-based SUBSCRIBE_REQ (non-IDT format).
///
/// The app sends a completely different binary format from IDT:
///   - No IDT magic (0x20 at byte[0] instead of 0xD17A)
///   - 12-byte proprietary header; bytes[6..8] = req_id (LE u16)
///   - No CRC32C footer
///   - Items are TLV triplets: tag(1b)=0x03 | len_u16_le(2b)=24 | nested_tlvs(24b)
///   - Nested sub-TLVs (tag=01→source_id, tag=02→signal_id LE u16, tag=03→mode, ...)
///   - signal_id located at item_base + 10 (LE u16, legacy simple IDs 1/2/3)
///
/// Returns (req_id, signal_ids) on success, None if format is not recognized.
pub fn parse_tlv_subscribe_req(data: &[u8]) -> Option<(u16, Vec<u16>)> {
    // Byte[0] must be 0x20 (TLV SUBSCRIBE_CMD marker)
    if data.len() < 12 + 27 || data[0] != 0x20 {
        return None;
    }

    let req_id = u16::from_le_bytes([data[6], data[7]]);

    // Scan items starting at offset 12
    let mut pos = 12;
    let mut signal_ids = Vec::new();

    while pos + 27 <= data.len() {
        // Item header: tag=0x03, len_u16_le=24 (0x18 0x00)
        if data[pos] == 0x03 && data[pos + 1] == 0x18 && data[pos + 2] == 0x00 {
            // signal_id (LE u16) is at item_base + 10
            let signal_id = u16::from_le_bytes([data[pos + 10], data[pos + 11]]);
            if signal_id > 0 {
                signal_ids.push(signal_id);
            }
            pos += 27; // 3 (item header) + 24 (item value)
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

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper: build a valid ACK_FRAME byte buffer ───────────────────────────
    fn make_ack_frame_bytes(session_id: u16, stream_id: u16, ack_upto: u32) -> Vec<u8> {
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_ACK_FRAME,
            flags: 0,
            session_id,
            stream_id,
            seq: 0,
            payload_len: 4,
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(&ack_upto.to_le_bytes());
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    // ── Helper: build a valid NACK_FRAME byte buffer ──────────────────────────
    fn make_nack_frame_bytes(session_id: u16, stream_id: u16, reason: u8, seqs: &[u32]) -> Vec<u8> {
        let n = seqs.len();
        let payload_len = 2 + n * 4;
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_NACK_FRAME,
            flags: 0,
            session_id,
            stream_id,
            seq: 0,
            payload_len: payload_len as u16,
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
    fn make_subscribe_req_bytes(session_id: u16, req_id: u16, op: u8, items: &[(u8, u16)]) -> Vec<u8> {
        let n = items.len();
        let payload_len = 4 + n * SubscribeItem::SIZE;
        let header = IdtHeader {
            magic: IDT_MAGIC,
            version: IDT_VERSION,
            msg_type: MSG_SUBSCRIBE_REQ,
            flags: 0,
            session_id,
            stream_id: 0,
            seq: 0,
            payload_len: payload_len as u16,
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
        // Legacy simple IDs (I.pdf / Flutter client) — must also be accepted
        assert_eq!(SignalId::from_u16(1), Some(SignalId::HR));
        assert_eq!(SignalId::from_u16(2), Some(SignalId::SpO2));
        assert_eq!(SignalId::from_u16(3), Some(SignalId::Temperature));
        // Unknown IDs must be rejected (0x0001==1 is now valid as legacy HR, use other values)
        assert_eq!(SignalId::from_u16(0), None);
        assert_eq!(SignalId::from_u16(4), None);
        assert_eq!(SignalId::from_u16(0x0200), None);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-003
    #[test]
    fn test_signal_id_metadata() {
        assert_eq!(SignalId::HR.name(), "HR");
        assert_eq!(SignalId::SpO2.name(), "PLETH_SPO2");
        assert_eq!(SignalId::Temperature.name(), "BT1_TEMP");
        assert_eq!(SignalId::HR.unit_code(), UNIT_BPM);
        assert_eq!(SignalId::SpO2.unit_code(), UNIT_PCT);
        assert_eq!(SignalId::Temperature.unit_code(), UNIT_DEGC);
        assert_eq!(SignalId::HR.nominal_period_ms(), 1000);
        assert_eq!(SignalId::Temperature.nominal_period_ms(), 2000);
        assert_eq!(SignalId::HR.source_id(), 1);
        assert_eq!(SignalId::HR.value_type(), VALUE_TYPE_FLOAT32);
    }

    // ── IdtHeader tests ───────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-004
    #[test]
    fn test_idt_header_byte_layout() {
        let h = IdtHeader::new_data(42, 7, 100, 15);
        let b = h.to_bytes();
        assert_eq!(b.len(), 15);
        // magic at [0..2] = 0xD17A → LE: 0x7A 0xD1
        assert_eq!(b[0], 0x7A);
        assert_eq!(b[1], 0xD1);
        assert_eq!(b[2], IDT_VERSION);
        assert_eq!(b[3], MSG_DATA_FRAME);
        assert_eq!(b[4], 0); // flags
        assert_eq!(u16::from_le_bytes([b[5], b[6]]), 42); // session_id
        assert_eq!(u16::from_le_bytes([b[7], b[8]]), 7); // stream_id
        assert_eq!(u32::from_le_bytes([b[9], b[10], b[11], b[12]]), 100); // seq
        assert_eq!(u16::from_le_bytes([b[13], b[14]]), 15); // payload_len
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-005
    #[test]
    fn test_idt_header_magic_mismatch() {
        let mut b = IdtHeader::new_data(1, 1, 1, 4).to_bytes();
        b[0] = 0xFF; // corrupt magic
        assert!(IdtHeader::from_bytes(&b).is_none());
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-006
    #[test]
    fn test_idt_header_roundtrip() {
        let original = IdtHeader::new_data(5, 3, 999, 15);
        let parsed = IdtHeader::from_bytes(&original.to_bytes()).unwrap();
        assert_eq!(original, parsed);
    }

    // ── DataFrame tests ───────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-007
    #[test]
    fn test_data_frame_total_length() {
        let frame = DataFrame::new(1, 1, 1, 0, 65.0);
        assert_eq!(frame.to_ble_bytes().len(), DataFrame::TOTAL_LEN);
        assert_eq!(DataFrame::TOTAL_LEN, 34);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-008
    #[test]
    fn test_data_frame_byte_layout() {
        let t0_ms: u64 = 1_700_000_000_000;
        let value: f32 = 72.5;
        let bytes = DataFrame::new(1, 2, 10, t0_ms, value).to_ble_bytes();

        // Header occupies [0..15]
        assert_eq!(u16::from_le_bytes([bytes[0], bytes[1]]), IDT_MAGIC);
        assert_eq!(bytes[3], MSG_DATA_FRAME);

        // t0_ms at [15..23]
        let parsed_t0 = u64::from_le_bytes([
            bytes[15], bytes[16], bytes[17], bytes[18],
            bytes[19], bytes[20], bytes[21], bytes[22],
        ]);
        assert_eq!(parsed_t0, t0_ms);

        // count at [23] = 1
        assert_eq!(bytes[23], 1u8);

        // dt_ms at [24..26] = 0
        assert_eq!(u16::from_le_bytes([bytes[24], bytes[25]]), 0u16);

        // value at [26..30]
        let parsed_val = f32::from_le_bytes([bytes[26], bytes[27], bytes[28], bytes[29]]);
        assert!((parsed_val - value).abs() < f32::EPSILON);

        // CRC at [30..34]
        let expected_crc = crc32c::crc32c(&bytes[..30]);
        let actual_crc = u32::from_le_bytes([bytes[30], bytes[31], bytes[32], bytes[33]]);
        assert_eq!(expected_crc, actual_crc);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-009
    #[test]
    fn test_data_frame_crc_corruption() {
        let mut bytes = DataFrame::new(1, 1, 1, 0, 65.0).to_ble_bytes();
        bytes[10] ^= 0xFF; // corrupt a header byte
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
    #[test]
    fn test_ack_frame_total_length() {
        assert_eq!(AckFrame::TOTAL_LEN, 23);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-012
    #[test]
    fn test_ack_frame_parse() {
        let bytes = make_ack_frame_bytes(1, 2, 99);
        let ack = AckFrame::from_ble_bytes(&bytes).unwrap();
        assert_eq!(ack.header.session_id, 1);
        assert_eq!(ack.header.stream_id, 2);
        assert_eq!(ack.ack_upto, 99);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-013
    #[test]
    fn test_ack_frame_crc_fail() {
        let mut bytes = make_ack_frame_bytes(1, 1, 10);
        bytes[15] ^= 0xFF; // corrupt ack_upto
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
        // Verify magic in header
        assert_eq!(u16::from_le_bytes([bytes[0], bytes[1]]), IDT_MAGIC);
        assert_eq!(bytes[3], MSG_SUBSCRIBE_RSP);
        // Size = header(15) + req_id(2)+status(1)+n(1) + result(10) + crc(4) = 33
        assert_eq!(bytes.len(), 15 + 4 + 10 + 4);
    }

    // ── Catalog tests ─────────────────────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-018
    #[test]
    fn test_catalog_default_medical() {
        let catalog = Catalog::default_medical_catalog();
        assert_eq!(catalog.entries.len(), 3);
        assert_eq!(catalog.entries[0].signal_id, 0x0101);
        assert_eq!(catalog.entries[1].signal_id, 0x0102);
        assert_eq!(catalog.entries[2].signal_id, 0x0103);
    }

    /// ID SRS: SRS-TEST-BLEPROTOCOL-019
    #[test]
    fn test_catalog_to_ble_bytes() {
        let catalog = Catalog::default_medical_catalog();
        let bytes = catalog.to_ble_bytes();
        assert!(!bytes.is_empty());
        // First entry: source_id=1 at [0], signal_id=0x0101 at [1..3] LE
        assert_eq!(bytes[0], 1u8); // source_id
        assert_eq!(bytes[1], 0x01); // signal_id low byte (0x0101 LE → 0x01, 0x01)
        assert_eq!(bytes[2], 0x01); // signal_id high byte
        assert_eq!(bytes[3], VALUE_TYPE_FLOAT32); // value_type
        assert_eq!(bytes[4], UNIT_BPM); // unit_code
    }

    // ── InboundFrame dispatch tests ───────────────────────────────────────────

    /// ID SRS: SRS-TEST-BLEPROTOCOL-020
    #[test]
    fn test_inbound_frame_dispatch_ack() {
        let bytes = make_ack_frame_bytes(1, 1, 5);
        match InboundFrame::from_ble_bytes(&bytes) {
            Some(InboundFrame::Ack(ack)) => assert_eq!(ack.ack_upto, 5),
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
    #[test]
    fn test_inbound_frame_bad_magic() {
        let mut bytes = make_ack_frame_bytes(1, 1, 1);
        bytes[0] = 0x00; // corrupt magic
        assert!(InboundFrame::from_ble_bytes(&bytes).is_none());
    }
}
