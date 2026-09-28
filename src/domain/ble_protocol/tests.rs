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
fn make_subscribe_req_bytes(session_id: u16, req_id: u16, op: u8, items: &[(u8, u16)]) -> Vec<u8> {
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
/// Version: V1.0
#[test]
fn test_signal_id_new_values() {
    assert_eq!(SignalId::HR.as_u16(), 0x0101);
    assert_eq!(SignalId::SpO2.as_u16(), 0x0102);
    assert_eq!(SignalId::Temperature.as_u16(), 0x0103);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-002
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
#[test]
fn test_idt_header_magic_mismatch() {
    let mut b = IdtHeader::new_data(1, 1, 1).to_bytes();
    b[0] = 0xFF; // corrupt magic
    assert!(IdtHeader::from_bytes(&b).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-006
/// Version: V1.0
#[test]
fn test_idt_header_roundtrip() {
    let original = IdtHeader::new_data(5, 3, 999);
    let parsed = IdtHeader::from_bytes(&original.to_bytes()).unwrap();
    assert_eq!(original, parsed);
}

// ── DataFrame tests ───────────────────────────────────────────────────────

/// ID SRS: SRS-TEST-BLEPROTOCOL-007
/// Version: V1.0
#[test]
fn test_data_frame_total_length() {
    let frame = DataFrame::new(1, 1, 1, 0, 65.0);
    assert_eq!(frame.to_ble_bytes().len(), DataFrame::TOTAL_LEN);
    assert_eq!(DataFrame::TOTAL_LEN, 34); // includes 4-byte CRC32C
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-008
/// Version: V1.0
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

    // CRC32C at [30..34]
    assert_eq!(bytes.len(), 34);
    assert!(DataFrame::verify_crc(&bytes));
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-009
/// Version: V1.0
/// Data frame with corrupted magic returns None from from_ble_bytes
#[test]
fn test_data_frame_bad_magic() {
    let mut bytes = DataFrame::new(1, 1, 1, 0, 65.0).to_ble_bytes();
    bytes[0] = 0xFF; // corrupt magic
    assert!(DataFrame::from_ble_bytes(&bytes).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-010
/// Version: V1.0
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
/// Version: V1.0
/// ACK_FRAME total wire length is 30 bytes (IDT header + payload + CRC32C)
#[test]
fn test_ack_frame_total_len() {
    assert_eq!(AckFrame::TOTAL_LEN, 30);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-012
/// Version: V1.0
#[test]
fn test_ack_frame_parse() {
    let bytes = make_ack_frame_bytes(1, 2, 99);
    let ack = AckFrame::from_ble_bytes(&bytes).unwrap();
    assert_eq!(ack.session_id, 1);
    assert_eq!(ack.stream_id, 2);
    assert_eq!(ack.ack_upto, 99);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-013
/// Version: V1.0
/// ACK_FRAME that is too short returns None
#[test]
fn test_ack_frame_too_short() {
    let bytes = vec![0u8; AckFrame::TOTAL_LEN - 1]; // 29 bytes, need 30
    assert!(AckFrame::from_ble_bytes(&bytes).is_none());
}

// ── NackFrame tests ───────────────────────────────────────────────────────

/// ID SRS: SRS-TEST-BLEPROTOCOL-014
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
#[test]
fn test_catalog_default_medical() {
    let catalog = Catalog::default_medical_catalog();
    assert_eq!(catalog.entries.len(), 12);
    assert_eq!(catalog.entries[0].signal_id, 0x0101); // HR
    assert_eq!(catalog.entries[1].signal_id, 0x0102); // SpO2
    assert_eq!(catalog.entries[2].signal_id, 0x0103); // Temperature
    assert_eq!(catalog.entries[3].signal_id, 0x0201); // SBP
    assert_eq!(catalog.entries[4].signal_id, 0x0202); // DBP
    assert_eq!(catalog.entries[5].signal_id, 0x0203); // MBP
    assert_eq!(catalog.entries[6].signal_id, 0x0301); // ST_II
    assert_eq!(catalog.entries[7].signal_id, 0x0302); // ST_V
    assert_eq!(catalog.entries[8].signal_id, 0x0303); // ST_AVL
    assert_eq!(catalog.entries[9].signal_id, 0x0401); // SPV
    assert_eq!(catalog.entries[10].signal_id, 0x0402); // PPV
    assert_eq!(catalog.entries[11].signal_id, 0x0501); // AmbPres
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-019
/// Version: V1.0
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
    assert_eq!(
        u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]),
        1000
    ); // period_ms
    let name_len = bytes[9] as usize;
    assert_eq!(&bytes[10..10 + name_len], b"HR");
}

// ── InboundFrame dispatch tests ───────────────────────────────────────────

/// ID SRS: SRS-TEST-BLEPROTOCOL-020
/// Version: V1.0
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
/// Version: V1.0
#[test]
fn test_inbound_frame_dispatch_nack() {
    let bytes = make_nack_frame_bytes(1, 1, 2, &[3, 5]);
    match InboundFrame::from_ble_bytes(&bytes) {
        Some(InboundFrame::Nack(nack)) => assert_eq!(nack.seq_list, vec![3, 5]),
        _ => panic!("Expected InboundFrame::Nack"),
    }
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-022
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
/// A buffer shorter than IdtHeader::SIZE returns None from InboundFrame
#[test]
fn test_inbound_frame_too_short_for_ack() {
    // Any buffer shorter than the 13-byte IDT header returns None
    let bytes = vec![0x7Au8, 0xD1, 0x01]; // IDT magic + 1 byte, too short for header
    assert!(InboundFrame::from_ble_bytes(&bytes).is_none());
}

// ── AckFrame::is_acked tests ──────────────────────────────────────────────

/// ID SRS: SRS-TEST-BLEPROTOCOL-025
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
/// IdtHeader::from_bytes returns None if buffer is shorter than 13 bytes
#[test]
fn test_idt_header_too_short() {
    let b = vec![0x7A, 0xD1, 0x01]; // only 3 bytes
    assert!(IdtHeader::from_bytes(&b).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-030
/// Version: V1.0
/// DataFrame::from_ble_bytes returns None if buffer is shorter than TOTAL_LEN (34) bytes
#[test]
fn test_data_frame_too_short() {
    let b = vec![0u8; DataFrame::TOTAL_LEN - 1]; // 33 bytes
    assert!(DataFrame::from_ble_bytes(&b).is_none());
}

// ── NackFrame CRC guard ───────────────────────────────────────────────────

/// ID SRS: SRS-TEST-BLEPROTOCOL-031
/// Version: V1.0
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
/// Version: V1.0
/// InboundFrame::from_ble_bytes returns None for buffers shorter than IdtHeader::SIZE
#[test]
fn test_inbound_frame_single_byte_returns_none() {
    assert!(InboundFrame::from_ble_bytes(&[]).is_none());
    assert!(InboundFrame::from_ble_bytes(&[0x7A]).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-036
/// Version: V1.0
/// InboundFrame returns None when IDT magic matches but buffer is shorter than IdtHeader::SIZE
#[test]
fn test_inbound_frame_idt_magic_too_short() {
    // 4 bytes: magic (0x7A 0xD1) + two padding bytes — length < IdtHeader::SIZE (13)
    let bytes = vec![0x7Au8, 0xD1, 0x01, 0x21]; // IDT_MAGIC LE + partial header
    assert!(InboundFrame::from_ble_bytes(&bytes).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-037
/// Version: V1.0
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
/// Version: V1.0
/// SubscribeReq::from_ble_bytes returns None when CRC is corrupted (no valid stride found)
#[test]
fn test_subscribe_req_bad_crc_returns_none() {
    let mut buf = make_subscribe_req_bytes(1, 1, SUB_OP_SUBSCRIBE, &[(1, SignalId::HR.as_u16())]);
    // Corrupt last CRC byte so detect_item_stride finds no valid stride
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    assert!(SubscribeReq::from_ble_bytes(&buf).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-040
/// Version: V1.0
/// NackFrame::from_ble_bytes with n=0 (empty seq_list) parses cleanly
#[test]
fn test_nack_frame_zero_seqs() {
    let buf = make_nack_frame_bytes(1, 2, 2, &[]); // n=0, reason=MISSING
    let frame = NackFrame::from_ble_bytes(&buf).expect("n=0 NackFrame must parse");
    assert_eq!(frame.reason, 2);
    assert!(frame.seq_list.is_empty());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-041
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
    assert!(r.get(0x0301).is_some(), "ST_II must be registered");
    assert!(r.get(0x0302).is_some(), "ST_V must be registered");
    assert!(r.get(0x0303).is_some(), "ST_AVL must be registered");
    assert!(r.get(0x0401).is_some(), "SPV must be registered");
    assert!(r.get(0x0402).is_some(), "PPV must be registered");
    assert!(r.get(0x0501).is_some(), "AmbPres must be registered");
    assert_eq!(r.all_signal_ids().len(), 12);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-046
/// Version: V1.0
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
    assert_eq!(r.all_signal_ids().len(), 13);
    assert!(r.get(0x0204).is_some());
    let catalog = r.build_catalog();
    assert_eq!(catalog.entries.len(), 13);
    // Sorted by signal_id: 0x0101..0x0103, 0x0201..0x0204, 0x0301..0x0303, 0x0401..0x0402, 0x0501
    assert_eq!(catalog.entries[6].signal_id, 0x0204);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-047
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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
/// Version: V1.0
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

/// ID SRS: SRS-TEST-BLEPROTOCOL-051
/// Title: parse_tlv_subscribe_req parses the real 89-byte Flutter payload
///
/// Description: The captured payload carries period_ms=0 in every item (what every
///              deployed app sends today) — each entry's requested period must
///              normalize to None, not Some(0).
///
/// Version: V2.0
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
        0x03, 0x01, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01,
        0x03, 0x18, 0x00, // item 3 tag+len
        0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x03, 0x00, // signal_id=3 (Temperature)
        0x03, 0x01, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01,
    ];
    assert_eq!(bytes.len(), 89);
    let (req_id, entries) = parse_tlv_subscribe_req(&bytes).unwrap();
    assert_eq!(req_id, 42);
    assert_eq!(entries, vec![(1u16, None), (2u16, None), (3u16, None)]);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-052
/// Title: parse_tlv_subscribe_req returns None for wrong marker byte
///
/// Description: byte[0] must be 0x20; any other value is rejected outright.
///
/// Version: V2.0
#[test]
fn test_parse_tlv_subscribe_req_wrong_marker() {
    let mut bytes = vec![0u8; 8 + 27];
    bytes[0] = 0x7A; // not 0x20
    assert!(parse_tlv_subscribe_req(&bytes).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-053
/// Title: parse_idt_wrapped_tlv_subscribe_req accepts a MyPredi TLV SUBSCRIBE_REQ
///        wrapped in an IDT envelope
///
/// Description: Delegates to parse_tlv_subscribe_req on the unwrapped payload; the
///              return type carries (signal_id, requested_period_ms) pairs.
///
/// Version: V2.0
#[test]
fn test_parse_idt_wrapped_tlv_subscribe_req() {
    let mut bytes = vec![0u8; 24];
    bytes[0..2].copy_from_slice(&IDT_MAGIC.to_le_bytes());
    bytes[2] = IDT_VERSION;
    bytes[3] = MSG_SUBSCRIBE_REQ;
    bytes.extend_from_slice(&[
        0x20, 0x56, 0x00, 0x01, 0x02, 0x00, 0x2A, 0x00, // tlv header
        0x03, 0x18, 0x00, 0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x01, 0x00, 0x03, 0x01, 0x00,
        0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01, 0x03, 0x18, 0x00,
        0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x02, 0x00, 0x03, 0x01, 0x00, 0x00, 0x04, 0x04,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01, 0x03, 0x18, 0x00, 0x01, 0x01, 0x00,
        0x01, 0x02, 0x02, 0x00, 0x03, 0x00, 0x03, 0x01, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x05, 0x01, 0x00, 0x01,
    ]);
    bytes.extend_from_slice(&[0u8; 4]);

    assert_eq!(
        parse_idt_wrapped_tlv_subscribe_req(&bytes),
        Some((0x002A, vec![(1u16, None), (2u16, None), (3u16, None)]))
    );
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-054
/// Title: parse_tlv_subscribe_req returns None for a buffer that is too short
///
/// Description: A buffer shorter than an 8-byte header + a minimal 3-byte item
///              (tag+len, len=0) is rejected before any item scanning starts.
///
/// Version: V2.0
#[test]
fn test_parse_tlv_subscribe_req_too_short() {
    let bytes = vec![0x20u8; 30];
    // All bytes after the header are 0x20 too, so the first item-tag check
    // (data[pos] == 0x03) fails immediately and no entries are collected.
    assert!(parse_tlv_subscribe_req(&bytes).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-055
/// Title: parse_tlv_subscribe_req reads a non-zero period_ms as a throttle request
///
/// Description: Patching the real Flutter capture's first-item period_ms bytes
///              (offset item_base+16..+20, value 0 -> 5000 LE) must surface
///              Some(5000) for that entry while the other two (still period_ms=0)
///              stay None.
///
/// Version: V1.0
#[test]
fn test_parse_tlv_subscribe_req_period_ms_requested() {
    let mut bytes: Vec<u8> = vec![
        0x20, 0x56, 0x00, 0x01, 0x02, 0x00, 0x2A, 0x00, // header (8b)
        0x03, 0x18, 0x00, // item 1 tag+len
        0x01, 0x01, 0x00, 0x01, // nested TLV: source_id=1
        0x02, 0x02, 0x00, 0x01, 0x00, // nested TLV: signal_id=1 (HR)
        0x03, 0x01, 0x00, 0x00, // nested TLV: mode=0
        0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, // nested TLV: period_ms=0 (patched below)
        0x05, 0x01, 0x00, 0x01, // nested TLV: batch_max=1
        0x03, 0x18, 0x00, // item 2 tag+len
        0x01, 0x01, 0x00, 0x01, 0x02, 0x02, 0x00, 0x02, 0x00, // signal_id=2 (SpO2)
        0x03, 0x01, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x01,
    ];
    // period_ms value bytes for item 1 are at absolute offset 27..31.
    assert_eq!(
        &bytes[24..27],
        &[0x04, 0x04, 0x00],
        "sanity: tag+len of nested 0x04"
    );
    bytes[27..31].copy_from_slice(&5_000u32.to_le_bytes());

    let (req_id, entries) = parse_tlv_subscribe_req(&bytes).unwrap();
    assert_eq!(req_id, 42);
    assert_eq!(entries, vec![(1u16, Some(5_000)), (2u16, None)]);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-056
/// Title: parse_tlv_subscribe_req is length-driven, not fixed to a 24-byte item
///
/// Description: An item carrying only a signal_id nested TLV (item len=5, not the
///              usual 24) must still parse correctly — proves the cursor advances by
///              `3 + len` rather than a hardcoded stride.
///
/// Version: V1.0
#[test]
fn test_parse_tlv_subscribe_req_variable_item_length() {
    let bytes: Vec<u8> = vec![
        0x20, 0x00, 0x00, 0x01, 0x00, 0x00, 0x0A, 0x00, // header (8b), req_id=10
        0x03, 0x05, 0x00, // item tag=0x03, len=5 (not 24)
        0x02, 0x02, 0x00, 0x07, 0x00, // nested TLV: signal_id=7, no other fields
    ];
    let (req_id, entries) = parse_tlv_subscribe_req(&bytes).unwrap();
    assert_eq!(req_id, 10);
    assert_eq!(entries, vec![(7u16, None)]);
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-057
/// Title: parse_tlv_subscribe_req rejects a truncated nested TLV without panicking
///
/// Description: A nested TLV whose declared length overruns the enclosing item's
///              value bounds must be dropped cleanly (no signal_id captured for that
///              item), never panic or read out of bounds.
///
/// Version: V1.0
#[test]
fn test_parse_tlv_subscribe_req_truncated_nested_tlv() {
    let bytes: Vec<u8> = vec![
        0x20, 0x00, 0x00, 0x01, 0x00, 0x00, 0x63, 0x00, // header (8b), req_id=99
        0x03, 0x04, 0x00, // item tag=0x03, len=4
        0x02, 0x02, 0x00, 0x01, // nested TLV claims len=2 but only 1 byte remains
    ];
    assert!(parse_tlv_subscribe_req(&bytes).is_none());
}

/// ID SRS: SRS-TEST-BLEPROTOCOL-058
/// Title: parse_tlv_subscribe_req keeps already-parsed entries when a later item is truncated
///
/// Description: A well-formed first item followed by a second item whose declared
///              length overruns the buffer must stop the scan (break) but still
///              return the entries collected before the truncation.
///
/// Version: V1.0
#[test]
fn test_parse_tlv_subscribe_req_truncated_item_keeps_prior_entries() {
    let bytes: Vec<u8> = vec![
        0x20, 0x00, 0x00, 0x01, 0x00, 0x00, 0x37, 0x00, // header (8b), req_id=55
        0x03, 0x05, 0x00, // item 1: tag=0x03, len=5
        0x02, 0x02, 0x00, 0x01, 0x00, // nested TLV: signal_id=1
        0x03, 0x18, 0x00, // item 2: tag=0x03, len=24 — but no bytes follow
    ];
    let (req_id, entries) = parse_tlv_subscribe_req(&bytes).unwrap();
    assert_eq!(req_id, 55);
    assert_eq!(entries, vec![(1u16, None)]);
}
