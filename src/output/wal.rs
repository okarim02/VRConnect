// /src/output/wal.rs
// Module: output.wal
// Purpose: Write-Ahead Log (WAL) entry format for the BLE history ring-buffer.
//          Each entry is 22 bytes: magic(4) + signal_id(2) + t0_ms(8) + value_f32(4) + CRC32C(4).
//          File I/O (append, fsync, compaction) lives in ble_reliable.rs::wal_task.

/// ID SRS: SRS-MOD-WAL-001
/// Title: WalEntry
///
/// Description: VRConnect shall represent a WAL journal entry as a 22-byte binary record.
///   Layout: magic(4) + signal_id(2) + t0_ms(8) + value_f32(4) + CRC32C(4).
///   CRC32C is computed over bytes [4..18] (signal_id + t0_ms + value_f32).
///   All integers are little-endian.
///
/// Version: V1.0
pub struct WalEntry {
    /// IDT signal identifier (e.g. 0x0101 = HR, 0x0102 = SpO2).
    /// Matches the `signal_id` used in `BleSessionState::record_history`.
    pub signal_id: u16,
    /// Sample timestamp in milliseconds since the Unix epoch.
    /// Used as the dedup key in `record_history` on WAL replay.
    pub t0_ms: u64,
    /// Measured signal value as a 32-bit float (unit depends on signal_id).
    pub value: f32,
}

pub const WAL_ENTRY_SIZE: usize = 22;
const WAL_MAGIC: u32 = 0x5741_4C45; // "WALE" LE

impl WalEntry {
    /// ID SRS: SRS-FN-BLESESSION-023
    /// Title: to_bytes
    ///
    /// Description: VRConnect shall serialize a WalEntry to a fixed 22-byte little-endian array.
    ///   CRC32C is appended over bytes [4..18] (signal_id + t0_ms + value_f32).
    ///
    /// Version: V1.0
    pub fn to_bytes(&self) -> [u8; WAL_ENTRY_SIZE] {
        let mut buf = [0u8; WAL_ENTRY_SIZE];
        buf[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        buf[4..6].copy_from_slice(&self.signal_id.to_le_bytes());
        buf[6..14].copy_from_slice(&self.t0_ms.to_le_bytes());
        buf[14..18].copy_from_slice(&self.value.to_le_bytes());
        let crc = crc32c::crc32c(&buf[4..18]);
        buf[18..22].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// ID SRS: SRS-FN-BLESESSION-024
    /// Title: from_bytes
    ///
    /// Description: VRConnect shall deserialize a 22-byte buffer into a WalEntry, validating
    ///   the magic header (0x57414C45) and CRC32C checksum over bytes [4..18].
    ///   Returns None if: buffer is shorter than 22 bytes, magic mismatch, or CRC mismatch.
    ///
    /// Version: V1.0
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < WAL_ENTRY_SIZE {
            return None;
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != WAL_MAGIC {
            return None;
        }
        let expected_crc = u32::from_le_bytes(buf[18..22].try_into().ok()?);
        let actual_crc = crc32c::crc32c(&buf[4..18]);
        if actual_crc != expected_crc {
            return None;
        }
        Some(Self {
            signal_id: u16::from_le_bytes(buf[4..6].try_into().ok()?),
            t0_ms: u64::from_le_bytes(buf[6..14].try_into().ok()?),
            value: f32::from_le_bytes(buf[14..18].try_into().ok()?),
        })
    }
}

/// ID SRS: SRS-FN-BLESESSION-025
/// Title: replay_wal_entries
///
/// Description: VRConnect shall read a WAL file and return all valid WalEntry records in
///   file order. Reading stops at the first complete entry with bad magic or CRC (indicates
///   a crash boundary — subsequent entries may be corrupt). A non-zero trailing byte count
///   (file length not a multiple of WAL_ENTRY_SIZE) is logged as a warning.
///   Returns an empty Vec if the file does not exist or cannot be read.
///
/// Version: V1.0
pub fn replay_wal_entries(path: &str) -> Vec<WalEntry> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => return vec![],
    };

    let n_complete = data.len() / WAL_ENTRY_SIZE;
    let trailing = data.len() % WAL_ENTRY_SIZE;
    let mut entries = Vec::with_capacity(n_complete);

    for i in 0..n_complete {
        let slice = &data[i * WAL_ENTRY_SIZE..(i + 1) * WAL_ENTRY_SIZE];
        match WalEntry::from_bytes(slice) {
            Some(e) => entries.push(e),
            None => {
                log::warn!(
                    "[WAL] Invalid entry at offset {} — stopping replay (CRC mismatch or bad magic)",
                    i * WAL_ENTRY_SIZE
                );
                break;
            }
        }
    }
    if trailing > 0 {
        log::warn!(
            "[WAL] {} trailing bytes at end of WAL file (incomplete write at crash?)",
            trailing
        );
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ID SRS: SRS-TEST-BLESESSION-048
    /// Title: WalEntry roundtrip
    ///
    /// Description: to_bytes followed by from_bytes must reproduce all fields exactly.
    ///
    /// Version: V1.0
    #[test]
    fn test_wal_entry_roundtrip() {
        let entry = WalEntry {
            signal_id: 0x0101,
            t0_ms: 1_700_000_000_000,
            value: 72.5_f32,
        };
        let bytes = entry.to_bytes();
        let decoded = WalEntry::from_bytes(&bytes).expect("roundtrip must succeed");
        assert_eq!(decoded.signal_id, entry.signal_id);
        assert_eq!(decoded.t0_ms, entry.t0_ms);
        assert_eq!(decoded.value, entry.value);
    }

    /// ID SRS: SRS-TEST-BLESESSION-049
    /// Title: WalEntry bad CRC returns None
    ///
    /// Description: from_bytes with a single corrupted byte in the CRC field must return None.
    ///
    /// Version: V1.0
    #[test]
    fn test_wal_entry_bad_crc_returns_none() {
        let entry = WalEntry {
            signal_id: 0x0102,
            t0_ms: 2000,
            value: 99.0,
        };
        let mut bytes = entry.to_bytes();
        bytes[18] ^= 0xFF; // flip bits in the first CRC byte
        assert!(WalEntry::from_bytes(&bytes).is_none());
    }

    /// ID SRS: SRS-TEST-BLESESSION-050
    /// Title: replay_wal_entries stops at trailing bytes
    ///
    /// Description: Given 2 valid entries followed by 5 trailing bytes (simulating a crash
    ///   mid-write), replay_wal_entries must return exactly the 2 valid entries.
    ///
    /// Version: V1.0
    #[test]
    fn test_replay_wal_entries_with_trailing_bytes() {
        use std::io::Write as _;

        let e1 = WalEntry {
            signal_id: 0x0101,
            t0_ms: 1000,
            value: 72.0,
        };
        let e2 = WalEntry {
            signal_id: 0x0102,
            t0_ms: 2000,
            value: 98.0,
        };

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&e1.to_bytes()).unwrap();
        tmp.write_all(&e2.to_bytes()).unwrap();
        // 5 trailing bytes — simulates crash mid-write of a third entry
        tmp.write_all(&[0x57, 0x41, 0x4C, 0x45, 0x00]).unwrap();
        tmp.flush().unwrap();

        let path = tmp.path().to_str().unwrap();
        let entries = replay_wal_entries(path);
        assert_eq!(
            entries.len(),
            2,
            "must return exactly the 2 complete valid entries"
        );
        assert_eq!(entries[0].signal_id, 0x0101);
        assert_eq!(entries[0].t0_ms, 1000);
        assert_eq!(entries[1].signal_id, 0x0102);
        assert_eq!(entries[1].t0_ms, 2000);
    }
}
