// src/output/health.rs
// Module: output.health
// Purpose: Health monitoring — reads OS state from health.json (written by
//          HealthWriter.ps1) and combines it with GATE's internal state to
//          produce a compact BLE health payload emitted on the Control
//          characteristic (0x90b0, Notify).

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ──────────────────────────────────────────────
// OS snapshot (from health.json)
// ──────────────────────────────────────────────

/// ID SRS: SRS-MOD-HEALTH-001
/// Title: OsHealthSnapshot
///
/// Description: VRConnect shall deserialize the OS health state written by
/// HealthWriter.ps1 from logs/health.json. If the file is absent, unreadable,
/// or its timestamp is stale, all indicator fields are zeroed.
///
/// Version: V1.0
#[derive(Debug, Deserialize, Default)]
pub struct OsHealthSnapshot {
    pub ts:           u64,
    pub vr:           u8,
    pub disk:         u8,
    pub disk_free_gb: f32,
    pub wd_vr:        u8,
    pub wd_gate:      u8,
    // disk_used_pct and writer_ver are present in the file but not needed here.
}

// ──────────────────────────────────────────────
// GATE internal state
// ──────────────────────────────────────────────

/// ID SRS: SRS-MOD-HEALTH-002
/// Title: GateHealthState
///
/// Description: VRConnect shall maintain the internal GATE health indicators
/// behind an Arc<RwLock<GateHealthState>> shared between the processing
/// pipeline, the BLE write-handler, and the health task.
///
/// `ble = 1` when at least one IDT stream is active (signal_to_stream non-empty).
/// This reflects active data flow, NOT merely a BLE link presence. A Central
/// that is connected but has not yet sent SUBSCRIBE_REQ yields ble = 0.
///
/// Version: V1.0
#[derive(Debug)]
pub struct GateHealthState {
    /// Socket.IO client is connected to VitalRecorder.
    pub sio_connected: bool,
    /// At least one IDT signal stream is active (Central subscribed via SUBSCRIBE_REQ).
    /// Updated from BleSessionState::signal_to_stream: !is_empty() → true.
    pub ble_subscriber: bool,
    /// Instant of the last ProcessedData forwarded to outputs.
    pub last_processed_data: Option<Instant>,
    /// flow = 0 if no ProcessedData received within this many seconds.
    pub flow_timeout_sec: u64,
}

impl Default for GateHealthState {
    fn default() -> Self {
        Self {
            sio_connected: false,
            ble_subscriber: false,
            last_processed_data: None,
            flow_timeout_sec: 60,
        }
    }
}

impl GateHealthState {
    /// ID SRS: SRS-FN-HEALTH-001
    /// Title: flow_ok
    ///
    /// Description: Returns 1 if a ProcessedData frame was received within
    /// `flow_timeout_sec` seconds, 0 otherwise (including when no data has
    /// ever been received).
    ///
    /// Timing note: when VR dies, sio transitions to 0 immediately (via
    /// Arc<AtomicBool>) but flow only transitions to 0 after flow_timeout_sec
    /// seconds. A transient payload showing sio=0, flow=1 is therefore expected
    /// and correct — it means "the last frame was recent but the source just
    /// disconnected." Do NOT treat this window as a bug.
    ///
    /// Version: V1.0
    pub fn flow_ok(&self) -> u8 {
        match self.last_processed_data {
            Some(t) if t.elapsed().as_secs() < self.flow_timeout_sec => 1,
            _ => 0,
        }
    }
}

// ──────────────────────────────────────────────
// BLE payload
// ──────────────────────────────────────────────

/// ID SRS: SRS-MOD-HEALTH-003
/// Title: HealthPayload
///
/// Description: VRConnect shall serialize this struct to UTF-8 JSON (no
/// pretty-printing) and transmit it on the Control GATT characteristic
/// (UUID suffix 0x90b0, Notify). Target size: < 120 bytes (hard cap: < 247
/// bytes for single BLE MTU packet with negotiated MTU=247).
///
/// `ok = gate & sio & ble & flow & vr & disk & wd_vr & wd_gate`
///
/// `ver` is bumped only on breaking schema changes (field rename/removal).
/// Additive fields (new indicators) do not require a version bump.
///
/// disk_free_gb is intentionally omitted — it is a float, not a binary
/// health indicator, and is already available in health.json locally.
///
/// Version: V1.0
#[derive(Debug, Serialize)]
pub struct HealthPayload {
    /// Unix timestamp (seconds) at payload build time.
    pub ts:      u64,
    /// Schema version. Currently 1. Bump on breaking changes only.
    pub ver:     u8,
    /// Global health: 1 only if ALL indicators are 1.
    pub ok:      u8,
    /// GATE process alive (invariant: always 1 if payload is emitted).
    pub gate:    u8,
    /// Socket.IO connected to VitalRecorder.
    pub sio:     u8,
    /// At least one IDT signal stream active (reflects data flow, not link).
    pub ble:     u8,
    /// ProcessedData received within flow_timeout_sec seconds.
    pub flow:    u8,
    /// VitalRecorder process alive (from health.json).
    pub vr:      u8,
    /// Disk usage within thresholds (from health.json).
    pub disk:    u8,
    /// VitalRecorder watchdog task running (from health.json).
    pub wd_vr:   u8,
    /// VRConnect watchdog task running (from health.json).
    pub wd_gate: u8,
}

// ──────────────────────────────────────────────
// Core functions
// ──────────────────────────────────────────────

/// ID SRS: SRS-FN-HEALTH-002
/// Title: read_os_snapshot
///
/// Description: VRConnect shall read and parse health.json from `path`.
/// Returns a zeroed OsHealthSnapshot (all fields = 0) if:
///   - the file does not exist or cannot be read
///   - the file contains invalid JSON
///   - the `ts` field is older than `stale_threshold_sec` seconds
///
/// The caller should pass `stale_threshold_sec = 2 * check_interval_sec`.
///
/// Version: V1.0
pub fn read_os_snapshot(path: &Path, stale_threshold_sec: u64) -> OsHealthSnapshot {
    let content = match std::fs::read_to_string(path) {
        Ok(s)  => s,
        Err(_) => return OsHealthSnapshot::default(),
    };
    // PS 5.1 Out-File / WriteAllText with Encoding::UTF8 prepends a UTF-8 BOM (EF BB BF).
    // Strip it so serde_json can parse the file regardless of how it was written.
    let content = content.trim_start_matches('\u{FEFF}');

    let snapshot: OsHealthSnapshot = match serde_json::from_str(content) {
        Ok(s)  => s,
        Err(e) => {
            log::warn!("[health] Failed to parse {}: {}", path.display(), e);
            return OsHealthSnapshot::default();
        }
    };

    let now_sec = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();

    let age = now_sec.saturating_sub(snapshot.ts);
    if age > stale_threshold_sec {
        log::warn!(
            "[health] health.json stale (ts={}, age={}s > threshold={}s) — HealthWriter may be dead",
            snapshot.ts, age, stale_threshold_sec
        );
        return OsHealthSnapshot::default();
    }

    snapshot
}

/// ID SRS: SRS-FN-HEALTH-003
/// Title: build_payload
///
/// Description: VRConnect shall merge an OsHealthSnapshot with GateHealthState
/// to produce the final HealthPayload. `gate` is always 1 (invariant: if this
/// function executes, GATE is alive by definition).
///
/// Version: V1.0
pub fn build_payload(os: &OsHealthSnapshot, gate: &GateHealthState) -> HealthPayload {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();

    let g_gate: u8 = 1; // invariant: task running → GATE alive
    let g_sio:  u8 = gate.sio_connected  as u8;
    let g_ble:  u8 = gate.ble_subscriber as u8;
    let g_flow: u8 = gate.flow_ok();

    let ok = g_gate & g_sio & g_ble & g_flow & os.vr & os.disk & os.wd_vr & os.wd_gate;

    HealthPayload {
        ts,
        ver:     1,
        ok,
        gate:    g_gate,
        sio:     g_sio,
        ble:     g_ble,
        flow:    g_flow,
        vr:      os.vr,
        disk:    os.disk,
        wd_vr:   os.wd_vr,
        wd_gate: os.wd_gate,
    }
}

// ──────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn now_sec() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn write_temp(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "{}", content).unwrap();
        f.flush().unwrap();
        f
    }

    fn fresh_json(ts: u64) -> String {
        format!(
            r#"{{"ts":{ts},"writer_ver":"1.0","ok_os":1,"vr":1,"disk":1,"disk_free_gb":45.30,"disk_used_pct":12.5,"wd_vr":1,"wd_gate":1}}"#
        )
    }

    fn all_ok_gate() -> GateHealthState {
        GateHealthState {
            sio_connected: true,
            ble_subscriber: true,
            last_processed_data: Some(Instant::now() - Duration::from_secs(5)),
            flow_timeout_sec: 60,
        }
    }

    fn all_ok_os() -> OsHealthSnapshot {
        OsHealthSnapshot {
            ts: now_sec(),
            vr: 1, disk: 1, disk_free_gb: 45.0, wd_vr: 1, wd_gate: 1,
        }
    }

    /// ID SRS: SRS-TEST-HEALTH-001
    /// HT-001 — valid health.json read and deserialized correctly.
    #[test]
    fn ht_001_valid_snapshot() {
        let f = write_temp(&fresh_json(now_sec()));
        let snap = read_os_snapshot(f.path(), 60);
        assert_eq!(snap.vr, 1);
        assert_eq!(snap.disk, 1);
        assert_eq!(snap.wd_vr, 1);
        assert_eq!(snap.wd_gate, 1);
        assert!((snap.disk_free_gb - 45.30).abs() < 0.01);
    }

    /// ID SRS: SRS-TEST-HEALTH-002
    /// HT-002 — absent file → all OS fields zeroed, no panic.
    #[test]
    fn ht_002_absent_file() {
        let snap = read_os_snapshot(Path::new("/nonexistent/health.json"), 60);
        assert_eq!(snap.vr, 0);
        assert_eq!(snap.disk, 0);
        assert_eq!(snap.wd_vr, 0);
        assert_eq!(snap.wd_gate, 0);
        assert_eq!(snap.ts, 0);
    }

    /// ID SRS: SRS-TEST-HEALTH-002B
    /// Malformed JSON → all OS fields zeroed, no panic.
    #[test]
    fn ht_002b_invalid_json() {
        let f = write_temp("{ this is not valid json }");
        let snap = read_os_snapshot(f.path(), 60);
        assert_eq!(snap.vr, 0);
        assert_eq!(snap.disk, 0);
        assert_eq!(snap.ts, 0);
    }

    /// ID SRS: SRS-TEST-HEALTH-003
    /// HT-003 — stale ts (120s old, threshold 60s) → all OS fields zeroed.
    #[test]
    fn ht_003_stale_snapshot() {
        let old_ts = now_sec().saturating_sub(120);
        let f = write_temp(&fresh_json(old_ts));
        let snap = read_os_snapshot(f.path(), 60);
        assert_eq!(snap.vr, 0);
        assert_eq!(snap.disk, 0);
        assert_eq!(snap.wd_vr, 0);
        assert_eq!(snap.wd_gate, 0);
    }

    /// ID SRS: SRS-TEST-HEALTH-004
    /// HT-004 — flow_ok() with last_processed_data = None → 0.
    #[test]
    fn ht_004_flow_none() {
        assert_eq!(GateHealthState::default().flow_ok(), 0);
    }

    /// ID SRS: SRS-TEST-HEALTH-005
    /// HT-005 — flow_ok() with data 30s ago, timeout 60s → 1.
    #[test]
    fn ht_005_flow_within_timeout() {
        let s = GateHealthState {
            last_processed_data: Some(Instant::now() - Duration::from_secs(30)),
            flow_timeout_sec: 60,
            ..Default::default()
        };
        assert_eq!(s.flow_ok(), 1);
    }

    /// ID SRS: SRS-TEST-HEALTH-006
    /// HT-006 — flow_ok() with data 90s ago, timeout 60s → 0.
    ///
    /// Note: if sio has just transitioned to 0 (VR died), the payload may show
    /// sio=0, flow=1 for up to flow_timeout_sec seconds before flow also turns 0.
    /// This is EXPECTED AND CORRECT — do not treat the transient window as a bug.
    #[test]
    fn ht_006_flow_past_timeout() {
        let s = GateHealthState {
            last_processed_data: Some(Instant::now() - Duration::from_secs(90)),
            flow_timeout_sec: 60,
            ..Default::default()
        };
        assert_eq!(s.flow_ok(), 0);
    }

    /// ID SRS: SRS-TEST-HEALTH-007
    /// HT-007 — build_payload with all indicators = 1 → ok = 1, ver = 1.
    #[test]
    fn ht_007_build_payload_all_ok() {
        let p = build_payload(&all_ok_os(), &all_ok_gate());
        assert_eq!(p.ok,   1);
        assert_eq!(p.gate, 1);
        assert_eq!(p.sio,  1);
        assert_eq!(p.ble,  1);
        assert_eq!(p.flow, 1);
        assert_eq!(p.vr,   1);
        assert_eq!(p.disk, 1);
        assert_eq!(p.wd_vr,   1);
        assert_eq!(p.wd_gate, 1);
        assert_eq!(p.ver,  1);
    }

    /// ID SRS: SRS-TEST-HEALTH-008
    /// HT-008 — build_payload with disk = 0 → ok = 0.
    #[test]
    fn ht_008_build_payload_disk_fail() {
        let os = OsHealthSnapshot { disk: 0, ..all_ok_os() };
        let p = build_payload(&os, &all_ok_gate());
        assert_eq!(p.ok,   0);
        assert_eq!(p.disk, 0);
    }

    /// ID SRS: SRS-TEST-HEALTH-009
    /// Each individual failing indicator propagates ok = 0.
    #[test]
    fn ht_009_each_failing_indicator_clears_ok() {
        let cases: &[fn(&mut OsHealthSnapshot, &mut GateHealthState)] = &[
            |os, _|  { os.vr = 0; },
            |os, _|  { os.disk = 0; },
            |os, _|  { os.wd_vr = 0; },
            |os, _|  { os.wd_gate = 0; },
            |_, g|   { g.sio_connected = false; },
            |_, g|   { g.ble_subscriber = false; },
            |_, g|   { g.last_processed_data = None; },
        ];
        for mutate in cases {
            let mut os = all_ok_os();
            let mut gate = all_ok_gate();
            mutate(&mut os, &mut gate);
            let p = build_payload(&os, &gate);
            assert_eq!(p.ok, 0, "Expected ok=0 but got ok=1 after mutation");
        }
    }

    /// ID SRS: SRS-TEST-HEALTH-010
    /// HT-010 — serialized JSON payload fits in a single BLE MTU (< 247 bytes).
    /// Soft target: < 120 bytes.
    #[test]
    fn ht_010_payload_size() {
        // Use worst-case values: max u64 ts, large float
        let os = OsHealthSnapshot {
            ts: 9_999_999_999,
            vr: 1, disk: 1, disk_free_gb: 999.99, wd_vr: 1, wd_gate: 1,
        };
        let gate = all_ok_gate();
        let mut p = build_payload(&os, &gate);
        p.ts = 9_999_999_999; // override with worst-case 10-digit value
        let json = serde_json::to_string(&p).unwrap();
        assert!(
            json.len() < 247,
            "Payload exceeds BLE MTU (247 bytes): {} bytes — `{}`",
            json.len(), json
        );
        assert!(
            json.len() < 120,
            "Payload exceeds 120-byte soft target: {} bytes — `{}`",
            json.len(), json
        );
    }
}
