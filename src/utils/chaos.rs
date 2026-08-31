// /src/utils/chaos.rs
// Module: utils.chaos
// Purpose: Chaos Monkey — probabilistic fault injection for non-production testing.
//
// Env variables:
//   APP_ENV              — must be explicitly "development"/"dev"/"staging"/"test"/"local"
//                           for chaos to ever run. Fail-safe: unset, empty, "production",
//                           "prod", or any unrecognized value disables all chaos.
//   ENABLE_CHAOS_MONKEY  — "true" to activate (master switch). Default: false.
//   CHAOS_RATIO          — trigger probability per check point, 0.0–1.0. Default: 0.1.
//   CHAOS_DISK_FULL      — "true" to enable disk-full simulation in file output.
//   CHAOS_NETWORK_JITTER — "true" to enable BLE latency spike simulation.
//
// Every triggered failure prints: [CHAOS] Triggered <Failure_Type> in <File_Name>
//
// All public functions are intentionally decoupled from domain types:
//   • maybe_drop_frame  → returns bool; caller decides whether to skip the send.
//   • maybe_disk_full   → returns bool; caller maps to its own error type.
//   • maybe_network_jitter → async void; caller awaits before sending.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Monotonic counter shared across all chaos check-points.
/// Used to approximate `CHAOS_RATIO` probability without a random-number crate.
static CHAOS_COUNTER: AtomicUsize = AtomicUsize::new(0);

// ─────────────────────────────────────────────────────────────────────────────
// Guards
// ─────────────────────────────────────────────────────────────────────────────

/// Returns `true` if chaos is globally active:
///   - `APP_ENV` is explicitly one of a known non-production value (allow-list
///     guardrail — fail-safe: unset, empty, or unrecognized `APP_ENV` disables
///     chaos, same as "production" does. This is the opposite of a deny-list on
///     "production"/"prod" alone, which would stay active if `APP_ENV` were simply
///     never set — the default state of a freshly copied `.env` file)
///   - `ENABLE_CHAOS_MONKEY=true`               (master switch)
fn is_enabled() -> bool {
    // Safety guardrail: only ever run when APP_ENV explicitly opts in to a
    // known non-production environment. A missing/unset APP_ENV — e.g. a
    // staging .env copied to a production host without editing it — is
    // treated the same as production: chaos stays off.
    let app_env = std::env::var("APP_ENV").unwrap_or_default().to_lowercase();
    let is_known_non_production = matches!(
        app_env.as_str(),
        "development" | "dev" | "staging" | "test" | "local"
    );
    if !is_known_non_production {
        return false;
    }

    std::env::var("ENABLE_CHAOS_MONKEY")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
}

/// Returns `true` with probability `CHAOS_RATIO` (default 0.1 = 10%).
///
/// Increments `CHAOS_COUNTER` and tests `counter % 100 < threshold`.
/// This gives a deterministic but uniform trigger rate without `rand`.
fn should_trigger() -> bool {
    let ratio = std::env::var("CHAOS_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.1)
        .clamp(0.0, 1.0);

    let threshold = (ratio * 100.0).round() as usize;
    if threshold == 0 {
        return false;
    }

    let c = CHAOS_COUNTER.fetch_add(1, Ordering::Relaxed);
    c % 100 < threshold
}

/// Returns a human-readable summary of active chaos modes for startup logging,
/// or `None` when chaos is fully inactive (the expected state in production).
///
/// Called once at startup so an operator sees an unmissable log/console line
/// if a misconfigured `.env` left chaos active — defense in depth on top of
/// the `is_enabled()` guardrail itself.
pub fn startup_status() -> Option<String> {
    if !is_enabled() {
        return None;
    }

    let mut modes = Vec::new();
    if std::env::var("CHAOS_DISK_FULL")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        modes.push("DiskFull");
    }
    if std::env::var("CHAOS_NETWORK_JITTER")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        modes.push("NetworkJitter");
    }
    // PacketDrop has no separate per-mode toggle — it's active whenever the
    // master switch is, unlike DiskFull/NetworkJitter which need an extra flag.
    modes.push("PacketDrop");

    let app_env = std::env::var("APP_ENV").unwrap_or_default();
    Some(format!(
        "ENABLE_CHAOS_MONKEY=true, APP_ENV={:?}, modes=[{}]",
        app_env,
        modes.join(", ")
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Failure mode 1 — Packet Drop  (original Chaos Monkey, reactivated)
// ─────────────────────────────────────────────────────────────────────────────

/// Determines whether the current BLE DATA frame should be intentionally dropped.
///
/// Replaces the commented-out `CHAOS MONKEY` block that existed in
/// `ble_reliable.rs::output()`. Returns `true` when the caller should skip
/// the BLE notify (simulating a lossy link); `false` otherwise.
///
/// Caller: `ble_reliable.rs` — inside the per-frame loop of `output()`.
pub fn maybe_drop_frame(file_name: &str) -> bool {
    if !is_enabled() || !should_trigger() {
        return false;
    }
    log::warn!("[CHAOS] Triggered PacketDrop in {}", file_name);
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Failure mode 2 — Resource Exhaustion  (disk-full simulation)
// ─────────────────────────────────────────────────────────────────────────────

/// Simulates a disk-full write failure.
///
/// Returns `true` when the caller should abort its current write operation and
/// surface a storage-exhaustion error to the orchestrator. Only active when
/// `CHAOS_DISK_FULL=true`.
///
/// Caller: `file.rs` — at the start of `FileOutput::output()`, before any write.
pub fn maybe_disk_full(file_name: &str) -> bool {
    if !std::env::var("CHAOS_DISK_FULL")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        return false;
    }
    if !is_enabled() || !should_trigger() {
        return false;
    }
    log::warn!("[CHAOS] Triggered DiskFull in {}", file_name);
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Failure mode 3 — Network Jitter  (BLE latency spike)
// ─────────────────────────────────────────────────────────────────────────────

/// Injects a pseudo-random latency spike (100–3000 ms) before the BLE notify,
/// simulating an unstable radio link. Only active when `CHAOS_NETWORK_JITTER=true`.
///
/// The delay is derived from `CHAOS_COUNTER` via an LCG step so no external
/// random-number crate is required.
///
/// Caller: `ble_reliable.rs` — immediately before `server.notify("Data_OUT", …)`.
pub async fn maybe_network_jitter(file_name: &str) {
    if !std::env::var("CHAOS_NETWORK_JITTER")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        return;
    }
    if !is_enabled() || !should_trigger() {
        return;
    }

    // Pseudo-random delay without the `rand` crate.
    // LCG step maps the current counter to [100, 3000] ms.
    let c = CHAOS_COUNTER.load(Ordering::Relaxed);
    let pseudo = c.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    let delay_ms = 100u64 + (pseudo as u64 % 2901);

    log::warn!(
        "[CHAOS] Triggered NetworkJitter ({}ms) in {}",
        delay_ms,
        file_name
    );
    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    /// ID SRS: SRS-TEST-CHAOS-001
    /// Version: V1.0
    #[test]
    #[serial]
    fn unset_app_env_disables_chaos_even_with_master_switch_on() {
        std::env::remove_var("APP_ENV");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        assert!(!is_enabled());
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
    }

    /// ID SRS: SRS-TEST-CHAOS-002
    /// Version: V1.0
    #[test]
    #[serial]
    fn production_app_env_disables_chaos_even_with_master_switch_on() {
        std::env::set_var("APP_ENV", "production");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        assert!(!is_enabled());
        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
    }

    /// ID SRS: SRS-TEST-CHAOS-003
    /// Version: V1.0
    #[test]
    #[serial]
    fn explicit_development_app_env_allows_chaos_when_switch_on() {
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        assert!(is_enabled());
        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
    }

    /// ID SRS: SRS-TEST-CHAOS-004
    /// Version: V1.0
    #[test]
    #[serial]
    fn startup_status_none_when_master_switch_off() {
        std::env::set_var("APP_ENV", "development");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        assert_eq!(startup_status(), None);
        std::env::remove_var("APP_ENV");
    }

    /// ID SRS: SRS-TEST-CHAOS-005
    /// Version: V1.0
    #[test]
    #[serial]
    fn startup_status_lists_all_active_modes() {
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        std::env::set_var("CHAOS_DISK_FULL", "true");
        std::env::set_var("CHAOS_NETWORK_JITTER", "true");

        let status = startup_status().expect("chaos is enabled, must be Some");
        assert!(status.contains("DiskFull"));
        assert!(status.contains("NetworkJitter"));
        assert!(status.contains("PacketDrop"));
        assert!(status.contains("APP_ENV=\"development\""));

        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::remove_var("CHAOS_DISK_FULL");
        std::env::remove_var("CHAOS_NETWORK_JITTER");
    }

    /// ID SRS: SRS-TEST-CHAOS-006
    /// Version: V1.0
    #[test]
    #[serial]
    fn startup_status_omits_disk_full_and_jitter_when_their_flags_are_off() {
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        std::env::remove_var("CHAOS_DISK_FULL");
        std::env::remove_var("CHAOS_NETWORK_JITTER");

        let status = startup_status().expect("chaos is enabled, must be Some");
        assert!(!status.contains("DiskFull"));
        assert!(!status.contains("NetworkJitter"));
        assert!(status.contains("PacketDrop"));

        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
    }

    /// ID SRS: SRS-TEST-CHAOS-007
    /// Version: V1.0
    #[test]
    #[serial]
    fn should_trigger_zero_ratio_never_fires() {
        std::env::set_var("CHAOS_RATIO", "0.0");
        // Deterministic regardless of CHAOS_COUNTER's current value: threshold=0
        // means `c % 100 < 0` is never checked — should_trigger returns early.
        for _ in 0..5 {
            assert!(!should_trigger());
        }
        std::env::remove_var("CHAOS_RATIO");
    }

    /// ID SRS: SRS-TEST-CHAOS-008
    /// Version: V1.0
    #[test]
    #[serial]
    fn should_trigger_full_ratio_always_fires() {
        std::env::set_var("CHAOS_RATIO", "1.0");
        // Deterministic regardless of CHAOS_COUNTER's current value: threshold=100
        // means `c % 100 < 100` holds for every possible c.
        for _ in 0..5 {
            assert!(should_trigger());
        }
        std::env::remove_var("CHAOS_RATIO");
    }

    /// ID SRS: SRS-TEST-CHAOS-009
    /// Version: V1.0
    #[test]
    #[serial]
    fn should_trigger_out_of_range_ratio_is_clamped() {
        // CHAOS_RATIO > 1.0 must clamp to 1.0 (always fires), not panic or overflow.
        std::env::set_var("CHAOS_RATIO", "5.0");
        assert!(should_trigger());
        std::env::remove_var("CHAOS_RATIO");
    }

    /// ID SRS: SRS-TEST-CHAOS-010
    /// Version: V1.0
    #[test]
    #[serial]
    fn maybe_drop_frame_false_when_chaos_disabled() {
        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        assert!(!maybe_drop_frame("test.rs"));
    }

    /// ID SRS: SRS-TEST-CHAOS-011
    /// Version: V1.0
    #[test]
    #[serial]
    fn maybe_drop_frame_true_when_enabled_and_ratio_one() {
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        std::env::set_var("CHAOS_RATIO", "1.0");

        assert!(maybe_drop_frame("test.rs"));

        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::remove_var("CHAOS_RATIO");
    }

    /// ID SRS: SRS-TEST-CHAOS-012
    /// Version: V1.0
    #[test]
    #[serial]
    fn maybe_disk_full_false_when_flag_unset() {
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        std::env::set_var("CHAOS_RATIO", "1.0");
        std::env::remove_var("CHAOS_DISK_FULL");

        assert!(!maybe_disk_full("test.rs"));

        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::remove_var("CHAOS_RATIO");
    }

    /// ID SRS: SRS-TEST-CHAOS-013
    /// Version: V1.0
    #[test]
    #[serial]
    fn maybe_disk_full_false_when_flag_set_but_chaos_disabled() {
        // CHAOS_DISK_FULL alone isn't enough — the master switch/APP_ENV guardrail
        // must also allow chaos, same as every other failure mode.
        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::set_var("CHAOS_DISK_FULL", "true");

        assert!(!maybe_disk_full("test.rs"));

        std::env::remove_var("CHAOS_DISK_FULL");
    }

    /// ID SRS: SRS-TEST-CHAOS-014
    /// Version: V1.0
    #[test]
    #[serial]
    fn maybe_disk_full_true_when_flag_set_and_triggered() {
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        std::env::set_var("CHAOS_RATIO", "1.0");
        std::env::set_var("CHAOS_DISK_FULL", "true");

        assert!(maybe_disk_full("test.rs"));

        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::remove_var("CHAOS_RATIO");
        std::env::remove_var("CHAOS_DISK_FULL");
    }

    /// ID SRS: SRS-TEST-CHAOS-015
    /// Version: V1.0
    #[tokio::test]
    #[serial]
    async fn network_jitter_returns_immediately_when_flag_unset() {
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        std::env::set_var("CHAOS_RATIO", "1.0");
        std::env::remove_var("CHAOS_NETWORK_JITTER");

        // Must return without sleeping — real (unpaused) time, so a hang would
        // time out the test rather than just being slow.
        maybe_network_jitter("test.rs").await;

        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::remove_var("CHAOS_RATIO");
    }

    /// ID SRS: SRS-TEST-CHAOS-016
    /// Version: V1.0
    #[tokio::test]
    #[serial]
    async fn network_jitter_returns_immediately_when_flag_set_but_chaos_disabled() {
        // CHAOS_NETWORK_JITTER alone isn't enough — the master switch/APP_ENV
        // guardrail must also allow chaos, same as every other failure mode.
        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::set_var("CHAOS_NETWORK_JITTER", "true");

        maybe_network_jitter("test.rs").await;

        std::env::remove_var("CHAOS_NETWORK_JITTER");
    }

    /// ID SRS: SRS-TEST-CHAOS-017
    /// Version: V1.0
    #[tokio::test]
    #[serial]
    async fn network_jitter_sleeps_when_triggered() {
        tokio::time::pause();
        std::env::set_var("APP_ENV", "development");
        std::env::set_var("ENABLE_CHAOS_MONKEY", "true");
        std::env::set_var("CHAOS_RATIO", "1.0");
        std::env::set_var("CHAOS_NETWORK_JITTER", "true");

        let handle = tokio::spawn(async {
            maybe_network_jitter("test.rs").await;
        });
        // Yield so the spawned task registers its sleep before we fast-forward.
        tokio::task::yield_now().await;
        // Max possible delay is 100 + 2900 = 3000ms; advance past it.
        tokio::time::advance(std::time::Duration::from_millis(3001)).await;
        handle.await.expect("jitter task must not panic");

        std::env::remove_var("APP_ENV");
        std::env::remove_var("ENABLE_CHAOS_MONKEY");
        std::env::remove_var("CHAOS_RATIO");
        std::env::remove_var("CHAOS_NETWORK_JITTER");
    }
}
