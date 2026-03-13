// /src/utils/chaos.rs
// Module: utils.chaos
// Purpose: Chaos Monkey — probabilistic fault injection for non-production testing.
//
// Env variables:
//   APP_ENV              — set to "production"/"prod" to permanently disable all chaos.
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
///   - `APP_ENV` is NOT "production" / "prod"   (safety guardrail)
///   - `ENABLE_CHAOS_MONKEY=true`               (master switch)
fn is_enabled() -> bool {
    // Safety guardrail: never run in production.
    let app_env = std::env::var("APP_ENV").unwrap_or_default();
    if matches!(app_env.to_lowercase().as_str(), "production" | "prod") {
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
