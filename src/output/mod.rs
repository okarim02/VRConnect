// /src/output/mod.rs
// Module: output
// Purpose: Output modules for console, BLE, and file recording

pub mod ble; // Legacy state-sync BLE output (kept for history)
pub mod ble_gatt; // Custom GATT server with Write callback support
pub mod ble_reliable; // Reliable stream BLE output (active protocol)
pub mod ble_session; // Sliding window engine (protocol state)
pub mod console;
pub mod file;
pub mod health; // Health monitoring — OS snapshot + GATE state → BLE Control notify
pub mod wal; // Write-Ahead Log entry format + replay (WAL file I/O in ble_reliable)

pub use ble::BleOutput;
pub use ble_reliable::ReliableBleOutput;
pub use console::ConsoleOutput;
pub use file::FileOutput;
