// /src/output/ble.rs
// Module: output.ble
// Purpose: BLE GATT server output using ble-windows-server with per-track characteristics
//
// ── LEGACY — DO NOT USE IN NEW CODE ─────────────────────────────────────────
// This module implements the V3 state-sync BLE protocol (one characteristic per
// track, ble-windows-server crate).  It has been superseded by the IDT reliable
// protocol in `ble_reliable.rs` + `ble_session.rs` which provide per-stream
// sequence tracking, selective ACKs, and Flutter compatibility.
//
// This file is retained for reference only.  `BleOutput` is no longer
// instantiated by `core/processor.rs`; it is exported solely for historical
// continuity.  All active BLE output goes through `ReliableBleOutput`.
// ─────────────────────────────────────────────────────────────────────────────
#![allow(dead_code)]

use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use ble_windows_server::{Uuid, WindowsBLEGattServer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

/// ID SRS: SRS-MOD-BLE-V3-001
/// Title: BleOutput
///
/// Description: VRConnect shall provide BLE GATT server output transmitting
/// each selected track as an individual BLE characteristic using ble-windows-server.
///
/// Version: V3.0
pub struct BleOutput {
    server: Arc<RwLock<WindowsBLEGattServer>>,
    track_names: Vec<String>,
    empty_value: String,
    update_interval_ms: u64,
    current_data: Arc<RwLock<Option<ProcessedData>>>,
}

impl BleOutput {
    /// ID SRS: SRS-FN-BLE-V3-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a BleOutput instance with device
    /// name, service UUID, and one BLE characteristic per selected track.
    /// Characteristic UUIDs are derived from service_uuid + (track_index + 1).
    ///
    /// Version: V3.0
    ///
    /// # Arguments
    /// * `device_name` - BLE advertising name
    /// * `service_uuid_str` - Service UUID string
    /// * `track_values` - Comma-separated track names to transmit
    /// * `empty_value` - Value to use when track is missing
    /// * `update_interval_ms` - Update interval in milliseconds
    ///
    /// # Returns
    /// New BleOutput instance or error
    pub async fn new(
        device_name: String,
        service_uuid_str: String,
        track_values: String,
        empty_value: String,
        update_interval_ms: u64,
    ) -> Result<Self> {
        // Parse service UUID
        let service_uuid = Uuid::parse_str(&service_uuid_str)
            .map_err(|e| VitalError::Config(format!("Invalid BLE service UUID: {}", e)))?;

        // Parse track names (case-insensitive, trimmed)
        let track_names: Vec<String> = track_values
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect();

        if track_names.is_empty() {
            return Err(VitalError::Config(
                "BLE_VALUES cannot be empty when BLE is enabled".to_string(),
            ));
        }

        log::info!("BLE Output Configuration:");
        log::info!("  Device Name: {}", device_name);
        log::info!("  Service UUID: {}", service_uuid);
        log::info!("  Tracks: {:?}", track_names);
        log::info!("  Empty Value: '{}'", empty_value);
        log::info!("  Update Interval: {}ms", update_interval_ms);

        // Create BLE server and register one characteristic per track
        let mut server = WindowsBLEGattServer::new(device_name, service_uuid);

        for (i, track_name) in track_names.iter().enumerate() {
            let char_uuid = Uuid::from_u128(service_uuid.as_u128() + (i as u128) + 1);
            server.add_characteristic(track_name, char_uuid, track_name);
            log::info!("  Characteristic: {} -> {}", track_name, char_uuid);
        }

        Ok(Self {
            server: Arc::new(RwLock::new(server)),
            track_names,
            empty_value,
            update_interval_ms,
            current_data: Arc::new(RwLock::new(None)),
        })
    }

    /// ID SRS: SRS-FN-BLE-V3-002
    /// Title: start
    ///
    /// Description: VRConnect shall start BLE GATT server and begin periodic
    /// per-characteristic data transmission using the configured update interval.
    ///
    /// Version: V3.0
    ///
    /// # Returns
    /// Result indicating success or error
    pub async fn start(&self) -> Result<()> {
        log::info!("Starting BLE GATT server...");

        // Clone for the update loop
        let server_for_updates = self.server.clone();
        let current_data = self.current_data.clone();
        let track_names = self.track_names.clone();
        let empty_value = self.empty_value.clone();
        let interval_ms = self.update_interval_ms;

        // Start the server in a separate task
        let server_for_run = self.server.clone();
        tokio::spawn(async move {
            log::info!("BLE server task started");
            let mut server = server_for_run.write().await;

            match server.start().await {
                Ok(_) => {
                    log::info!("BLE server completed normally");
                }
                Err(e) => {
                    log::error!("BLE server failed: {:?}", e);
                    log::error!("Error details: {}", e);
                    if e.to_string().contains("No Bluetooth adapters found") {
                        log::error!("Please ensure you have a Bluetooth adapter that supports BLE peripheral role.");
                    }
                }
            }
            log::info!("BLE server task ended");
        });

        // Wait a bit for server to initialize
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Start the periodic update loop
        tokio::spawn(async move {
            log::info!("BLE update loop started");
            let mut ticker = interval(Duration::from_millis(interval_ms));

            loop {
                ticker.tick().await;

                // Get current data
                let data_opt = current_data.read().await.clone();

                if let Some(data) = data_opt {
                    // Build per-track values
                    let track_values = Self::build_track_values(&data, &track_names, &empty_value);

                    // Send each track as its own characteristic notification
                    let server = server_for_updates.read().await;
                    for (name, value) in &track_values {
                        if let Err(e) = server.notify_str(name, value).await {
                            log::warn!("BLE notification failed for {}: {}", name, e);
                        } else {
                            log::debug!("BLE notify: {} = {}", name, value);
                        }
                    }
                }
            }
        });

        log::info!("BLE GATT server started successfully");
        log::info!("Waiting for BLE client connections...");

        Ok(())
    }

    /// ID SRS: SRS-FN-BLE-V3-003
    /// Title: output
    ///
    /// Description: VRConnect shall update current data buffer for BLE transmission,
    /// filtering tracks from room 0 (BED_01) only.
    ///
    /// Version: V3.0
    ///
    /// # Arguments
    /// * `data` - Processed vital data
    ///
    /// # Returns
    /// Result indicating success or error
    pub async fn output(&self, data: &ProcessedData) -> Result<()> {
        // Update current data buffer
        *self.current_data.write().await = Some(data.clone());
        Ok(())
    }

    /// ID SRS: SRS-FN-BLE-V3-004
    /// Title: build_track_values
    ///
    /// Description: VRConnect shall build a list of (track_name, value) pairs
    /// from selected tracks in room index 0. Each pair maps to its own BLE characteristic.
    ///
    /// Version: V3.0
    ///
    /// # Arguments
    /// * `data` - Processed vital data
    /// * `track_names` - List of track names to include
    /// * `empty_value` - Value to use when track is missing
    ///
    /// # Returns
    /// Vec of (track_name, value) pairs
    fn build_track_values(
        data: &ProcessedData,
        track_names: &[String],
        empty_value: &str,
    ) -> Vec<(String, String)> {
        // Build a map of track names to values (only from room 0)
        let mut track_map: HashMap<String, String> = HashMap::new();

        for track in &data.all_tracks {
            // Only include tracks from room 0 (first room)
            if track.room_index == 0 {
                let track_name_upper = track.name.to_uppercase();

                // Use raw_value if available (for numbers), otherwise use display_value
                let value = if let Some(raw) = track.raw_value {
                    format!("{:.0}", raw) // No decimals for BLE transmission
                } else {
                    track.display_value.clone()
                };

                track_map.insert(track_name_upper, value);
            }
        }

        // Build pairs in the order specified by track_names
        track_names
            .iter()
            .map(|name| {
                let value = track_map
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| empty_value.to_string());
                (name.clone(), value)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ProcessedRoom, ProcessedTrack, TrackType};
    use chrono::Utc;

    /// Helper to create test track
    fn create_test_track(
        name: &str,
        value: f64,
        room_index: i32,
        room_name: &str,
    ) -> ProcessedTrack {
        ProcessedTrack {
            name: name.to_string(),
            display_value: format!("{:.3}", value),
            raw_value: Some(value),
            unit: "unit".to_string(),
            timestamp: Utc::now(),
            room_index,
            room_name: room_name.to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        }
    }

    /// ID SRS: SRS-TEST-BLE-V3-001
    /// Title: Test track values building with all tracks present
    ///
    /// Description: VRConnect shall build correct per-track values when all
    /// requested tracks are present in room 0.
    ///
    /// Version: V3.0
    #[test]
    fn test_build_track_values_all_present() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                create_test_track("HR", 75.0, 0, "BED_01"),
                create_test_track("SPO2", 98.0, 0, "BED_01"),
                create_test_track("NIBP_SYS", 120.0, 0, "BED_01"),
            ],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let track_names = vec!["HR".to_string(), "SPO2".to_string(), "NIBP_SYS".to_string()];
        let empty_value = "null";

        let result = BleOutput::build_track_values(&data, &track_names, empty_value);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("HR".to_string(), "75".to_string()));
        assert_eq!(result[1], ("SPO2".to_string(), "98".to_string()));
        assert_eq!(result[2], ("NIBP_SYS".to_string(), "120".to_string()));
    }

    /// ID SRS: SRS-TEST-BLE-V3-002
    /// Title: Test track values building with missing tracks
    ///
    /// Description: VRConnect shall use empty_value for missing tracks.
    ///
    /// Version: V3.0
    #[test]
    fn test_build_track_values_missing_tracks() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                create_test_track("HR", 75.0, 0, "BED_01"),
                // SPO2 missing
                create_test_track("NIBP_SYS", 120.0, 0, "BED_01"),
            ],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let track_names = vec!["HR".to_string(), "SPO2".to_string(), "NIBP_SYS".to_string()];
        let empty_value = "null";

        let result = BleOutput::build_track_values(&data, &track_names, empty_value);

        assert_eq!(result[0], ("HR".to_string(), "75".to_string()));
        assert_eq!(result[1], ("SPO2".to_string(), "null".to_string()));
        assert_eq!(result[2], ("NIBP_SYS".to_string(), "120".to_string()));
    }

    /// ID SRS: SRS-TEST-BLE-V3-003
    /// Title: Test track values with empty empty_value
    ///
    /// Description: VRConnect shall support empty string as empty_value.
    ///
    /// Version: V3.0
    #[test]
    fn test_build_track_values_empty_value() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![create_test_track("HR", 75.0, 0, "BED_01")],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let track_names = vec!["HR".to_string(), "SPO2".to_string()];
        let empty_value = ""; // Empty string

        let result = BleOutput::build_track_values(&data, &track_names, empty_value);

        assert_eq!(result[0], ("HR".to_string(), "75".to_string()));
        assert_eq!(result[1], ("SPO2".to_string(), "".to_string()));
    }

    /// ID SRS: SRS-TEST-BLE-V3-004
    /// Title: Test case-insensitive track matching
    ///
    /// Description: VRConnect shall match track names case-insensitively.
    ///
    /// Version: V3.0
    #[test]
    fn test_build_track_values_case_insensitive() {
        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![
                create_test_track("hr", 75.0, 0, "BED_01"),      // lowercase
                create_test_track("SpO2", 98.0, 0, "BED_01"),    // mixed case
                create_test_track("NIBP_SYS", 120.0, 0, "BED_01"), // uppercase
            ],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);
        let track_names = vec!["HR".to_string(), "SPO2".to_string(), "NIBP_SYS".to_string()];
        let empty_value = "null";

        let result = BleOutput::build_track_values(&data, &track_names, empty_value);

        assert_eq!(result[0], ("HR".to_string(), "75".to_string()));
        assert_eq!(result[1], ("SPO2".to_string(), "98".to_string()));
        assert_eq!(result[2], ("NIBP_SYS".to_string(), "120".to_string()));
    }

    /// ID SRS: SRS-TEST-BLE-V3-005
    /// Title: Test room filtering (only room 0)
    ///
    /// Description: VRConnect shall only include tracks from room index 0.
    ///
    /// Version: V3.0
    #[test]
    fn test_build_track_values_room_filtering() {
        let data = ProcessedData::new(
            "VR-TEST".to_string(),
            vec![
                ProcessedRoom {
                    room_index: 0,
                    room_name: "BED_01".to_string(),
                    tracks: vec![
                        create_test_track("HR", 75.0, 0, "BED_01"),
                        create_test_track("SPO2", 98.0, 0, "BED_01"),
                    ],
                },
                ProcessedRoom {
                    room_index: 1,
                    room_name: "BED_02".to_string(),
                    tracks: vec![
                        create_test_track("HR", 80.0, 1, "BED_02"), // Different room
                        create_test_track("SPO2", 95.0, 1, "BED_02"),
                    ],
                },
            ],
        );

        let track_names = vec!["HR".to_string(), "SPO2".to_string()];
        let empty_value = "null";

        let result = BleOutput::build_track_values(&data, &track_names, empty_value);

        // Should only use values from room 0
        assert_eq!(result[0], ("HR".to_string(), "75".to_string()));
        assert_eq!(result[1], ("SPO2".to_string(), "98".to_string()));
    }

    /// ID SRS: SRS-TEST-BLE-V3-006
    /// Title: Test BleOutput creation with valid config
    ///
    /// Description: VRConnect shall create BleOutput with valid configuration
    /// and register one characteristic per track.
    ///
    /// Version: V3.0
    #[tokio::test]
    async fn test_ble_output_creation() {
        let result = BleOutput::new(
            "TestDevice".to_string(),
            "12345678-1234-5678-1234-567812345678".to_string(),
            "HR,SPO2,NIBP_SYS".to_string(),
            "null".to_string(),
            100,
        )
            .await;

        assert!(result.is_ok());
        let ble = result.unwrap();
        assert_eq!(ble.track_names.len(), 3);
        assert_eq!(ble.track_names[0], "HR");
        assert_eq!(ble.empty_value, "null");
        assert_eq!(ble.update_interval_ms, 100);
    }

    /// ID SRS: SRS-TEST-BLE-V3-007
    /// Title: Test BleOutput creation with invalid UUID
    ///
    /// Description: VRConnect shall return error for invalid service UUID.
    ///
    /// Version: V3.0
    #[tokio::test]
    async fn test_ble_output_invalid_uuid() {
        let result = BleOutput::new(
            "TestDevice".to_string(),
            "INVALID-UUID".to_string(),
            "HR,SPO2".to_string(),
            "null".to_string(),
            100,
        )
            .await;

        assert!(result.is_err());
    }

    /// ID SRS: SRS-TEST-BLE-V3-008
    /// Title: Test BleOutput creation with empty track values
    ///
    /// Description: VRConnect shall return error when track_values is empty.
    ///
    /// Version: V3.0
    #[tokio::test]
    async fn test_ble_output_empty_tracks() {
        let result = BleOutput::new(
            "TestDevice".to_string(),
            "12345678-1234-5678-1234-567812345678".to_string(),
            "".to_string(), // Empty
            "null".to_string(),
            100,
        )
            .await;

        assert!(result.is_err());
    }

    /// ID SRS: SRS-TEST-BLE-V3-009
    /// Title: Test output method updates data buffer
    ///
    /// Description: VRConnect shall update internal data buffer when output is called.
    ///
    /// Version: V3.0
    #[tokio::test]
    async fn test_output_updates_buffer() {
        let ble = BleOutput::new(
            "TestDevice".to_string(),
            "12345678-1234-5678-1234-567812345678".to_string(),
            "HR,SPO2".to_string(),
            "null".to_string(),
            100,
        )
            .await
            .unwrap();

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![create_test_track("HR", 75.0, 0, "BED_01")],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Before output, buffer should be None
        assert!(ble.current_data.read().await.is_none());

        // Call output
        ble.output(&data).await.unwrap();

        // After output, buffer should contain data
        assert!(ble.current_data.read().await.is_some());
    }

    /// ID SRS: SRS-TEST-BLE-V3-010
    /// Title: Test track name trimming and parsing
    ///
    /// Description: VRConnect shall trim whitespace from track names.
    ///
    /// Version: V3.0
    #[tokio::test]
    async fn test_track_name_parsing() {
        let ble = BleOutput::new(
            "TestDevice".to_string(),
            "12345678-1234-5678-1234-567812345678".to_string(),
            " HR , SPO2 , NIBP_SYS ".to_string(), // With spaces
            "null".to_string(),
            100,
        )
            .await
            .unwrap();

        assert_eq!(ble.track_names.len(), 3);
        assert_eq!(ble.track_names[0], "HR");
        assert_eq!(ble.track_names[1], "SPO2");
        assert_eq!(ble.track_names[2], "NIBP_SYS");
    }
}
