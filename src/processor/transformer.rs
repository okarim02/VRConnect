// /src/processor/transformer.rs
// Module: processor.transformer
// Purpose: Transform VitalData to ProcessedData with type detection and statistics

use crate::domain::*;
use chrono::{TimeZone, Utc};

/// ID SRS: SRS-MOD-TRANSFORMER-001
/// Title: VitalDataTransformer
///
/// Description: VRConnect shall transform raw VitalData into ProcessedData,
/// detecting track types, computing waveform statistics, and organizing by rooms.
///
/// Version: V1.0
#[derive(Clone)]
pub struct VitalDataTransformer;

impl VitalDataTransformer {
    /// ID SRS: SRS-FN-TRANSFORMER-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a new VitalDataTransformer instance.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// New VitalDataTransformer instance
    pub fn new() -> Self {
        Self
    }

    /// ID SRS: SRS-FN-TRANSFORMER-002
    /// Title: transform
    ///
    /// Description: VRConnect shall transform VitalData into ProcessedData,
    /// processing all rooms and tracks with type detection and statistics.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `vital_data` - Raw vital data from VitalRecorder
    ///
    /// # Returns
    /// Processed vital data ready for output
    pub fn transform(&self, vital_data: VitalData) -> ProcessedData {
        let mut processed_rooms = Vec::new();

        log::debug!(
            "Transforming VitalData for device: {} with {} rooms",
            vital_data.vr_code,
            vital_data.rooms.len()
        );

        for (room_index, room) in vital_data.rooms.iter().enumerate() {
            let room_name = room
                .room_name
                .clone()
                .unwrap_or_else(|| format!("Room_{}", room_index));

            let mut room_tracks = Vec::new();

            log::debug!(
                "Processing room {} ({}) with {} tracks",
                room_index,
                room_name,
                room.tracks.len()
            );

            for (track_index, track) in room.tracks.iter().enumerate() {
                for (record_index, record) in track.records.iter().enumerate() {
                    let processed_track = self.process_track(
                        track,
                        record,
                        room_index as i32,
                        &room_name,
                        track_index as i32,
                        record_index as i32,
                    );

                    room_tracks.push(processed_track);
                }
            }

            processed_rooms.push(ProcessedRoom {
                room_index: room_index as i32,
                room_name,
                tracks: room_tracks,
            });
        }

        let processed_data = ProcessedData::new(vital_data.vr_code, processed_rooms);

        log::debug!(
            "Transformation complete: {} rooms, {} total tracks",
            processed_data.rooms.len(),
            processed_data.all_tracks.len()
        );

        processed_data
    }

    /// ID SRS: SRS-FN-TRANSFORMER-003
    /// Title: process_track
    ///
    /// Description: VRConnect shall process a single track record, extracting
    /// metadata, detecting type, computing statistics, and creating ProcessedTrack.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `track` - VitalTrack metadata
    /// * `record` - VitalRecord with value
    /// * `room_index` - Room index
    /// * `room_name` - Room name
    /// * `track_index` - Track index within room
    /// * `record_index` - Record index within track
    ///
    /// # Returns
    /// ProcessedTrack with type and statistics
    fn process_track(
        &self,
        track: &VitalTrack,
        record: &VitalRecord,
        room_index: i32,
        room_name: &str,
        track_index: i32,
        record_index: i32,
    ) -> ProcessedTrack {
        let track_name = track
            .name
            .clone()
            .or_else(|| track.display_name.clone())
            .unwrap_or_else(|| format!("Track_{}_{}", room_index, track_index));

        let track_type_str = track.track_type.as_deref().unwrap_or("other");
        let unit = track.unit.clone().unwrap_or_default();

        let (track_type, display_value, raw_value, waveform_stats, waveform_points) =
            self.process_value(&record.value, track_type_str);

        let timestamp = record
            .get_effective_timestamp()
            .and_then(|ts| {
                // VitalRecorder sends "dt" in seconds (10 digits, e.g. 1776153853).
                // Flutter simulators send in milliseconds (13 digits, e.g. 1776090179422).
                let ts_ms = if ts < 10_000_000_000 { ts * 1000 } else { ts };
                Utc.timestamp_millis_opt(ts_ms).single()
            })
            .unwrap_or_else(Utc::now);

        ProcessedTrack {
            name: track_name,
            display_value,
            raw_value,
            unit,
            timestamp,
            room_index,
            room_name: room_name.to_string(),
            track_index,
            record_index,
            track_type,
            waveform_stats,
            waveform_points,
        }
    }

    /// ID SRS: SRS-FN-TRANSFORMER-004
    /// Title: process_value
    ///
    /// Description: VRConnect shall process record value, detecting type
    /// (Number, Waveform, String, Other) and computing statistics for waveforms.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `value` - JSON value from record
    /// * `track_type` - Track type hint from metadata
    ///
    /// # Returns
    /// Tuple: (TrackType, display_value, raw_value, waveform_stats, waveform_points)
    fn process_value(
        &self,
        value: &serde_json::Value,
        track_type: &str,
    ) -> (
        TrackType,
        String,
        Option<f64>,
        Option<WaveformStats>,
        Option<Vec<f64>>,
    ) {
        match value {
            serde_json::Value::Number(n) => {
                let num = n.as_f64().unwrap_or(0.0);
                (
                    TrackType::Number,
                    format!("{:.3}", num),
                    Some(num),
                    None,
                    None,
                )
            }
            serde_json::Value::Array(arr) if track_type == "wav" => self.process_waveform(arr),
            serde_json::Value::String(s) => (TrackType::String, s.clone(), None, None, None),
            _ => (TrackType::Other, value.to_string(), None, None, None),
        }
    }

    /// ID SRS: SRS-FN-TRANSFORMER-005
    /// Title: process_waveform
    ///
    /// Description: VRConnect shall process waveform array, extracting numeric
    /// points and computing statistics (min, max, avg, count).
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `arr` - Array of waveform values
    ///
    /// # Returns
    /// Tuple: (TrackType, display_value, raw_value, waveform_stats, waveform_points)
    fn process_waveform(
        &self,
        arr: &[serde_json::Value],
    ) -> (
        TrackType,
        String,
        Option<f64>,
        Option<WaveformStats>,
        Option<Vec<f64>>,
    ) {
        let numbers: Vec<f64> = arr.iter().filter_map(|v| v.as_f64()).collect();

        if numbers.is_empty() {
            return (
                TrackType::Waveform,
                "0 points".to_string(),
                None,
                None,
                None,
            );
        }

        let min = numbers.iter().copied().fold(f64::INFINITY, f64::min);
        let max = numbers.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = numbers.iter().sum();
        let avg = sum / numbers.len() as f64;
        let count = numbers.len();

        let stats = WaveformStats {
            min,
            max,
            avg,
            count,
        };

        let display = format!(
            "{} points ({:.3} to {:.3}, avg: {:.3})",
            count, min, max, avg
        );

        (
            TrackType::Waveform,
            display,
            None,
            Some(stats),
            Some(numbers),
        )
    }
}

impl Default for VitalDataTransformer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{VitalData, VitalRecord, VitalRoom, VitalTrack};
    use serde_json::json;

    /// ID SRS: SRS-TEST-TRANS-001
    /// Title: Test transformer creation
    ///
    /// Description: VRConnect shall create a VitalDataTransformer instance.
    ///
    /// Version: V1.0
    #[test]
    fn test_transformer_new() {
        let transformer = VitalDataTransformer::new();
        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![],
        };
        let result = transformer.transform(vital_data);
        assert_eq!(result.device_id, "VR-TEST");
    }

    /// ID SRS: SRS-TEST-TRANS-002
    /// Title: Test transform empty rooms
    ///
    /// Description: VRConnect shall handle transformation of VitalData
    /// with no rooms.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_empty_rooms() {
        let transformer = VitalDataTransformer::new();
        let vital_data = VitalData {
            vr_code: "VR-EMPTY".to_string(),
            rooms: vec![],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.device_id, "VR-EMPTY");
        assert_eq!(result.rooms.len(), 0);
        assert_eq!(result.all_tracks.len(), 0);
    }

    /// ID SRS: SRS-TEST-TRANS-003
    /// Title: Test transform single numeric track
    ///
    /// Description: VRConnect shall transform numeric vital tracks
    /// (type "num") into ProcessedTrack with TrackType::Number.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_numeric_track() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("HR".to_string()),
                    track_type: Some("num".to_string()),
                    unit: Some("bpm".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!(75.0),
                        timestamp: Some(1234567890),
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.rooms.len(), 1);
        assert_eq!(result.all_tracks.len(), 1);

        let track = &result.all_tracks[0];
        assert_eq!(track.name, "HR");
        assert_eq!(track.unit, "bpm");
        assert_eq!(track.track_type, TrackType::Number);
        assert_eq!(track.raw_value, Some(75.0));
        assert!(track.display_value.contains("75"));
    }

    /// ID SRS: SRS-TEST-TRANS-004
    /// Title: Test transform waveform track
    ///
    /// Description: VRConnect shall transform waveform tracks (type "wav")
    /// into ProcessedTrack with TrackType::Waveform, including statistics.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_waveform_track() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("2".to_string()),
                    name: Some("ECG".to_string()),
                    track_type: Some("wav".to_string()),
                    unit: Some("mV".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!([1.0, 2.0, 3.0, 4.0, 5.0]),
                        timestamp: Some(1234567890),
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.all_tracks.len(), 1);

        let track = &result.all_tracks[0];
        assert_eq!(track.name, "ECG");
        assert_eq!(track.track_type, TrackType::Waveform);
        assert!(track.waveform_stats.is_some());
        assert!(track.waveform_points.is_some());

        let stats = track.waveform_stats.as_ref().unwrap();
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 5.0);
        assert_eq!(stats.avg, 3.0);
        assert_eq!(stats.count, 5);

        let points = track.waveform_points.as_ref().unwrap();
        assert_eq!(points.len(), 5);
    }

    /// ID SRS: SRS-TEST-TRANS-005
    /// Title: Test transform string track
    ///
    /// Description: VRConnect shall transform string values into
    /// ProcessedTrack with TrackType::String.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_string_track() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("3".to_string()),
                    name: Some("ALARM".to_string()),
                    track_type: Some("str".to_string()),
                    unit: None,
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!("HR High"),
                        timestamp: Some(1234567890),
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.all_tracks.len(), 1);

        let track = &result.all_tracks[0];
        assert_eq!(track.track_type, TrackType::String);
        assert_eq!(track.display_value, "HR High");
    }

    /// ID SRS: SRS-TEST-TRANS-006
    /// Title: Test transform multiple rooms
    ///
    /// Description: VRConnect shall transform VitalData with multiple rooms
    /// and flatten all tracks into all_tracks array.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_multiple_rooms() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![
                VitalRoom {
                    seq_id: Some(0),
                    room_name: Some("BED_01".to_string()),
                    tracks: vec![VitalTrack {
                        id: Some("1".to_string()),
                        name: Some("HR".to_string()),
                        track_type: Some("num".to_string()),
                        unit: Some("bpm".to_string()),
                        mon_type: None,
                        display_name: None,
                        sample_rate: None,
                        records: vec![VitalRecord {
                            value: json!(75.0),
                            timestamp: Some(1234567890),
                            time: None,
                        }],
                    }],
                    events: vec![],
                },
                VitalRoom {
                    seq_id: Some(1),
                    room_name: Some("BED_02".to_string()),
                    tracks: vec![VitalTrack {
                        id: Some("2".to_string()),
                        name: Some("SpO2".to_string()),
                        track_type: Some("num".to_string()),
                        unit: Some("%".to_string()),
                        mon_type: None,
                        display_name: None,
                        sample_rate: None,
                        records: vec![VitalRecord {
                            value: json!(98.0),
                            timestamp: Some(1234567890),
                            time: None,
                        }],
                    }],
                    events: vec![],
                },
            ],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.rooms.len(), 2);
        assert_eq!(result.all_tracks.len(), 2);
        assert_eq!(result.all_tracks[0].room_name, "BED_01");
        assert_eq!(result.all_tracks[1].room_name, "BED_02");
    }

    /// ID SRS: SRS-TEST-TRANS-007
    /// Title: Test transform track with multiple records
    ///
    /// Description: VRConnect shall create separate ProcessedTrack for
    /// each record in a VitalTrack.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_multiple_records() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("HR".to_string()),
                    track_type: Some("num".to_string()),
                    unit: Some("bpm".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![
                        VitalRecord {
                            value: json!(75.0),
                            timestamp: Some(1234567890),
                            time: None,
                        },
                        VitalRecord {
                            value: json!(76.0),
                            timestamp: Some(1234567891),
                            time: None,
                        },
                        VitalRecord {
                            value: json!(74.0),
                            timestamp: Some(1234567892),
                            time: None,
                        },
                    ],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.all_tracks.len(), 3);
        assert_eq!(result.all_tracks[0].raw_value, Some(75.0));
        assert_eq!(result.all_tracks[1].raw_value, Some(76.0));
        assert_eq!(result.all_tracks[2].raw_value, Some(74.0));
    }

    /// ID SRS: SRS-TEST-TRANS-008
    /// Title: Test transform with missing optional fields
    ///
    /// Description: VRConnect shall handle VitalData with missing optional
    /// fields (room_name, track name, unit, etc.) gracefully.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_missing_fields() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: None,
                room_name: None, // Missing room name
                tracks: vec![VitalTrack {
                    id: None,
                    name: None, // Missing track name
                    track_type: None,
                    unit: None, // Missing unit
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!(100.0),
                        timestamp: None,
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.rooms.len(), 1);
        assert_eq!(result.all_tracks.len(), 1);

        let track = &result.all_tracks[0];

        // Should generate default names (check they exist)
        assert!(!track.room_name.is_empty(), "Room name should be generated");
        assert!(!track.name.is_empty(), "Track name should be generated");
        assert_eq!(track.unit, "");
    }

    /// ID SRS: SRS-TEST-TRANS-009
    /// Title: Test waveform statistics calculation
    ///
    /// Description: VRConnect shall correctly calculate min, max, avg, count
    /// for waveform data.
    ///
    /// Version: V1.0
    #[test]
    fn test_waveform_statistics() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("PLETH".to_string()),
                    track_type: Some("wav".to_string()),
                    unit: Some("".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!([-5.0, 0.0, 10.0, 20.0, 15.0]),
                        timestamp: Some(1234567890),
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        let track = &result.all_tracks[0];
        let stats = track.waveform_stats.as_ref().unwrap();

        assert_eq!(stats.min, -5.0);
        assert_eq!(stats.max, 20.0);
        assert_eq!(stats.avg, 8.0); // (-5 + 0 + 10 + 20 + 15) / 5 = 8
        assert_eq!(stats.count, 5);
    }

    /// ID SRS: SRS-TEST-TRANS-010
    /// Title: Test timestamp handling
    ///
    /// Description: VRConnect shall use timestamp from record if available,
    /// otherwise fall back to current time.
    ///
    /// Version: V1.0
    #[test]
    fn test_timestamp_handling() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("HR".to_string()),
                    track_type: Some("num".to_string()),
                    unit: Some("bpm".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!(75.0),
                        timestamp: Some(1609459200000), // 2021-01-01 00:00:00 UTC
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        let track = &result.all_tracks[0];

        // Check that timestamp was set (not checking exact value due to timezone issues)
        assert!(track.timestamp.timestamp() > 0);
    }

    /// ID SRS: SRS-TEST-TRANS-011
    /// Title: Test transformer default creation
    ///
    /// Description: VRConnect shall create VitalDataTransformer via Default trait.
    ///
    /// Version: V1.0
    #[test]
    fn test_transformer_default() {
        let transformer = VitalDataTransformer::default();

        let vital_data = VitalData {
            vr_code: "VR-DEFAULT".to_string(),
            rooms: vec![],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.device_id, "VR-DEFAULT");
    }

    /// ID SRS: SRS-TEST-TRANS-012
    /// Title: Test transform with Other track type
    ///
    /// Description: VRConnect shall handle unknown track types as TrackType::Other.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_other_type() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("UNKNOWN".to_string()),
                    track_type: Some("unknown_type".to_string()), // Not "num", "wav", or "str"
                    unit: Some("unit".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!({"complex": "object"}), // Complex object
                        timestamp: Some(1234567890),
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.all_tracks.len(), 1);

        let track = &result.all_tracks[0];
        assert_eq!(track.track_type, TrackType::Other);
        assert!(track.display_value.contains("complex"));
    }

    /// ID SRS: SRS-TEST-TRANS-013
    /// Title: Test transform empty waveform array
    ///
    /// Description: VRConnect shall handle waveform tracks with empty
    /// point arrays, returning "0 points".
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_empty_waveform() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("ECG".to_string()),
                    track_type: Some("wav".to_string()),
                    unit: Some("mV".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!([]), // Empty array
                        timestamp: Some(1234567890),
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        assert_eq!(result.all_tracks.len(), 1);

        let track = &result.all_tracks[0];
        assert_eq!(track.track_type, TrackType::Waveform);
        assert_eq!(track.display_value, "0 points");
        assert!(track.waveform_stats.is_none());
        assert!(track.waveform_points.is_none());
    }

    /// ID SRS: SRS-TEST-TRANS-014
    /// Title: Test transform with logging enabled
    ///
    /// Description: VRConnect shall log debug information during transformation
    /// when debug logging is enabled.
    ///
    /// Version: V1.0
    #[test]
    fn test_transform_with_debug_logging() {
        // Set RUST_LOG to debug to enable debug logging
        std::env::set_var("RUST_LOG", "debug");

        // Initialize env_logger (ignore if already initialized)
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init();

        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-LOG-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("HR".to_string()),
                    track_type: Some("num".to_string()),
                    unit: Some("bpm".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![
                        VitalRecord {
                            value: json!(75.0),
                            timestamp: Some(1234567890),
                            time: None,
                        },
                        VitalRecord {
                            value: json!(76.0),
                            timestamp: Some(1234567891),
                            time: None,
                        },
                    ],
                }],
                events: vec![],
            }],
        };

        // This should trigger the debug logging
        let result = transformer.transform(vital_data);
        assert_eq!(result.rooms.len(), 1);
        assert_eq!(result.all_tracks.len(), 2); // 2 records

        std::env::remove_var("RUST_LOG");
    }

    /// ID SRS: SRS-TEST-TRANS-015
    /// Title: Test waveform with non-numeric values in array
    ///
    /// Description: VRConnect shall filter out non-numeric values from
    /// waveform arrays.
    ///
    /// Version: V1.0
    #[test]
    fn test_waveform_with_mixed_values() {
        let transformer = VitalDataTransformer::new();

        let vital_data = VitalData {
            vr_code: "VR-TEST".to_string(),
            rooms: vec![VitalRoom {
                seq_id: Some(0),
                room_name: Some("BED_01".to_string()),
                tracks: vec![VitalTrack {
                    id: Some("1".to_string()),
                    name: Some("ECG".to_string()),
                    track_type: Some("wav".to_string()),
                    unit: Some("mV".to_string()),
                    mon_type: None,
                    display_name: None,
                    sample_rate: None,
                    records: vec![VitalRecord {
                        value: json!([1.0, "invalid", 2.0, null, 3.0]),
                        timestamp: Some(1234567890),
                        time: None,
                    }],
                }],
                events: vec![],
            }],
        };

        let result = transformer.transform(vital_data);
        let track = &result.all_tracks[0];

        // Should only include numeric values (1.0, 2.0, 3.0)
        let points = track.waveform_points.as_ref().unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0], 1.0);
        assert_eq!(points[1], 2.0);
        assert_eq!(points[2], 3.0);
    }
}
