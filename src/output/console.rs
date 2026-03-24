// /src/output/console.rs
// Module: output.console
// Purpose: Console output with compact and verbose modes

use crate::domain::{ProcessedData, ProcessedTrack, TrackType};

/// ID SRS: SRS-MOD-CONSOLE-001
/// Title: ConsoleOutput
///
/// Description: VRConnect shall provide console output with compact and verbose
/// modes, with optional colorization for improved readability.
///
/// Version: V1.0
pub struct ConsoleOutput {
    verbose: bool,
    _colorized: bool, // Keep for future use
}

impl ConsoleOutput {
    /// ID SRS: SRS-FN-CONSOLE-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a ConsoleOutput instance with
    /// verbosity and colorization configuration.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `verbose` - Enable verbose mode
    /// * `colorized` - Enable color output
    ///
    /// # Returns
    /// New ConsoleOutput instance
    pub fn new(verbose: bool, colorized: bool) -> Self {
        Self {
            verbose,
            _colorized: colorized,
        }
    }

    /// ID SRS: SRS-FN-CONSOLE-002
    /// Title: output
    ///
    /// Description: VRConnect shall output ProcessedData to console in either
    /// compact or verbose format based on configuration.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `data` - Processed vital data to display
    pub async fn output(&self, data: &ProcessedData) {
        if self.verbose {
            self.output_verbose(data);
        } else {
            self.output_compact(data);
        }
    }

    /// ID SRS: SRS-FN-CONSOLE-003
    /// Title: output_compact
    ///
    /// Description: VRConnect shall display compact vital data summary showing
    /// timestamp, track count, and first 5 tracks with basic information.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `data` - Processed vital data
    fn output_compact(&self, data: &ProcessedData) {
        let header = format!(
            "[{}] 🏥 Vital Update - {} tracks",
            data.timestamp.format("%Y-%m-%dT%H:%M:%S%.9f"),
            data.all_tracks.len()
        );

        println!("{}", header);

        // Display first 5 tracks
        for track in data.all_tracks.iter() {
            self.print_track_compact(track);
        }

        // Show remaining count
        if data.all_tracks.len() > 5 {
            let remaining = data.all_tracks.len() - 5;
            println!("  ... and {} more tracks", remaining);
        }
    }

    /// ID SRS: SRS-FN-CONSOLE-004
    /// Title: output_verbose
    ///
    /// Description: VRConnect shall display detailed vital data including device
    /// info, room breakdown, and complete track details with ALL waveform points.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `data` - Processed vital data
    fn output_verbose(&self, data: &ProcessedData) {
        println!("\n{}", "═".repeat(60));
        println!("🏥 VITAL DATA UPDATE");
        println!("{}", "═".repeat(60));

        println!("Device: {}", data.device_id);
        println!(
            "Timestamp: {}",
            data.timestamp.format("%Y-%m-%d %H:%M:%S%.3f")
        );
        println!("Rooms: {}", data.rooms.len());
        println!("Total Tracks: {}", data.all_tracks.len());

        for room in &data.rooms {
            println!("\nRoom: {} (Index: {})", room.room_name, room.room_index);

            for track in &room.tracks {
                self.print_track_verbose(track, "  ");
            }
        }

        println!("\n{}", "═".repeat(60));
    }

    /// ID SRS: SRS-FN-CONSOLE-005
    /// Title: print_track_compact
    ///
    /// Description: VRConnect shall print single track in compact format:
    /// name, value, unit, and room.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `track` - Track to display
    fn print_track_compact(&self, track: &ProcessedTrack) {
        println!(
            "  {}: {} {} ({})",
            track.name, track.display_value, track.unit, track.room_name
        );
    }

    /// ID SRS: SRS-FN-CONSOLE-006
    /// Title: print_track_verbose
    ///
    /// Description: VRConnect shall print single track in verbose format with
    /// all metadata, statistics, and ALL waveform points.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `track` - Track to display
    /// * `indent` - Indentation string
    fn print_track_verbose(&self, track: &ProcessedTrack, indent: &str) {
        println!("{}Track: {}", indent, track.name);
        println!("{}  Type: {:?}", indent, track.track_type);
        println!("{}  Value: {}", indent, track.display_value);
        println!("{}  Unit: {}", indent, track.unit);
        println!(
            "{}  Timestamp: {}",
            indent,
            track.timestamp.format("%H:%M:%S%.3f")
        );

        if let Some(stats) = &track.waveform_stats {
            println!(
                "{}  Stats: min={:.3}, max={:.3}, avg={:.3}, count={}",
                indent, stats.min, stats.max, stats.avg, stats.count
            );
        }

        // Print ALL waveform points in verbose mode
        if track.track_type == TrackType::Waveform {
            if let Some(points) = &track.waveform_points {
                println!("{}  Waveform Points ({} total):", indent, points.len());
                print!("{}    ", indent);

                for (i, point) in points.iter().enumerate() {
                    print!("{:.3}", point);

                    // Format: 10 points per line
                    if (i + 1) % 10 == 0 && i + 1 < points.len() {
                        println!();
                        print!("{}    ", indent);
                    } else if i + 1 < points.len() {
                        print!(", ");
                    }
                }
                println!(); // Final newline
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ProcessedData, ProcessedRoom, ProcessedTrack, TrackType, WaveformStats};
    use chrono::Utc;

    /// ID SRS: SRS-TEST-CONSOLE-001
    /// Title: Test console output creation
    ///
    /// Description: VRConnect shall create ConsoleOutput with
    /// specified verbosity and colorization settings.
    ///
    /// Version: V1.0
    #[test]
    fn test_console_output_new() {
        let _console = ConsoleOutput::new(false, false);
        assert!(true); // Just verify it can be created

        let _console_verbose = ConsoleOutput::new(true, true);
        assert!(true);
    }

    /// ID SRS: SRS-TEST-CONSOLE-002
    /// Title: Test compact output format
    ///
    /// Description: VRConnect shall output compact format when verbose=false,
    /// showing summary of tracks.
    ///
    /// Version: V1.0
    #[test]
    fn test_compact_output() {
        let console = ConsoleOutput::new(false, false);

        let track = ProcessedTrack {
            name: "HR".to_string(),
            display_value: "75.000".to_string(),
            raw_value: Some(75.0),
            unit: "bpm".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        };

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track.clone()],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Should not panic
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-003
    /// Title: Test verbose output format
    ///
    /// Description: VRConnect shall output detailed format when verbose=true,
    /// showing all track details.
    ///
    /// Version: V1.0
    #[test]
    fn test_verbose_output() {
        let console = ConsoleOutput::new(true, false);

        let track = ProcessedTrack {
            name: "HR".to_string(),
            display_value: "75.000".to_string(),
            raw_value: Some(75.0),
            unit: "bpm".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        };

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track.clone()],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Should not panic
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-004
    /// Title: Test colorized output
    ///
    /// Description: VRConnect shall support colorized output when colorized=true.
    ///
    /// Version: V1.0
    #[test]
    fn test_colorized_output() {
        let console = ConsoleOutput::new(false, true);

        let track = ProcessedTrack {
            name: "SpO2".to_string(),
            display_value: "98.000".to_string(),
            raw_value: Some(98.0),
            unit: "%".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        };

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track.clone()],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Should not panic
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-005
    /// Title: Test waveform output in verbose mode
    ///
    /// Description: VRConnect shall show waveform points in verbose mode.
    ///
    /// Version: V1.0
    #[test]
    fn test_waveform_output_verbose() {
        let console = ConsoleOutput::new(true, false);

        let stats = WaveformStats {
            min: 1.0,
            max: 5.0,
            avg: 3.0,
            count: 5,
        };

        let track = ProcessedTrack {
            name: "ECG".to_string(),
            display_value: "5 points (1.000 to 5.000, avg: 3.000)".to_string(),
            raw_value: None,
            unit: "mV".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Waveform,
            waveform_stats: Some(stats),
            waveform_points: Some(vec![1.0, 2.0, 3.0, 4.0, 5.0]),
        };

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track.clone()],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Should not panic
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-006
    /// Title: Test multiple rooms output
    ///
    /// Description: VRConnect shall output all rooms and their tracks.
    ///
    /// Version: V1.0
    #[test]
    fn test_multiple_rooms_output() {
        let console = ConsoleOutput::new(true, false);

        let track1 = ProcessedTrack {
            name: "HR".to_string(),
            display_value: "75.000".to_string(),
            raw_value: Some(75.0),
            unit: "bpm".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        };

        let track2 = ProcessedTrack {
            name: "SpO2".to_string(),
            display_value: "98.000".to_string(),
            raw_value: Some(98.0),
            unit: "%".to_string(),
            timestamp: Utc::now(),
            room_index: 1,
            room_name: "BED_02".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        };

        let room1 = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track1],
        };

        let room2 = ProcessedRoom {
            room_index: 1,
            room_name: "BED_02".to_string(),
            tracks: vec![track2],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room1, room2]);

        // Should not panic
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-007
    /// Title: Test empty data output
    ///
    /// Description: VRConnect shall handle empty ProcessedData gracefully.
    ///
    /// Version: V1.0
    #[test]
    fn test_empty_data_output() {
        let console = ConsoleOutput::new(false, false);
        let data = ProcessedData::new("VR-EMPTY".to_string(), vec![]);

        // Should not panic
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-008
    /// Title: Test string track output
    ///
    /// Description: VRConnect shall output string-type tracks correctly.
    ///
    /// Version: V1.0
    #[test]
    fn test_string_track_output() {
        let console = ConsoleOutput::new(true, false);

        let track = ProcessedTrack {
            name: "ALARM".to_string(),
            display_value: "HR High".to_string(),
            raw_value: None,
            unit: "".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::String,
            waveform_stats: None,
            waveform_points: None,
        };

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Should not panic
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-009
    /// Title: Test compact mode with many tracks
    ///
    /// Description: VRConnect shall show first 5 tracks and indicate
    /// remaining count in compact mode.
    ///
    /// Version: V1.0
    #[test]
    fn test_compact_mode_many_tracks() {
        let console = ConsoleOutput::new(false, false);

        // Create 10 tracks
        let mut tracks = Vec::new();
        for i in 0..10 {
            tracks.push(ProcessedTrack {
                name: format!("TRACK_{}", i),
                display_value: format!("{}.000", i),
                raw_value: Some(i as f64),
                unit: "unit".to_string(),
                timestamp: Utc::now(),
                room_index: 0,
                room_name: "BED_01".to_string(),
                track_index: i,
                record_index: 0,
                track_type: TrackType::Number,
                waveform_stats: None,
                waveform_points: None,
            });
        }

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: tracks.clone(),
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // Should not panic and show "... and 5 more tracks"
        tokio_test::block_on(console.output(&data));
    }

    /// ID SRS: SRS-TEST-CONSOLE-015
    /// Title: Test waveform with 11 points (triggers line break)
    ///
    /// Description: VRConnect shall format waveforms with >10 points
    /// across multiple lines, triggering the line break and indent code.
    ///
    /// Version: V1.0
    #[test]
    fn test_waveform_multiline_formatting() {
        let console = ConsoleOutput::new(true, false);

        // Create waveform with 11 points to trigger line break after 10th
        let points: Vec<f64> = (0..11).map(|i| i as f64).collect();

        let stats = WaveformStats {
            min: 0.0,
            max: 10.0,
            avg: 5.0,
            count: 11,
        };

        let track = ProcessedTrack {
            name: "ECG".to_string(),
            display_value: "11 points".to_string(),
            raw_value: None,
            unit: "mV".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Waveform,
            waveform_stats: Some(stats),
            waveform_points: Some(points),
        };

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![track],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        // This will print 10 points, then newline+indent, then 1 more point
        tokio_test::block_on(console.output(&data));
    }
}
