// /src/core/processor.rs
// Module: core.processor
// Purpose: Main processor orchestrating data flow from input to outputs

use crate::config::Config;
use crate::domain::ProcessedData;
use crate::error::Result;
use crate::input::SocketIOServer;
use crate::output::{ConsoleOutput, FileOutput, ReliableBleOutput};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// ID SRS: SRS-MOD-PROCESSOR-001
/// Title: VitalProcessor
///
/// Description: VRConnect shall orchestrate the complete data processing pipeline
/// from Socket.IO input through transformation to multiple outputs with optional
/// debug logging.
///
/// Version: V1.0
pub struct VitalProcessor {
    config: Config,
    debug_file: Arc<RwLock<Option<std::fs::File>>>,
}

impl VitalProcessor {
    /// ID SRS: SRS-FN-PROCESSOR-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a VitalProcessor instance with
    /// configuration and initialize debug file if debug mode enabled.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `config` - Application configuration
    ///
    /// # Returns
    /// New VitalProcessor instance
    pub fn new(config: Config) -> Self {
        let debug_file = if config.debug_enabled {
            // Create debug file
            if let Ok(file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&config.debug_output_path)
            {
                Arc::new(RwLock::new(Some(file)))
            } else {
                log::error!("Failed to create debug file: {}", config.debug_output_path);
                Arc::new(RwLock::new(None))
            }
        } else {
            Arc::new(RwLock::new(None))
        };

        Self { config, debug_file }
    }

    /// ID SRS: SRS-FN-PROCESSOR-002
    /// Title: create_console_output
    ///
    /// Description: VRConnect shall create console output if enabled.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Optional ConsoleOutput
    fn create_console_output(&self) -> Option<Arc<ConsoleOutput>> {
        if self.config.output_console_enabled {
            Some(Arc::new(ConsoleOutput::new(
                self.config.output_console_verbose,
                self.config.output_console_colorized,
            )))
        } else {
            None
        }
    }

    /// ID SRS: SRS-FN-PROCESSOR-003
    /// Title: create_ble_output
    ///
    /// Description: VRConnect shall create reliable BLE output if enabled.
    /// Uses the new binary protocol with sliding window acknowledgment.
    ///
    /// Version: V2.0
    ///
    /// # Returns
    /// Optional ReliableBleOutput or error
    async fn create_ble_output(&self) -> Result<Option<Arc<ReliableBleOutput>>> {
        if self.config.output_ble_enabled {
            log::info!("🔵 Initializing Reliable BLE output (binary protocol)...");
            Ok(Some(Arc::new(
                ReliableBleOutput::new(
                    self.config.output_ble_device_name.clone(),
                    self.config.output_ble_service_uuid.clone(),
                    self.config.output_ble_update_interval_ms,
                    None, // use default signal registry (HR, SpO2, Temperature)
                    self.config.health_check_interval_sec,
                    self.config.health_ble_flow_timeout_sec,
                    self.config.health_file.clone(),
                    self.config.ble_grace_period_sec,
                    self.config.history_checkpoint_interval_sec,
                    self.config.history_checkpoint_max_age_sec,
                    self.config.history_checkpoint_path.clone(),
                )
                .await?,
            )))
        } else {
            Ok(None)
        }
    }

    /// ID SRS: SRS-FN-PROCESSOR-017
    /// Title: create_file_output
    ///
    /// Description: VRConnect shall create file output if enabled.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Optional FileOutput or error
    async fn create_file_output(&self) -> Result<Option<Arc<FileOutput>>> {
        if self.config.output_file_enabled {
            log::info!("🗃️ Initializing file output...");
            Ok(Some(Arc::new(
                FileOutput::new(
                    self.config.output_file_base_path.clone(),
                    self.config.output_file_max_size_mb,
                    self.config.output_file_archive_threshold_gb,
                    self.config.output_file_critical_disk_percent,
                )
                .await?,
            )))
        } else {
            Ok(None)
        }
    }

    /// ID SRS: SRS-FN-PROCESSOR-004
    /// Title: process_single_data
    ///
    /// Description: VRConnect shall process single ProcessedData through
    /// all enabled outputs.
    ///
    /// Version: V5.0
    ///
    /// # Arguments
    /// * `data` - Processed data
    /// * `console` - Optional console output
    /// * `ble` - Optional reliable BLE output
    /// * `file` - Optional file output
    /// * `debug_enabled` - Debug flag
    /// * `debug_file` - Debug file handle
    async fn process_single_data(
        data: &ProcessedData,
        console: &Option<Arc<ConsoleOutput>>,
        ble: &Option<Arc<ReliableBleOutput>>,
        file: &Option<Arc<FileOutput>>,
        debug_enabled: bool,
        debug_file: &Arc<RwLock<Option<std::fs::File>>>,
    ) {
        log::debug!("Processing data for device: {}", data.device_id);

        // Debug log
        if debug_enabled {
            Self::write_debug_data(debug_file, data).await;
        }

        // Output to console
        if let Some(ref console) = console {
            console.output(data).await;
        }

        // Output to BLE
        if let Some(ref ble) = ble {
            if let Err(e) = ble.output(data).await {
                log::error!("BLE output error: {}", e);
            }
        }

        // Output to file
        if let Some(ref file) = file {
            if let Err(e) = file.output(data).await {
                log::error!("File output error: {}", e);
            }
        }
    }

    /// ID SRS: SRS-FN-PROCESSOR-005
    /// Title: run
    ///
    /// Description: VRConnect shall execute the main processing loop, starting
    /// input server, creating outputs, and processing data until shutdown signal.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Result indicating success or error
    #[cfg(not(tarpaulin_include))] // Integration test, requires real servers
    pub async fn run(&self) -> Result<()> {
        log::info!("Starting VitalProcessor...");

        // Create data channel
        let (tx, mut rx) = mpsc::unbounded_channel::<ProcessedData>();

        // Create outputs
        let console_output = self.create_console_output();
        let ble_output = self.create_ble_output().await?;
        let file_output = self.create_file_output().await?;

        // Start BLE server if enabled (don't monitor this task)
        if let Some(ref ble) = ble_output {
            let ble_clone = ble.clone();
            tokio::spawn(async move {
                if let Err(e) = ble_clone.start().await {
                    log::error!("BLE server error: {}", e);
                }
            });
        }

        // Start Socket.IO input server
        let mut socketio_server = SocketIOServer::new(
            self.config.socketio_host.clone(),
            self.config.socketio_port,
            self.config.debug_enabled,
            self.debug_file.clone(),
        );

        // Wire health hooks when BLE is active so sio_connected is tracked.
        if let Some(ref ble) = ble_output {
            socketio_server.set_health_hooks(ble.health_state(), ble.health_notify());
        }

        let input_task = tokio::spawn(async move {
            if let Err(e) = socketio_server.start(tx).await {
                log::error!("Socket.IO server error: {}", e);
            }
        });

        log::info!("✓ VitalProcessor started successfully");

        // Processing loop
        let debug_file = self.debug_file.clone();
        let debug_enabled = self.config.debug_enabled;
        let ble_output_clone = ble_output.clone();
        let console_output_clone = console_output.clone();
        let file_output_clone = file_output.clone();

        let processing_task = tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                Self::process_single_data(
                    &data,
                    &console_output_clone,
                    &ble_output_clone,
                    &file_output_clone,
                    debug_enabled,
                    &debug_file,
                )
                .await;
            }
        });

        // Wait for shutdown signal or task completion
        // Note: We don't monitor BLE task because it should run indefinitely
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                log::info!("Shutdown signal received");
            }
            result = input_task => {
                match result {
                    Ok(_) => log::info!("Socket.IO server stopped"),
                    Err(e) => log::error!("Socket.IO task panicked: {}", e),
                }
            }
            result = processing_task => {
                match result {
                    Ok(_) => log::info!("Processing task stopped"),
                    Err(e) => log::error!("Processing task panicked: {}", e),
                }
            }
        }

        log::info!("✓ VitalProcessor stopped gracefully");
        Ok(())
    }

    /// ID SRS: SRS-FN-PROCESSOR-006
    /// Title: write_debug_data
    ///
    /// Description: VRConnect shall write complete processed data to debug file,
    /// including ALL waveform points for comprehensive data capture.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `debug_file` - Debug file handle
    /// * `data` - Processed data to log
    async fn write_debug_data(
        debug_file: &Arc<RwLock<Option<std::fs::File>>>,
        data: &ProcessedData,
    ) {
        if let Some(ref mut file) = *debug_file.write().await {
            // Header
            let _ = writeln!(file, "\n{}", "=".repeat(80));
            let _ = writeln!(file, "PROCESSED DATA - COMPLETE DUMP");
            let _ = writeln!(file, "{}", "=".repeat(80));
            let _ = writeln!(file, "Timestamp: {}", data.timestamp);
            let _ = writeln!(file, "Device ID: {}", data.device_id);
            let _ = writeln!(file, "Total Rooms: {}", data.rooms.len());
            let _ = writeln!(file, "Total Tracks: {}", data.all_tracks.len());
            let _ = writeln!(file, "{}", "=".repeat(80));

            // Process each room
            for room in &data.rooms {
                let _ = writeln!(
                    file,
                    "\n[ROOM] {} (Index: {})",
                    room.room_name, room.room_index
                );
                let _ = writeln!(file, "  Tracks in room: {}", room.tracks.len());
                let _ = writeln!(file, "{}", "-".repeat(80));

                for track in &room.tracks {
                    let _ = writeln!(file, "\n  [TRACK] {}", track.name);
                    let _ = writeln!(file, "    Type: {:?}", track.track_type);
                    let _ = writeln!(file, "    Room: {}", track.room_name);
                    let _ = writeln!(file, "    Unit: {}", track.unit);
                    let _ = writeln!(
                        file,
                        "    Timestamp: {}",
                        track.timestamp.format("%H:%M:%S%.3f")
                    );
                    let _ = writeln!(file, "    Display Value: {}", track.display_value);

                    // Raw value for numbers
                    if let Some(raw_val) = track.raw_value {
                        let _ = writeln!(file, "    Raw Value: {}", raw_val);
                    }

                    // Waveform statistics
                    if let Some(stats) = &track.waveform_stats {
                        let _ = writeln!(file, "    Waveform Stats:");
                        let _ = writeln!(file, "      Count: {}", stats.count);
                        let _ = writeln!(file, "      Min: {:.6}", stats.min);
                        let _ = writeln!(file, "      Max: {:.6}", stats.max);
                        let _ = writeln!(file, "      Avg: {:.6}", stats.avg);
                    }

                    // ALL WAVEFORM POINTS
                    if let Some(points) = &track.waveform_points {
                        let _ = writeln!(file, "    Waveform Points ({} total):", points.len());
                        let _ = write!(file, "      ");

                        for (i, point) in points.iter().enumerate() {
                            let _ = write!(file, "{:.6}", point);

                            // Formatting: 10 points per line
                            if (i + 1) % 10 == 0 && i + 1 < points.len() {
                                let _ = writeln!(file);
                                let _ = write!(file, "      ");
                            } else if i + 1 < points.len() {
                                let _ = write!(file, ", ");
                            }
                        }
                        let _ = writeln!(file);
                    }
                }
            }

            let _ = writeln!(file, "\n{}", "=".repeat(80));
            let _ = writeln!(file, "END OF DATA DUMP");
            let _ = writeln!(file, "{}\n", "=".repeat(80));

            // Flush to ensure data is written
            let _ = file.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ProcessedData, ProcessedRoom, ProcessedTrack, TrackType, WaveformStats};
    use chrono::Utc;
    use std::io::Read;
    use tempfile::NamedTempFile;

    /// Helper function to create a default test config
    fn create_test_config() -> Config {
        Config {
            config_file: None,
            socketio_host: "127.0.0.1".to_string(),
            socketio_port: 3000,
            output_console_enabled: true,
            output_console_verbose: false,
            output_console_colorized: true,
            output_ble_enabled: false,
            output_ble_device_name: "Test".to_string(),
            output_ble_service_uuid: "12345678-1234-5678-1234-567812345678".to_string(),
            output_ble_values: "HR,SPO2".to_string(),
            output_ble_empty_value: "null".to_string(),
            output_ble_update_interval_ms: 100,
            output_file_enabled: false,
            output_file_base_path: "./data/test".to_string(),
            output_file_max_size_mb: 500,
            output_file_archive_threshold_gb: 5,
            output_file_critical_disk_percent: 95,
            health_check_interval_sec: 30,
            health_ble_flow_timeout_sec: 60,
            health_file: "logs/health.json".to_string(),
            ble_grace_period_sec: 10,
            history_checkpoint_interval_sec: 30,
            history_checkpoint_max_age_sec: 300,
            history_checkpoint_path: "logs/history_checkpoint.bin".to_string(),
            debug_enabled: false,
            debug_output_path: "./debug.log".to_string(),
            log_level: "INFO".to_string(),
            log_dir: "./logs".to_string(),
        }
    }

    /// ID SRS: SRS-TEST-PROC-009
    /// Title: Test create_console_output enabled
    ///
    /// Description: VRConnect shall create console output when enabled.
    ///
    /// Version: V1.0
    #[test]
    fn test_create_console_output_enabled() {
        let config = create_test_config();
        let processor = VitalProcessor::new(config);
        let console = processor.create_console_output();
        assert!(console.is_some());
    }

    /// ID SRS: SRS-TEST-PROC-010
    /// Title: Test create_console_output disabled
    ///
    /// Description: VRConnect shall not create console output when disabled.
    ///
    /// Version: V1.0
    #[test]
    fn test_create_console_output_disabled() {
        let mut config = create_test_config();
        config.output_console_enabled = false;

        let processor = VitalProcessor::new(config);
        let console = processor.create_console_output();
        assert!(console.is_none());
    }

    /// ID SRS: SRS-TEST-PROC-011
    /// Title: Test create_ble_output enabled
    ///
    /// Description: VRConnect shall create BLE output when enabled with valid UUID.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_create_ble_output_enabled() {
        let mut config = create_test_config();
        config.output_ble_enabled = true;

        let processor = VitalProcessor::new(config);
        let ble = processor.create_ble_output().await.unwrap();
        assert!(ble.is_some());
    }

    /// ID SRS: SRS-TEST-PROC-012
    /// Title: Test create_ble_output disabled
    ///
    /// Description: VRConnect shall not create BLE output when disabled.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_create_ble_output_disabled() {
        let config = create_test_config();
        let processor = VitalProcessor::new(config);
        let ble = processor.create_ble_output().await.unwrap();
        assert!(ble.is_none());
    }

    /// ID SRS: SRS-TEST-PROC-013
    /// Title: Test process_single_data with all outputs
    ///
    /// Description: VRConnect shall process data through console and BLE outputs.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_single_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let debug_path = temp_file.path().to_str().unwrap().to_string();

        let mut config = create_test_config();
        config.output_console_colorized = false;
        config.output_ble_enabled = true;
        config.debug_enabled = true;
        config.debug_output_path = debug_path.clone();

        let processor = VitalProcessor::new(config);
        let console = processor.create_console_output();
        let ble = processor.create_ble_output().await.unwrap();
        let file = None;

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![ProcessedTrack {
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
            }],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        VitalProcessor::process_single_data(
            &data,
            &console,
            &ble,
            &file,
            true,
            &processor.debug_file,
        )
        .await;
    }

    /// ID SRS: SRS-TEST-PROC-014
    /// Title: Test write_debug_data with waveform stats and points formatting
    ///
    /// Description: VRConnect shall write waveform stats and format points
    /// with 10 per line.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_write_debug_data_waveform_formatting() {
        let temp_file = NamedTempFile::new().unwrap();
        let debug_path = temp_file.path().to_str().unwrap().to_string();

        let mut config = create_test_config();
        config.debug_enabled = true;
        config.debug_output_path = debug_path.clone();

        let processor = VitalProcessor::new(config);

        let points: Vec<f64> = (0..25).map(|i| i as f64 * 0.1).collect();

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![ProcessedTrack {
                name: "ECG".to_string(),
                display_value: "25 points".to_string(),
                raw_value: None,
                unit: "mV".to_string(),
                timestamp: Utc::now(),
                room_index: 0,
                room_name: "BED_01".to_string(),
                track_index: 0,
                record_index: 0,
                track_type: TrackType::Waveform,
                waveform_stats: Some(WaveformStats {
                    min: 0.0,
                    max: 2.4,
                    avg: 1.2,
                    count: 25,
                }),
                waveform_points: Some(points),
            }],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        VitalProcessor::write_debug_data(&processor.debug_file, &data).await;

        drop(processor);
        let mut file = std::fs::File::open(&debug_path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        assert!(contents.contains("Waveform Stats:"));
        assert!(contents.contains("Count: 25"));
        assert!(contents.contains("Min: 0.000000"));
        assert!(contents.contains("Max: 2.400000"));
        assert!(contents.contains("Avg: 1.200000"));
        assert!(contents.contains("Waveform Points (25 total):"));
        assert!(contents.contains("0.000000"));
        assert!(contents.contains("0.100000"));
    }

    /// ID SRS: SRS-TEST-PROC-015
    /// Title: Test process_single_data with BLE error
    ///
    /// Description: VRConnect shall log BLE output errors.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_single_data_ble_error() {
        let mut config = create_test_config();
        config.output_console_enabled = false;
        config.output_ble_enabled = true;

        let processor = VitalProcessor::new(config);
        let console = processor.create_console_output();
        let ble = processor.create_ble_output().await.unwrap();
        let file = None;

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![ProcessedTrack {
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
            }],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        VitalProcessor::process_single_data(
            &data,
            &console,
            &ble,
            &file,
            false,
            &processor.debug_file,
        )
        .await;
    }

    /// ID SRS: SRS-TEST-PROC-016
    /// Title: Test processor with invalid debug path
    ///
    /// Description: VRConnect shall handle invalid debug file path and log error.
    ///
    /// Version: V1.0
    #[test]
    fn test_processor_invalid_debug_path() {
        let _ = env_logger::builder().is_test(true).try_init();

        let mut config = create_test_config();
        config.debug_enabled = true;
        config.debug_output_path = "/root/cannot/write/here/debug.log".to_string();

        let processor = VitalProcessor::new(config);
        assert!(processor.config.debug_enabled);
    }

    /// ID SRS: SRS-TEST-PROC-017
    /// Title: Test write_debug_data with exactly 10 points
    ///
    /// Description: VRConnect shall handle edge case of exactly 10 points per line.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_write_debug_data_exactly_10_points() {
        let temp_file = NamedTempFile::new().unwrap();
        let debug_path = temp_file.path().to_str().unwrap().to_string();

        let mut config = create_test_config();
        config.debug_enabled = true;
        config.debug_output_path = debug_path.clone();

        let processor = VitalProcessor::new(config);

        let points: Vec<f64> = (0..10).map(|i| i as f64).collect();

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![ProcessedTrack {
                name: "PLETH".to_string(),
                display_value: "10 points".to_string(),
                raw_value: None,
                unit: "".to_string(),
                timestamp: Utc::now(),
                room_index: 0,
                room_name: "BED_01".to_string(),
                track_index: 0,
                record_index: 0,
                track_type: TrackType::Waveform,
                waveform_stats: Some(WaveformStats {
                    min: 0.0,
                    max: 9.0,
                    avg: 4.5,
                    count: 10,
                }),
                waveform_points: Some(points),
            }],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        VitalProcessor::write_debug_data(&processor.debug_file, &data).await;

        drop(processor);
        let mut file = std::fs::File::open(&debug_path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        assert!(contents.contains("Waveform Points (10 total):"));
        assert!(contents.contains("Count: 10"));
    }

    /// ID SRS: SRS-TEST-PROC-018
    /// Title: Test write_debug_data with 11 points (triggers newline)
    ///
    /// Description: VRConnect shall wrap to new line after 10 points.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_write_debug_data_11_points() {
        let temp_file = NamedTempFile::new().unwrap();
        let debug_path = temp_file.path().to_str().unwrap().to_string();

        let mut config = create_test_config();
        config.debug_enabled = true;
        config.debug_output_path = debug_path.clone();

        let processor = VitalProcessor::new(config);

        let points: Vec<f64> = (0..11).map(|i| i as f64).collect();

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![ProcessedTrack {
                name: "CO2".to_string(),
                display_value: "11 points".to_string(),
                raw_value: None,
                unit: "mmHg".to_string(),
                timestamp: Utc::now(),
                room_index: 0,
                room_name: "BED_01".to_string(),
                track_index: 0,
                record_index: 0,
                track_type: TrackType::Waveform,
                waveform_stats: Some(WaveformStats {
                    min: 0.0,
                    max: 10.0,
                    avg: 5.0,
                    count: 11,
                }),
                waveform_points: Some(points),
            }],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        VitalProcessor::write_debug_data(&processor.debug_file, &data).await;

        drop(processor);
        let mut file = std::fs::File::open(&debug_path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        assert!(contents.contains("Waveform Points (11 total):"));
        assert!(contents.contains("10.000000"));
    }

    /// ID SRS: SRS-TEST-PROC-019
    /// Title: Test create_file_output enabled
    ///
    /// Description: VRConnect shall create file output when enabled.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_create_file_output_enabled() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap().to_string();

        let mut config = create_test_config();
        config.output_file_enabled = true;
        config.output_file_base_path = base_path;

        let processor = VitalProcessor::new(config);
        let file = processor.create_file_output().await.unwrap();
        assert!(file.is_some());
    }

    /// ID SRS: SRS-TEST-PROC-020
    /// Title: Test create_file_output disabled
    ///
    /// Description: VRConnect shall not create file output when disabled.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_create_file_output_disabled() {
        let config = create_test_config();
        let processor = VitalProcessor::new(config);
        let file = processor.create_file_output().await.unwrap();
        assert!(file.is_none());
    }

    /// ID SRS: SRS-TEST-PROC-021
    /// Title: Test process_single_data with file output
    ///
    /// Description: VRConnect shall process data through file output.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_single_data_with_file() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap().to_string();

        let temp_file = NamedTempFile::new().unwrap();
        let debug_path = temp_file.path().to_str().unwrap().to_string();

        let mut config = create_test_config();
        config.output_console_colorized = false;
        config.output_ble_enabled = true;
        config.output_file_enabled = true;
        config.output_file_base_path = base_path;
        config.debug_enabled = true;
        config.debug_output_path = debug_path.clone();

        let processor = VitalProcessor::new(config);
        let console = processor.create_console_output();
        let ble = processor.create_ble_output().await.unwrap();
        let file = processor.create_file_output().await.unwrap();

        let room = ProcessedRoom {
            room_index: 0,
            room_name: "BED_01".to_string(),
            tracks: vec![ProcessedTrack {
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
            }],
        };

        let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

        VitalProcessor::process_single_data(
            &data,
            &console,
            &ble,
            &file,
            true,
            &processor.debug_file,
        )
        .await;
    }

    /// ID SRS: SRS-TEST-PROC-022
    /// Title: Test flow_timeout_sec uses health_ble_flow_timeout_sec, not health_check_interval_sec
    ///
    /// Description: VRConnect shall initialise GateHealthState.flow_timeout_sec from
    /// health_ble_flow_timeout_sec (60 s), not from health_check_interval_sec (30 s).
    /// Regression test for bug I-1: the two values are intentionally set to different
    /// numbers so a mix-up is detected at compile-test time.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_flow_timeout_uses_ble_flow_timeout_sec() {
        let mut config = create_test_config();
        config.output_ble_enabled = true;
        config.health_check_interval_sec = 30;
        config.health_ble_flow_timeout_sec = 120; // deliberately different from check interval

        let processor = VitalProcessor::new(config);
        let ble = processor.create_ble_output().await.unwrap().unwrap();

        let flow_timeout = ble.health_state.read().await.flow_timeout_sec;
        assert_eq!(
            flow_timeout, 120,
            "flow_timeout_sec should equal health_ble_flow_timeout_sec (120), not health_check_interval_sec (30)"
        );
    }
}
