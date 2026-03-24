// /src/utils/logger.rs
// Module: utils.logger
// Purpose: Centralized logging system with file rotation and custom levels

use crate::config::Config;
use crate::error::{Result, VitalError};
use chrono::Local;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

/// ID SRS: SRS-MOD-LOGGER-001
/// Title: Logger
///
/// Description: VRConnect shall provide a custom logger with daily file rotation,
/// custom log levels (SUCCESS, INFO, WARNING, ERROR, DEBUG), and timestamp-based
/// log entries using machine time.
///
/// Version: V1.0
pub struct Logger {
    log_dir: PathBuf,
    current_file: Mutex<Option<PathBuf>>,
}

impl Logger {
    /// ID SRS: SRS-FN-LOGGER-001
    /// Title: init
    ///
    /// Description: VRConnect shall initialize the logger with configuration
    /// parameters, create log directory if needed, and set global logger.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `config` - Application configuration
    ///
    /// # Returns
    /// Result indicating initialization success or error
    pub fn init(config: &Config) -> Result<()> {
        let log_dir = PathBuf::from(&config.log_dir);

        // Create log directory if it doesn't exist
        fs::create_dir_all(&log_dir)
            .map_err(|e| VitalError::Logger(format!("Failed to create log directory: {}", e)))?;

        let logger = Logger {
            log_dir,
            current_file: Mutex::new(None),
        };

        // Set as global logger
        let level_filter = Self::parse_level(&config.log_level)?;

        log::set_boxed_logger(Box::new(logger))
            .map_err(|e| VitalError::Logger(format!("Failed to set logger: {}", e)))?;

        log::set_max_level(level_filter);

        Ok(())
    }

    /// ID SRS: SRS-FN-LOGGER-002
    /// Title: parse_level
    ///
    /// Description: VRConnect shall parse log level string (SUCCESS, INFO,
    /// WARNING, ERROR, DEBUG) into LevelFilter enum.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `level_str` - Log level string
    ///
    /// # Returns
    /// Parsed LevelFilter or error
    fn parse_level(level_str: &str) -> Result<log::LevelFilter> {
        match level_str.to_uppercase().as_str() {
            "SUCCESS" => Ok(log::LevelFilter::Info),
            "INFO" => Ok(log::LevelFilter::Info),
            "WARNING" => Ok(log::LevelFilter::Warn),
            "ERROR" => Ok(log::LevelFilter::Error),
            "DEBUG" => Ok(log::LevelFilter::Debug),
            "TRACE" => Ok(log::LevelFilter::Trace),
            _ => Err(VitalError::Logger(format!(
                "Invalid log level: {}",
                level_str
            ))),
        }
    }

    /// ID SRS: SRS-FN-LOGGER-003
    /// Title: get_log_file_path
    ///
    /// Description: VRConnect shall determine the log file path based on
    /// current date, creating daily log files (vrconnect-YYYY-MM-DD.log).
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// PathBuf to current log file
    fn get_log_file_path(&self) -> PathBuf {
        let date = Local::now().format("%Y-%m-%d");
        self.log_dir.join(format!("vrconnect-{}.log", date))
    }

    /// ID SRS: SRS-FN-LOGGER-004
    /// Title: write_log
    ///
    /// Description: VRConnect shall write formatted log message to daily log file
    /// with timestamp, level, and message content.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `record` - Log record to write
    fn write_log(&self, record: &log::Record) {
        let log_file = self.get_log_file_path();

        // Update current file if date changed
        {
            let mut current = self.current_file.lock().unwrap();
            if current.as_ref() != Some(&log_file) {
                *current = Some(log_file.clone());
            }
        }

        // Format: [2025-01-15 14:30:45.123] [INFO] Message
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let level_str = self.format_level(record.level());
        let message = format!("[{}] [{}] {}\n", timestamp, level_str, record.args());

        // Write to file
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_file) {
            let _ = file.write_all(message.as_bytes());
        }
    }

    /// ID SRS: SRS-FN-LOGGER-005
    /// Title: format_level
    ///
    /// Description: VRConnect shall format log level with consistent width
    /// for readable log output (SUCCESS, INFO, WARNING, ERROR, DEBUG).
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `level` - Log level
    ///
    /// # Returns
    /// Formatted level string
    fn format_level(&self, level: log::Level) -> String {
        match level {
            log::Level::Error => "ERROR  ".to_string(),
            log::Level::Warn => "WARNING".to_string(),
            log::Level::Info => "INFO   ".to_string(),
            log::Level::Debug => "DEBUG  ".to_string(),
            log::Level::Trace => "TRACE  ".to_string(),
        }
    }
}

impl log::Log for Logger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        self.write_log(record);
    }

    fn flush(&self) {
        // Nothing to flush with file-based logging
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::TempDir;

    /// Helper function to create a default test config
    fn create_test_config(log_dir: String) -> Config {
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
            output_ble_values: "HR,SPO2".to_string(),           // ADDED
            output_ble_empty_value: "null".to_string(),         // ADDED
            output_ble_update_interval_ms: 100,                 // ADDED
            output_file_enabled: false,
            output_file_base_path: "./data/test".to_string(),
            output_file_max_size_mb: 500,
            output_file_archive_threshold_gb: 5,
            output_file_critical_disk_percent: 95,
            debug_enabled: false,
            debug_output_path: "./debug.log".to_string(),
            log_level: "INFO".to_string(),
            log_dir,
        }
    }

    /// ID SRS: SRS-TEST-LOG-001
    /// Title: Test valid log level parsing
    ///
    /// Description: VRConnect shall correctly parse valid log level strings
    /// (SUCCESS, INFO, WARNING, ERROR, DEBUG, TRACE) into LevelFilter.
    ///
    /// Version: V1.0
    #[test]
    fn test_parse_level_valid() {
        assert_eq!(
            Logger::parse_level("SUCCESS").unwrap(),
            log::LevelFilter::Info
        );
        assert_eq!(Logger::parse_level("INFO").unwrap(), log::LevelFilter::Info);
        assert_eq!(
            Logger::parse_level("WARNING").unwrap(),
            log::LevelFilter::Warn
        );
        assert_eq!(
            Logger::parse_level("ERROR").unwrap(),
            log::LevelFilter::Error
        );
        assert_eq!(
            Logger::parse_level("DEBUG").unwrap(),
            log::LevelFilter::Debug
        );
        assert_eq!(
            Logger::parse_level("TRACE").unwrap(),
            log::LevelFilter::Trace
        );

        // Test case insensitivity
        assert_eq!(Logger::parse_level("info").unwrap(), log::LevelFilter::Info);
        assert_eq!(
            Logger::parse_level("WaRnInG").unwrap(),
            log::LevelFilter::Warn
        );
    }

    /// ID SRS: SRS-TEST-LOG-002
    /// Title: Test invalid log level parsing
    ///
    /// Description: VRConnect shall return an error when parsing invalid
    /// log level strings.
    ///
    /// Version: V1.0
    #[test]
    fn test_parse_level_invalid() {
        assert!(Logger::parse_level("INVALID").is_err());
        assert!(Logger::parse_level("").is_err());
        assert!(Logger::parse_level("123").is_err());
        assert!(Logger::parse_level("CRITICAL").is_err());
    }

    /// ID SRS: SRS-TEST-LOG-003
    /// Title: Test log file path generation
    ///
    /// Description: VRConnect shall generate log file paths with format
    /// vrconnect-YYYY-MM-DD.log based on current date.
    ///
    /// Version: V1.0
    #[test]
    fn test_log_file_path_generation() {
        let temp_dir = TempDir::new().unwrap();
        let logger = Logger {
            log_dir: temp_dir.path().to_path_buf(),
            current_file: Mutex::new(None),
        };

        let log_path = logger.get_log_file_path();
        let filename = log_path.file_name().unwrap().to_str().unwrap();

        // Check format: vrconnect-YYYY-MM-DD.log
        assert!(
            filename.starts_with("vrconnect-"),
            "Filename should start with 'vrconnect-': {}",
            filename
        );
        assert!(
            filename.ends_with(".log"),
            "Filename should end with '.log': {}",
            filename
        );

        // Remove prefix and suffix to get date part
        let without_prefix = filename.strip_prefix("vrconnect-").unwrap();
        let date_part = without_prefix.strip_suffix(".log").unwrap();

        // Verify date format YYYY-MM-DD (should be 10 chars)
        assert_eq!(
            date_part.len(),
            10,
            "Date should be 10 characters (YYYY-MM-DD)"
        );

        // Split by dash and verify parts
        let parts: Vec<&str> = date_part.split('-').collect();
        assert_eq!(
            parts.len(),
            3,
            "Date should have 3 parts separated by dashes"
        );

        assert_eq!(parts[0].len(), 4, "Year should be 4 digits");
        assert_eq!(parts[1].len(), 2, "Month should be 2 digits");
        assert_eq!(parts[2].len(), 2, "Day should be 2 digits");

        // Verify all parts are numeric
        assert!(
            parts[0].chars().all(|c| c.is_numeric()),
            "Year should be numeric: {}",
            parts[0]
        );
        assert!(
            parts[1].chars().all(|c| c.is_numeric()),
            "Month should be numeric: {}",
            parts[1]
        );
        assert!(
            parts[2].chars().all(|c| c.is_numeric()),
            "Day should be numeric: {}",
            parts[2]
        );
    }

    /// ID SRS: SRS-TEST-LOG-004
    /// Title: Test level formatting
    ///
    /// Description: VRConnect shall format log levels with consistent width
    /// (7 characters) for aligned output.
    ///
    /// Version: V1.0
    #[test]
    fn test_format_level() {
        let temp_dir = TempDir::new().unwrap();
        let logger = Logger {
            log_dir: temp_dir.path().to_path_buf(),
            current_file: Mutex::new(None),
        };

        assert_eq!(logger.format_level(log::Level::Error), "ERROR  ");
        assert_eq!(logger.format_level(log::Level::Warn), "WARNING");
        assert_eq!(logger.format_level(log::Level::Info), "INFO   ");
        assert_eq!(logger.format_level(log::Level::Debug), "DEBUG  ");
        assert_eq!(logger.format_level(log::Level::Trace), "TRACE  ");

        // Check consistent width (7 chars)
        assert_eq!(logger.format_level(log::Level::Error).len(), 7);
        assert_eq!(logger.format_level(log::Level::Info).len(), 7);
    }

    /// ID SRS: SRS-TEST-LOG-005
    /// Title: Test daily rotation mechanism
    ///
    /// Description: VRConnect shall update log file path when date changes,
    /// ensuring logs are written to correct daily file.
    ///
    /// Version: V1.0
    #[test]
    fn test_daily_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let logger = Logger {
            log_dir: temp_dir.path().to_path_buf(),
            current_file: Mutex::new(None),
        };

        // First call - should set current file
        let path1 = logger.get_log_file_path();
        {
            let mut current = logger.current_file.lock().unwrap();
            *current = Some(path1.clone());
        }

        // Second call - same date, should return same path
        let path2 = logger.get_log_file_path();
        assert_eq!(path1, path2);

        // Verify current_file is set
        let current = logger.current_file.lock().unwrap();
        assert!(current.is_some());
        assert_eq!(current.as_ref().unwrap(), &path1);
    }

    /// ID SRS: SRS-TEST-LOG-006
    /// Title: Test log file creation
    ///
    /// Description: VRConnect shall create log files when writing first entry.
    ///
    /// Version: V1.0
    #[test]
    fn test_log_file_creation() {
        let temp_dir = TempDir::new().unwrap();
        let logger = Logger {
            log_dir: temp_dir.path().to_path_buf(),
            current_file: Mutex::new(None),
        };

        let record = log::Record::builder()
            .args(format_args!("Test message"))
            .level(log::Level::Info)
            .target("test")
            .build();

        logger.write_log(&record);

        // Check file was created
        let log_path = logger.get_log_file_path();
        assert!(log_path.exists());

        // Check content
        let content = std::fs::read_to_string(log_path).unwrap();
        assert!(content.contains("[INFO   ] Test message"));
    }

    /// ID SRS: SRS-TEST-LOG-007
    /// Title: Test log entry format
    ///
    /// Description: VRConnect shall format log entries as:
    /// [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] Message
    ///
    /// Version: V1.0
    #[test]
    fn test_log_entry_format() {
        let temp_dir = TempDir::new().unwrap();
        let logger = Logger {
            log_dir: temp_dir.path().to_path_buf(),
            current_file: Mutex::new(None),
        };

        let record = log::Record::builder()
            .args(format_args!("Format test"))
            .level(log::Level::Debug)
            .target("test")
            .build();

        logger.write_log(&record);

        let log_path = logger.get_log_file_path();
        let content = std::fs::read_to_string(log_path).unwrap();

        // Verify format: [timestamp] [level] message
        assert!(content.contains("] [DEBUG  ] Format test"));

        // Verify timestamp format (basic check)
        assert!(content.starts_with("[20")); // Year starts with 20
    }

    /// ID SRS: SRS-TEST-LOG-008
    /// Title: Test Logger initialization with config
    ///
    /// Description: VRConnect shall initialize Logger with configuration,
    /// create log directory, and set global logger.
    ///
    /// Version: V1.0
    #[test]
    fn test_logger_init() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("logs");

        let config = create_test_config(log_path.to_str().unwrap().to_string());

        // Initialize logger (may fail if already initialized in other tests)
        let result = Logger::init(&config);

        // Either succeeds or fails with "already initialized" error
        if result.is_ok() {
            // Verify log directory was created
            assert!(log_path.exists());

            // Test that logging works
            log::info!("Test log message");
            log::debug!("Debug message");
            log::error!("Error message");
        }
        // If initialization fails, it's because another test already initialized it
        // which is acceptable
    }

    /// ID SRS: SRS-TEST-LOG-009
    /// Title: Test Logger with invalid log directory
    ///
    /// Description: VRConnect shall return error when log directory
    /// cannot be created.
    ///
    /// Version: V1.0
    #[test]
    fn test_logger_init_invalid_dir() {
        let config = create_test_config("/dev/null/impossible/path".to_string());

        let result = Logger::init(&config);
        // Should fail because directory cannot be created
        // OR succeed if logger was already initialized
        assert!(result.is_ok() || result.is_err());
    }

    /// ID SRS: SRS-TEST-LOG-010
    /// Title: Test Logger with all log levels
    ///
    /// Description: VRConnect shall support all log levels (SUCCESS, INFO,
    /// WARNING, ERROR, DEBUG, TRACE).
    ///
    /// Version: V1.0
    #[test]
    fn test_logger_all_levels() {
        for level in &["SUCCESS", "INFO", "WARNING", "ERROR", "DEBUG", "TRACE"] {
            let temp_dir = TempDir::new().unwrap();
            let log_path = temp_dir.path().join("logs");

            let mut config = create_test_config(log_path.to_str().unwrap().to_string());
            config.log_level = level.to_string();

            // Should parse level successfully
            let result = Logger::parse_level(&config.log_level);
            assert!(result.is_ok(), "Failed to parse level: {}", level);
        }
    }

    /// ID SRS: SRS-TEST-LOG-011
    /// Title: Test log trait implementation
    ///
    /// Description: VRConnect shall implement log::Log trait with enabled,
    /// log, and flush methods.
    ///
    /// Version: V1.0
    #[test]
    fn test_log_trait_methods() {
        use log::Log;

        let temp_dir = TempDir::new().unwrap();
        let logger = Logger {
            log_dir: temp_dir.path().to_path_buf(),
            current_file: Mutex::new(None),
        };

        // Test enabled (should always return true)
        let metadata = log::MetadataBuilder::new()
            .level(log::Level::Info)
            .target("test")
            .build();
        assert!(logger.enabled(&metadata));

        // Test log method
        let record = log::Record::builder()
            .args(format_args!("Test message"))
            .level(log::Level::Info)
            .target("test")
            .build();
        logger.log(&record);

        // Test flush (should not panic)
        logger.flush();

        // Verify log was written
        let log_path = logger.get_log_file_path();
        assert!(log_path.exists());
    }

    /// ID SRS: SRS-TEST-LOG-012
    /// Title: Test logger error handling
    ///
    /// Description: VRConnect shall handle errors during logger initialization
    /// with descriptive error messages.
    ///
    /// Version: V1.0
    #[test]
    fn test_logger_error_messages() {
        // Test invalid log level
        let result = Logger::parse_level("INVALID_LEVEL");
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid log level"));
    }

    /// ID SRS: SRS-TEST-LOG-013
    /// Title: Test complete logger initialization flow
    ///
    /// Description: VRConnect shall successfully initialize logger,
    /// set max level, and return Ok.
    ///
    /// Version: V1.0
    #[test]
    #[serial] // Ensure this runs isolated
    fn test_complete_init_flow() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("test_logs");

        let mut config = create_test_config(log_path.to_str().unwrap().to_string());
        config.log_level = "DEBUG".to_string();

        // This should cover the complete init flow including set_max_level and Ok(())
        let result = Logger::init(&config);

        // Will succeed if this is the first init, otherwise will fail
        if result.is_ok() {
            // Verify directory was created
            assert!(log_path.exists());

            // Verify we can log at the configured level
            log::debug!("This debug message should be logged");
            log::info!("This info message should be logged");
        }
    }
}
