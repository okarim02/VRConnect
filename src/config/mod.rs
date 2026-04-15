// /src/config/mod.rs
// Module: config
// Purpose: Configuration management with CLI and file support

pub mod loader;

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// ID SRS: SRS-MOD-CONFIG-001
/// Title: Config
///
/// Description: VRConnect shall provide a configuration structure supporting
/// both CLI arguments and environment file loading for all application parameters.
///
/// Version: V1.0
#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(name = "VRConnect")]
#[command(version = "1.0.0")]
#[command(author = "UTBM Team")]
#[command(about = "Medical vital data middleware")]
pub struct Config {
    /// Path to configuration file (.env format)
    #[arg(long)]
    #[serde(skip)]
    pub config_file: Option<PathBuf>,

    // Socket.IO Configuration
    /// Socket.IO server host
    #[arg(long, default_value = "127.0.0.1")]
    pub socketio_host: String,

    /// Socket.IO server port
    #[arg(long, short = 'p', default_value = "3000")]
    pub socketio_port: u16,

    // Console Output Configuration
    /// Enable console output
    #[arg(long, default_value = "true")]
    pub output_console_enabled: bool,

    /// Enable verbose console output
    #[arg(long, short = 'v', default_value = "false")]
    pub output_console_verbose: bool,

    /// Enable colorized console output
    #[arg(long, default_value = "true")]
    pub output_console_colorized: bool,

    // BLE Output Configuration
    /// Enable BLE output
    #[arg(long, default_value = "false")]
    pub output_ble_enabled: bool,

    /// BLE device name
    #[arg(long, default_value = "VRConnect")]
    pub output_ble_device_name: String,

    /// BLE service UUID
    #[arg(long, default_value = "12345678-1234-5678-1234-567812345678")]
    pub output_ble_service_uuid: String,

    /// BLE track values to transmit (comma-separated, mandatory if BLE enabled)
    #[arg(long, default_value = "")]
    pub output_ble_values: String,

    /// BLE empty value placeholder (what to send when track is missing)
    #[arg(long, default_value = "null")]
    pub output_ble_empty_value: String,

    /// BLE update interval in milliseconds
    #[arg(long, default_value = "100")]
    pub output_ble_update_interval_ms: u64,

    // File Output Configuration
    /// Enable file output for complete data recording
    #[arg(long, default_value = "false")]
    pub output_file_enabled: bool,

    /// Base directory path for file output (data and archives subdirs will be created)
    #[arg(long, default_value = "./data/vrconnect/recording")]
    pub output_file_base_path: String,

    /// Maximum size per file in MB before rotation
    #[arg(long, default_value = "500")]
    pub output_file_max_size_mb: u64,

    /// Threshold in GB for daily folder before archiving old files
    #[arg(long, default_value = "5")]
    pub output_file_archive_threshold_gb: u64,

    /// Critical disk usage percentage that triggers shutdown
    #[arg(long, default_value = "95")]
    pub output_file_critical_disk_percent: u8,

    // Health Monitoring Configuration
    /// Health check interval in seconds (heartbeat period for BLE health notify)
    #[arg(long, default_value = "30")]
    pub health_check_interval_sec: u64,

    /// BLE flow timeout in seconds — flow=0 if no ProcessedData received within this window
    #[arg(long, default_value = "60")]
    pub health_ble_flow_timeout_sec: u64,

    /// Path to health.json written by HealthWriter.ps1
    #[arg(long, default_value = "logs/health.json")]
    pub health_file: String,

    // Debug Configuration
    /// Enable debug mode
    #[arg(long, default_value = "false")]
    pub debug_enabled: bool,

    /// Debug output file path
    #[arg(long, default_value = "./logs/debug.log")]
    pub debug_output_path: String,

    // Logging Configuration
    /// Log level (SUCCESS, INFO, WARNING, ERROR, DEBUG)
    #[arg(long, default_value = "INFO")]
    pub log_level: String,

    /// Log directory
    #[arg(long, default_value = "./logs")]
    pub log_dir: String,
}

impl Config {
    /// ID SRS: SRS-FN-CONFIG-001
    /// Title: parse
    ///
    /// Description: VRConnect shall parse the configuration from CLI arguments
    /// and optionally merge with environment file, returning a validated Config instance.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Parsed and validated configuration
    pub fn parse() -> Self {
        // First pass: check if --config-file is specified
        let args: Vec<String> = std::env::args().collect();
        let mut config_file_path: Option<PathBuf> = None;

        for i in 0..args.len() {
            if args[i] == "--config-file" && i + 1 < args.len() {
                config_file_path = Some(PathBuf::from(&args[i + 1]));
                break;
            }
        }

        // If config file specified, load it to set environment variables
        if let Some(ref path) = config_file_path {
            if let Err(e) = dotenvy::from_path(path) {
                eprintln!("Warning: Failed to load config file {}: {}", path.display(), e);
            }
        }

        // Parse CLI arguments
        let mut config = <Config as Parser>::parse();

        // Store the config file path
        config.config_file = config_file_path.clone();

        // If config file was loaded, override with env vars where CLI didn't specify
        if config_file_path.is_some() {
            config = Self::apply_env_overrides(config, &args);
        }

        // Validate
        config.validate().expect("Invalid configuration");

        config
    }

    /// ID SRS: SRS-FN-CONFIG-001-B
    /// Title: apply_env_overrides
    ///
    /// Description: VRConnect shall apply environment variable overrides to configuration
    /// for values that were not explicitly set via CLI arguments.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `config` - Parsed configuration
    /// * `args` - Command line arguments
    ///
    /// # Returns
    /// Config with environment overrides applied
    fn apply_env_overrides(mut config: Config, args: &[String]) -> Self {
        // Helper to check if an argument was explicitly provided
        let has_arg = |flag: &str| args.iter().any(|arg| arg == flag);
        let has_arg_short = |short: char| args.iter().any(|arg| arg == &format!("-{}", short));

        // Socket.IO
        if !has_arg("--socketio-host") {
            if let Ok(val) = std::env::var("SOCKETIO_HOST") {
                config.socketio_host = val;
            }
        }
        if !has_arg("--socketio-port") && !has_arg_short('p') {
            if let Ok(val) = std::env::var("SOCKETIO_PORT") {
                if let Ok(port) = val.parse() {
                    config.socketio_port = port;
                }
            }
        }

        // Console Output
        if !has_arg("--output-console-enabled") {
            if let Ok(val) = std::env::var("OUTPUT_CONSOLE_ENABLED") {
                if let Ok(enabled) = val.parse() {
                    config.output_console_enabled = enabled;
                }
            }
        }
        if !has_arg("--output-console-verbose") && !has_arg_short('v') {
            if let Ok(val) = std::env::var("OUTPUT_CONSOLE_VERBOSE") {
                if let Ok(verbose) = val.parse() {
                    config.output_console_verbose = verbose;
                }
            }
        }
        if !has_arg("--output-console-colorized") {
            if let Ok(val) = std::env::var("OUTPUT_CONSOLE_COLORIZED") {
                if let Ok(colorized) = val.parse() {
                    config.output_console_colorized = colorized;
                }
            }
        }

        // BLE Output
        if !has_arg("--output-ble-enabled") {
            if let Ok(val) = std::env::var("OUTPUT_BLE_ENABLED") {
                if let Ok(enabled) = val.parse() {
                    config.output_ble_enabled = enabled;
                }
            }
        }
        if !has_arg("--output-ble-device-name") {
            if let Ok(val) = std::env::var("OUTPUT_BLE_DEVICE_NAME") {
                config.output_ble_device_name = val;
            }
        }
        if !has_arg("--output-ble-service-uuid") {
            if let Ok(val) = std::env::var("OUTPUT_BLE_SERVICE_UUID") {
                config.output_ble_service_uuid = val;
            }
        }
        if !has_arg("--output-ble-values") {
            if let Ok(val) = std::env::var("BLE_VALUES") {
                config.output_ble_values = val;
            }
        }
        if !has_arg("--output-ble-empty-value") {
            if let Ok(val) = std::env::var("BLE_EMPTY_VALUE") {
                config.output_ble_empty_value = val;
            }
        }
        if !has_arg("--output-ble-update-interval-ms") {
            if let Ok(val) = std::env::var("BLE_UPDATE_INTERVAL_MS") {
                if let Ok(interval) = val.parse() {
                    config.output_ble_update_interval_ms = interval;
                }
            }
        }

        // File Output
        if !has_arg("--output-file-enabled") {
            if let Ok(val) = std::env::var("OUTPUT_FILE_ENABLED") {
                if let Ok(enabled) = val.parse() {
                    config.output_file_enabled = enabled;
                }
            }
        }
        if !has_arg("--output-file-base-path") {
            if let Ok(val) = std::env::var("OUTPUT_FILE_BASE_PATH") {
                config.output_file_base_path = val;
            }
        }
        if !has_arg("--output-file-max-size-mb") {
            if let Ok(val) = std::env::var("OUTPUT_FILE_MAX_SIZE_MB") {
                if let Ok(size) = val.parse() {
                    config.output_file_max_size_mb = size;
                }
            }
        }
        if !has_arg("--output-file-archive-threshold-gb") {
            if let Ok(val) = std::env::var("OUTPUT_FILE_ARCHIVE_THRESHOLD_GB") {
                if let Ok(threshold) = val.parse() {
                    config.output_file_archive_threshold_gb = threshold;
                }
            }
        }
        if !has_arg("--output-file-critical-disk-percent") {
            if let Ok(val) = std::env::var("OUTPUT_FILE_CRITICAL_DISK_PERCENT") {
                if let Ok(percent) = val.parse() {
                    config.output_file_critical_disk_percent = percent;
                }
            }
        }

        // Health
        if !has_arg("--health-check-interval-sec") {
            if let Ok(val) = std::env::var("HEALTH_CHECK_INTERVAL_SEC") {
                if let Ok(interval) = val.parse() {
                    config.health_check_interval_sec = interval;
                }
            }
        }
        if !has_arg("--health-ble-flow-timeout-sec") {
            if let Ok(val) = std::env::var("HEALTH_BLE_FLOW_TIMEOUT_SEC") {
                if let Ok(timeout) = val.parse() {
                    config.health_ble_flow_timeout_sec = timeout;
                }
            }
        }
        if !has_arg("--health-file") {
            if let Ok(val) = std::env::var("HEALTH_FILE") {
                config.health_file = val;
            }
        }

        // Debug
        if !has_arg("--debug-enabled") {
            if let Ok(val) = std::env::var("DEBUG_ENABLED") {
                if let Ok(enabled) = val.parse() {
                    config.debug_enabled = enabled;
                }
            }
        }
        if !has_arg("--debug-output-path") {
            if let Ok(val) = std::env::var("DEBUG_OUTPUT_PATH") {
                config.debug_output_path = val;
            }
        }

        // Logging
        if !has_arg("--log-level") {
            if let Ok(val) = std::env::var("LOG_LEVEL") {
                config.log_level = val;
            }
        }
        if !has_arg("--log-dir") {
            if let Ok(val) = std::env::var("LOG_DIR") {
                config.log_dir = val;
            }
        }

        config
    }

    /// ID SRS: SRS-FN-CONFIG-002
    /// Title: merge_with
    ///
    /// Description: VRConnect shall merge the current configuration with values
    /// from a file-loaded configuration, with CLI arguments taking precedence.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `file_config` - Configuration loaded from file
    ///
    /// # Returns
    /// Merged configuration
    fn merge_with(self, _file_config: Config) -> Self {
        // CLI arguments already parsed, just return self
        // File config is loaded via dotenvy before CLI parsing
        self
    }

    /// ID SRS: SRS-FN-CONFIG-003
    /// Title: validate
    ///
    /// Description: VRConnect shall validate the configuration parameters,
    /// returning an error if any value is invalid or inconsistent.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Result indicating validation success or error
    pub fn validate(&self) -> Result<(), String> {
        // Validate port range
        if self.socketio_port == 0 {
            return Err("Socket.IO port cannot be 0".to_string());
        }

        // Validate UUID format if BLE enabled
        if self.output_ble_enabled {
            if Uuid::parse_str(&self.output_ble_service_uuid).is_err() {
                return Err(format!(
                    "Invalid BLE service UUID: {}",
                    self.output_ble_service_uuid
                ));
            }

            // BLE_VALUES is only used by legacy state-sync (ble.rs).
            // The reliable protocol uses the hardcoded signal catalog instead.

            // Validate update interval
            if self.output_ble_update_interval_ms == 0 {
                return Err("BLE update interval must be greater than 0".to_string());
            }
        }

        // Validate file output configuration
        if self.output_file_enabled {
            if self.output_file_max_size_mb == 0 {
                return Err("File output max size must be greater than 0".to_string());
            }

            if self.output_file_archive_threshold_gb == 0 {
                return Err("File output archive threshold must be greater than 0".to_string());
            }

            if self.output_file_critical_disk_percent == 0
                || self.output_file_critical_disk_percent > 100
            {
                return Err(
                    "File output critical disk percent must be between 1 and 100".to_string(),
                );
            }

            if self.output_file_base_path.trim().is_empty() {
                return Err("File output base path cannot be empty".to_string());
            }
        }

        // Validate health config
        if self.health_check_interval_sec == 0 {
            return Err("health_check_interval_sec must be greater than 0".to_string());
        }
        if self.health_ble_flow_timeout_sec == 0 {
            return Err("health_ble_flow_timeout_sec must be greater than 0".to_string());
        }

        // Validate log level
        let valid_levels = ["SUCCESS", "INFO", "WARNING", "ERROR", "DEBUG"];
        if !valid_levels.contains(&self.log_level.to_uppercase().as_str()) {
            return Err(format!("Invalid log level: {}", self.log_level));
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-CONFIG-004
    /// Title: socket_url
    ///
    /// Description: VRConnect shall construct the complete Socket.IO URL
    /// from host and port configuration parameters.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Complete Socket.IO URL string
    #[allow(dead_code)]
    pub fn socket_url(&self) -> String {
        format!("http://{}:{}", self.socketio_host, self.socketio_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

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
            output_file_base_path: "./data/vrconnect/recording".to_string(),
            output_file_max_size_mb: 500,
            output_file_archive_threshold_gb: 5,
            output_file_critical_disk_percent: 95,
            health_check_interval_sec: 30,
            health_ble_flow_timeout_sec: 60,
            health_file: "logs/health.json".to_string(),
            debug_enabled: false,
            debug_output_path: "./debug.log".to_string(),
            log_level: "INFO".to_string(),
            log_dir: "./logs".to_string(),
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = Config::parse_from(vec!["vrconnect"]);

        assert_eq!(config.socketio_host, "127.0.0.1");
        assert_eq!(config.socketio_port, 3000);
        assert!(config.output_console_enabled);
        assert!(!config.output_console_verbose);
        assert!(config.output_console_colorized);
        assert!(!config.output_ble_enabled);
        assert_eq!(config.output_ble_device_name, "VRConnect");
        assert_eq!(config.output_ble_values, "");
        assert_eq!(config.output_ble_empty_value, "null");
        assert_eq!(config.output_ble_update_interval_ms, 100);
        assert!(!config.output_file_enabled);
        assert_eq!(
            config.output_file_base_path,
            "./data/vrconnect/recording"
        );
        assert_eq!(config.output_file_max_size_mb, 500);
        assert_eq!(config.output_file_archive_threshold_gb, 5);
        assert_eq!(config.output_file_critical_disk_percent, 95);
        assert!(!config.debug_enabled);
        assert_eq!(config.log_level, "INFO");
        assert_eq!(config.log_dir, "./logs");
    }

    #[test]
    fn test_config_parse_port() {
        let config = Config::parse_from(vec!["vrconnect", "--socketio-port", "5000"]);
        assert_eq!(config.socketio_port, 5000);
    }

    #[test]
    fn test_config_parse_host() {
        let config = Config::parse_from(vec!["vrconnect", "--socketio-host", "0.0.0.0"]);
        assert_eq!(config.socketio_host, "0.0.0.0");
    }

    #[test]
    fn test_config_parse_verbose() {
        let config = Config::parse_from(vec!["vrconnect", "--output-console-verbose"]);
        assert!(config.output_console_verbose);
    }

    #[test]
    fn test_config_parse_ble_name() {
        let config = Config::parse_from(vec!["vrconnect", "--output-ble-device-name", "MyDevice"]);
        assert_eq!(config.output_ble_device_name, "MyDevice");
    }

    #[test]
    fn test_config_ble_disabled_default() {
        let config = Config::parse_from(vec!["vrconnect"]);
        assert!(!config.output_ble_enabled);
    }

    #[test]
    fn test_config_parse_log_level() {
        let config = Config::parse_from(vec!["vrconnect", "--log-level", "debug"]);
        assert_eq!(config.log_level, "debug");
    }

    #[test]
    fn test_config_parse_log_dir() {
        let config = Config::parse_from(vec!["vrconnect", "--log-dir", "/var/log/vrconnect"]);
        assert_eq!(config.log_dir, "/var/log/vrconnect");
    }

    #[test]
    fn test_config_parse_debug_mode() {
        let config = Config::parse_from(vec!["vrconnect", "--debug-enabled"]);
        assert!(config.debug_enabled);
    }

    #[test]
    fn test_config_parse_multiple_args() {
        let config = Config::parse_from(vec![
            "vrconnect",
            "--socketio-port",
            "5000",
            "--socketio-host",
            "0.0.0.0",
            "--output-console-verbose",
            "--output-ble-device-name",
            "TestDevice",
            "--log-level",
            "debug",
        ]);

        assert_eq!(config.socketio_port, 5000);
        assert_eq!(config.socketio_host, "0.0.0.0");
        assert!(config.output_console_verbose);
        assert_eq!(config.output_ble_device_name, "TestDevice");
        assert_eq!(config.log_level, "debug");
    }

    #[test]
    fn test_config_validate_success() {
        let config = Config::parse_from(vec!["vrconnect"]);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_invalid_port() {
        let config = Config::parse_from(vec!["vrconnect", "--socketio-port", "0"]);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("port"));
    }

    #[test]
    fn test_config_debug_display() {
        let config = Config::parse_from(vec!["vrconnect"]);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("Config"));
    }

    #[test]
    #[serial]
    fn test_config_with_file_loading() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        std::env::remove_var("SOCKETIO_PORT");
        std::env::remove_var("LOG_LEVEL");

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "SOCKETIO_PORT=7777").unwrap();
        writeln!(temp_file, "LOG_LEVEL=DEBUG").unwrap();
        temp_file.flush().unwrap();

        dotenvy::from_path(temp_file.path()).unwrap();

        let args = vec![
            "vrconnect".to_string(),
            "--config-file".to_string(),
            temp_file.path().to_str().unwrap().to_string(),
        ];

        let base_config = Config::parse_from(&args);
        let config = Config::apply_env_overrides(base_config, &args);

        assert_eq!(config.socketio_port, 7777);
        assert_eq!(config.log_level, "DEBUG");

        std::env::remove_var("SOCKETIO_PORT");
        std::env::remove_var("LOG_LEVEL");
    }

    #[test]
    fn test_config_with_missing_file() {
        let config = Config::parse_from(&[
            "vrconnect",
            "--config-file",
            "/non/existent/path.env",
            "--socketio-port",
            "5555",
        ]);

        assert_eq!(config.socketio_port, 5555);
    }

    #[test]
    fn test_validate_invalid_ble_uuid() {
        let mut config = create_test_config();
        config.output_ble_enabled = true;
        config.output_ble_service_uuid = "INVALID-UUID".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid BLE service UUID"));
    }

    #[test]
    fn test_validate_invalid_log_level() {
        let mut config = create_test_config();
        config.log_level = "INVALID_LEVEL".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid log level"));
    }

    #[test]
    fn test_socket_url() {
        let mut config = create_test_config();
        config.socketio_host = "192.168.1.100".to_string();
        config.socketio_port = 8080;

        assert_eq!(config.socket_url(), "http://192.168.1.100:8080");
    }

    #[test]
    fn test_merge_with() {
        let cli_config = create_test_config();
        let file_config = create_test_config();

        let merged = cli_config.merge_with(file_config);

        assert_eq!(merged.socketio_host, "127.0.0.1");
        assert_eq!(merged.socketio_port, 3000);
    }

    #[test]
    fn test_parse_with_valid_config() {
        let config = Config::parse_from(&[
            "vrconnect",
            "--socketio-port",
            "5000",
            "--socketio-host",
            "0.0.0.0",
        ]);

        assert_eq!(config.socketio_port, 5000);
        assert_eq!(config.socketio_host, "0.0.0.0");
    }

    #[test]
    #[serial]
    fn test_config_parse_default() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "SOCKETIO_PORT=6000").unwrap();
        writeln!(temp_file, "LOG_LEVEL=INFO").unwrap();
        temp_file.flush().unwrap();

        let config = Config::parse_from(&[
            "vrconnect",
            "--config-file",
            temp_file.path().to_str().unwrap(),
            "--socketio-port",
            "4000",
        ]);

        assert_eq!(config.socketio_port, 4000);
        assert!(config.config_file.is_some());
    }

    #[test]
    #[serial]
    fn test_config_file_merge_path() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "SOCKETIO_HOST=192.168.1.1").unwrap();
        writeln!(temp_file, "SOCKETIO_PORT=7000").unwrap();
        writeln!(temp_file, "LOG_LEVEL=TRACE").unwrap();
        temp_file.flush().unwrap();

        std::env::set_var("TEST_CONFIG_FILE", temp_file.path().to_str().unwrap());

        let config = Config::parse_from(&[
            "vrconnect",
            "--config-file",
            temp_file.path().to_str().unwrap(),
        ]);

        assert!(config.config_file.is_some());

        let validation_result = config.validate();
        assert!(validation_result.is_ok());

        std::env::remove_var("TEST_CONFIG_FILE");
    }

    #[test]
    fn test_file_output_validation() {
        let mut config = create_test_config();
        config.output_file_enabled = true;

        config.output_file_max_size_mb = 0;
        assert!(config.validate().is_err());

        config.output_file_max_size_mb = 500;
        config.output_file_archive_threshold_gb = 0;
        assert!(config.validate().is_err());

        config.output_file_archive_threshold_gb = 5;
        config.output_file_critical_disk_percent = 0;
        assert!(config.validate().is_err());

        config.output_file_critical_disk_percent = 101;
        assert!(config.validate().is_err());

        config.output_file_critical_disk_percent = 95;
        config.output_file_base_path = "".to_string();
        assert!(config.validate().is_err());

        config.output_file_base_path = "./data/test".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_parse_file_output() {
        let config = Config::parse_from(vec![
            "vrconnect",
            "--output-file-enabled",
            "--output-file-base-path",
            "/data/recordings",
            "--output-file-max-size-mb",
            "1000",
            "--output-file-archive-threshold-gb",
            "10",
            "--output-file-critical-disk-percent",
            "90",
        ]);

        assert!(config.output_file_enabled);
        assert_eq!(config.output_file_base_path, "/data/recordings");
        assert_eq!(config.output_file_max_size_mb, 1000);
        assert_eq!(config.output_file_archive_threshold_gb, 10);
        assert_eq!(config.output_file_critical_disk_percent, 90);
    }

    #[test]
    #[serial]
    fn test_env_var_loading() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        std::env::remove_var("SOCKETIO_PORT");
        std::env::remove_var("OUTPUT_FILE_ENABLED");

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "SOCKETIO_PORT=9999").unwrap();
        writeln!(temp_file, "OUTPUT_FILE_ENABLED=true").unwrap();
        temp_file.flush().unwrap();

        dotenvy::from_path(temp_file.path()).unwrap();

        let args = vec![
            "vrconnect".to_string(),
            "--config-file".to_string(),
            temp_file.path().to_str().unwrap().to_string(),
        ];
        let base_config = Config::parse_from(&args);
        let config = Config::apply_env_overrides(base_config, &args);

        assert_eq!(config.socketio_port, 9999);
        assert!(config.output_file_enabled);

        std::env::remove_var("SOCKETIO_PORT");
        std::env::remove_var("OUTPUT_FILE_ENABLED");
    }

    #[test]
    fn test_validate_ble_empty_values_allowed() {
        // BLE_VALUES is only used by legacy state-sync; reliable protocol uses catalog
        let mut config = create_test_config();
        config.output_ble_enabled = true;
        config.output_ble_values = "".to_string();

        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_ble_update_interval() {
        let mut config = create_test_config();
        config.output_ble_enabled = true;
        config.output_ble_values = "HR,SPO2".to_string();
        config.output_ble_update_interval_ms = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("BLE update interval"));
    }

    #[test]
    fn test_config_parse_ble_values() {
        let config = Config::parse_from(vec![
            "vrconnect",
            "--output-ble-values",
            "HR,SPO2,NIBP_SYS",
        ]);

        assert_eq!(config.output_ble_values, "HR,SPO2,NIBP_SYS");
    }

    #[test]
    fn test_config_parse_ble_empty_value() {
        let config = Config::parse_from(vec![
            "vrconnect",
            "--output-ble-empty-value",
            "",
        ]);

        assert_eq!(config.output_ble_empty_value, "");
    }

    #[test]
    fn test_config_parse_ble_update_interval() {
        let config = Config::parse_from(vec![
            "vrconnect",
            "--output-ble-update-interval-ms",
            "200",
        ]);

        assert_eq!(config.output_ble_update_interval_ms, 200);
    }

    // --- Health config tests ---

    #[test]
    fn test_health_defaults() {
        let config = Config::parse_from(vec!["vrconnect"]);
        assert_eq!(config.health_check_interval_sec, 30);
        assert_eq!(config.health_ble_flow_timeout_sec, 60);
        assert_eq!(config.health_file, "logs/health.json");
    }

    #[test]
    fn test_health_parse_args() {
        let config = Config::parse_from(vec![
            "vrconnect",
            "--health-check-interval-sec", "10",
            "--health-ble-flow-timeout-sec", "120",
            "--health-file", "custom/health.json",
        ]);
        assert_eq!(config.health_check_interval_sec, 10);
        assert_eq!(config.health_ble_flow_timeout_sec, 120);
        assert_eq!(config.health_file, "custom/health.json");
    }

    #[test]
    fn test_health_validate_interval_zero() {
        let mut config = create_test_config();
        config.health_check_interval_sec = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("health_check_interval_sec"));
    }

    #[test]
    fn test_health_validate_flow_timeout_zero() {
        let mut config = create_test_config();
        config.health_ble_flow_timeout_sec = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("health_ble_flow_timeout_sec"));
    }

    #[test]
    #[serial]
    fn test_health_env_override() {
        std::env::remove_var("HEALTH_CHECK_INTERVAL_SEC");
        std::env::remove_var("HEALTH_BLE_FLOW_TIMEOUT_SEC");
        std::env::remove_var("HEALTH_FILE");

        std::env::set_var("HEALTH_CHECK_INTERVAL_SEC", "45");
        std::env::set_var("HEALTH_BLE_FLOW_TIMEOUT_SEC", "90");
        std::env::set_var("HEALTH_FILE", "logs/custom_health.json");

        let args = vec!["vrconnect".to_string()];
        let base = Config::parse_from(&args);
        let config = Config::apply_env_overrides(base, &args);

        assert_eq!(config.health_check_interval_sec, 45);
        assert_eq!(config.health_ble_flow_timeout_sec, 90);
        assert_eq!(config.health_file, "logs/custom_health.json");

        std::env::remove_var("HEALTH_CHECK_INTERVAL_SEC");
        std::env::remove_var("HEALTH_BLE_FLOW_TIMEOUT_SEC");
        std::env::remove_var("HEALTH_FILE");
    }
}