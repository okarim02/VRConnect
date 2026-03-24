// /src/error/mod.rs
// Module: error
// Purpose: Centralized error handling for the application

use thiserror::Error;

/// ID SRS: SRS-MOD-ERROR-001
/// Title: VitalError
///
/// Description: VRConnect shall provide a unified error type covering all
/// application domains (IO, parsing, network, BLE) with clear error messages.
///
/// Version: V1.0
#[derive(Error, Debug)]
pub enum VitalError {
    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("JSON parsing error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Socket.IO error: {0}")]
    SocketIo(String),

    #[error("Bluetooth error: {0}")]
    Bluetooth(String),  // CHANGED: Remove #[from] bluer::Error

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Data processing error: {0}")]
    Processing(String),

    #[error("Regex error: {0}")]
    Regex(#[from] fancy_regex::Error),

    #[error("Logger error: {0}")]
    Logger(String),
}

/// ID SRS: SRS-UTIL-ERROR-001
/// Title: Result
///
/// Description: VRConnect shall define a standard Result type alias
/// using VitalError for consistent error handling across the application.
///
/// Version: V1.0
pub type Result<T> = std::result::Result<T, VitalError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_decompression() {
        let err = VitalError::Decompression("test error".to_string());
        assert_eq!(err.to_string(), "Decompression error: test error");
    }

    #[test]
    fn test_error_json_parse() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json");
        assert!(json_err.is_err());

        let vital_err: VitalError = json_err.unwrap_err().into();
        match vital_err {
            VitalError::JsonParse(_) => {}
            _ => panic!("Expected JsonParse variant"),
        }
    }

    #[test]
    fn test_error_io() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let vital_err: VitalError = io_err.into();

        match vital_err {
            VitalError::Io(_) => {}
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_error_socketio() {
        let err = VitalError::SocketIo("connection failed".to_string());
        assert!(err.to_string().contains("Socket.IO error"));
        assert!(err.to_string().contains("connection failed"));
    }

    #[test]
    fn test_error_config() {
        let err = VitalError::Config("invalid port".to_string());
        assert!(err.to_string().contains("Configuration error"));
    }

    #[test]
    fn test_error_processing() {
        let err = VitalError::Processing("invalid data format".to_string());
        assert!(err.to_string().contains("Data processing error"));
    }

    #[test]
    fn test_result_type() {
        fn test_fn() -> Result<i32> {
            Ok(42)
        }

        assert_eq!(test_fn().unwrap(), 42);
    }

    #[test]
    fn test_error_bluetooth() {
        let err = VitalError::Bluetooth("adapter not found".to_string());
        assert!(err.to_string().contains("Bluetooth error"));
        assert!(err.to_string().contains("adapter not found"));
    }
}