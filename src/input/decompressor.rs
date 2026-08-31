// /src/input/decompressor.rs
// Module: input.decompressor
// Purpose: Automatic zlib decompression for Socket.IO binary data

use crate::error::{Result, VitalError};
use flate2::read::ZlibDecoder;
use std::io::Read;

/// ID SRS: SRS-MOD-DECOMPRESSOR-001
/// Title: VitalDataDecompressor
///
/// Description: VRConnect shall detect and decompress zlib-compressed data
/// automatically, supporting Socket.IO v4 binary indicators.
///
/// Version: V1.0
#[derive(Clone)]
pub struct VitalDataDecompressor;

impl VitalDataDecompressor {
    /// ID SRS: SRS-FN-DECOMPRESSOR-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a new VitalDataDecompressor instance.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// New decompressor instance
    pub fn new() -> Self {
        Self
    }

    /// ID SRS: SRS-FN-DECOMPRESSOR-002
    /// Title: decompress
    ///
    /// Description: VRConnect shall detect compression format (Socket.IO v4 binary
    /// or direct zlib) and decompress data, returning original data if uncompressed.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `data` - Raw data bytes
    ///
    /// # Returns
    /// Decompressed data or original if not compressed
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(data.to_vec());
        }

        // Check for Socket.IO v4 binary indicator (0x04) followed by zlib header (0x78)
        if data.len() >= 2 && data[0] == 0x04 && data[1] == 0x78 {
            log::debug!("Detected Socket.IO v4 binary frame with zlib compression");
            return self.decompress_zlib(&data[1..]);
        }

        // Check for direct zlib header (0x78 0x9C or 0x78 0xDA or 0x78 0x01)
        if data[0] == 0x78 && data.len() >= 2 {
            let second_byte = data[1];
            if second_byte == 0x9C || second_byte == 0xDA || second_byte == 0x01 {
                log::debug!("Detected zlib compression");
                return self.decompress_zlib(data);
            }
        }

        // No compression detected
        log::debug!("No compression detected, returning original data");
        Ok(data.to_vec())
    }

    /// ID SRS: SRS-FN-DECOMPRESSOR-003
    /// Title: decompress_zlib
    ///
    /// Description: VRConnect shall decompress zlib-compressed data using
    /// flate2 decoder, returning decompressed bytes.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `data` - Zlib-compressed data
    ///
    /// # Returns
    /// Decompressed data or error
    fn decompress_zlib(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = ZlibDecoder::new(data);
        let mut decompressed = Vec::new();

        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| VitalError::Decompression(format!("Zlib decompression failed: {}", e)))?;

        log::debug!(
            "Decompressed {} bytes to {} bytes",
            data.len(),
            decompressed.len()
        );
        Ok(decompressed)
    }
}

impl Default for VitalDataDecompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ID SRS: SRS-TEST-DECOMP-001
    /// Title: Test decompressor creation
    ///
    /// Description: VRConnect shall create a VitalDataDecompressor instance.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompressor_new() {
        // Just verify it can be created without panicking
        let _decompressor = VitalDataDecompressor::new();
    }

    /// ID SRS: SRS-TEST-DECOMP-002
    /// Title: Test uncompressed data passthrough
    ///
    /// Description: VRConnect shall return uncompressed data unchanged
    /// when no compression is detected.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_uncompressed_data() {
        let decompressor = VitalDataDecompressor::new();
        let data = b"Hello, World!";

        let result = decompressor.decompress(data).unwrap();
        assert_eq!(result, data);
    }

    /// ID SRS: SRS-TEST-DECOMP-003
    /// Title: Test empty data handling
    ///
    /// Description: VRConnect shall handle empty data without errors.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_empty_data() {
        let decompressor = VitalDataDecompressor::new();
        let data = b"";

        let result = decompressor.decompress(data).unwrap();
        assert_eq!(result.len(), 0);
    }

    /// ID SRS: SRS-TEST-DECOMP-004
    /// Title: Test zlib compression detection
    ///
    /// Description: VRConnect shall detect and decompress zlib-compressed data
    /// with header 0x78 0x9C.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_zlib_data() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let decompressor = VitalDataDecompressor::new();
        // Use longer text for better compression ratio
        let original = b"This is a test string for compression. \
		                 Repeated text helps compression work better. \
		                 This is a test string for compression. \
		                 Repeated text helps compression work better.";

        // Compress data
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Verify zlib header
        assert_eq!(compressed[0], 0x78); // Zlib header

        // Decompress
        let result = decompressor.decompress(&compressed).unwrap();
        assert_eq!(result, original);
    }

    /// ID SRS: SRS-TEST-DECOMP-005
    /// Title: Test Socket.IO v4 binary frame with zlib
    ///
    /// Description: VRConnect shall detect and decompress Socket.IO v4
    /// binary frames (0x04) followed by zlib compression (0x78).
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_socketio_binary_frame() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let decompressor = VitalDataDecompressor::new();
        let original = b"Socket.IO test data";

        // Compress data
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Prepend Socket.IO v4 binary indicator
        let mut socketio_data = vec![0x04];
        socketio_data.extend_from_slice(&compressed);

        // Decompress
        let result = decompressor.decompress(&socketio_data).unwrap();
        assert_eq!(result, original);
    }

    /// ID SRS: SRS-TEST-DECOMP-006
    /// Title: Test zlib with different compression levels
    ///
    /// Description: VRConnect shall decompress zlib data compressed
    /// with different compression levels (0x78 0x01, 0x78 0x9C, 0x78 0xDA).
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_different_zlib_levels() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let decompressor = VitalDataDecompressor::new();
        // Use longer text with repetitions for better compression
        let original = b"Test data for different compression levels. \
		                 This text repeats to ensure compression works. \
		                 Test data for different compression levels. \
		                 This text repeats to ensure compression works.";

        // Test with fast compression
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(original).unwrap();
        let compressed_fast = encoder.finish().unwrap();
        let result = decompressor.decompress(&compressed_fast).unwrap();
        assert_eq!(result, original);

        // Test with default compression
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed_default = encoder.finish().unwrap();
        let result = decompressor.decompress(&compressed_default).unwrap();
        assert_eq!(result, original);

        // Test with best compression
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(original).unwrap();
        let compressed_best = encoder.finish().unwrap();
        let result = decompressor.decompress(&compressed_best).unwrap();
        assert_eq!(result, original);
    }

    /// ID SRS: SRS-TEST-DECOMP-007
    /// Title: Test invalid zlib data handling
    ///
    /// Description: VRConnect shall return an error when attempting to
    /// decompress invalid zlib data.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_invalid_zlib() {
        let decompressor = VitalDataDecompressor::new();

        // Create data that looks like zlib but isn't valid
        let invalid_data = vec![0x78, 0x9C, 0xFF, 0xFF, 0xFF, 0xFF];

        let result = decompressor.decompress(&invalid_data);
        assert!(result.is_err());
    }

    /// ID SRS: SRS-TEST-DECOMP-008
    /// Title: Test JSON data decompression
    ///
    /// Description: VRConnect shall successfully decompress JSON data
    /// that was compressed with zlib.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_json_data() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let decompressor = VitalDataDecompressor::new();
        let json_data = br#"{"vrcode":"VR-TEST","rooms":[{"roomname":"BED_01","trks":[]}]}"#;

        // Compress
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json_data).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress
        let result = decompressor.decompress(&compressed).unwrap();
        assert_eq!(result, json_data);

        // Verify it's valid JSON
        let json_str = String::from_utf8(result).unwrap();
        assert!(serde_json::from_str::<serde_json::Value>(&json_str).is_ok());
    }

    /// ID SRS: SRS-TEST-DECOMP-009
    /// Title: Test decompressor clone
    ///
    /// Description: VRConnect shall support cloning VitalDataDecompressor.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompressor_clone() {
        let decompressor1 = VitalDataDecompressor::new();
        let decompressor2 = decompressor1.clone();

        let data = b"Test data";
        let result1 = decompressor1.decompress(data).unwrap();
        let result2 = decompressor2.decompress(data).unwrap();

        assert_eq!(result1, result2);
    }

    /// ID SRS: SRS-TEST-DECOMP-010
    /// Title: Test large data decompression
    ///
    /// Description: VRConnect shall handle decompression of large data sets.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompress_large_data() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let decompressor = VitalDataDecompressor::new();

        // Create large data (10KB)
        let original: Vec<u8> = (0..10240).map(|i| (i % 256) as u8).collect();

        // Compress
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress
        let result = decompressor.decompress(&compressed).unwrap();
        assert_eq!(result, original);
        assert_eq!(result.len(), 10240);
    }
    /// ID SRS: SRS-TEST-DECOMP-011
    /// Title: Test VitalDataDecompressor default creation
    ///
    /// Description: VRConnect shall create VitalDataDecompressor via Default trait.
    ///
    /// Version: V1.0
    #[test]
    fn test_decompressor_default() {
        let decompressor = VitalDataDecompressor;

        // Verify it works the same as new()
        let data = b"Hello, World!";
        let result = decompressor.decompress(data).unwrap();
        assert_eq!(result, data);
    }
}
