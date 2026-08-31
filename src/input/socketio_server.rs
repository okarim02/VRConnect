// /src/input/socketio_server.rs
// Module: input.socketio_server
// Purpose: Socket.IO v4 WebSocket server for vital data reception

use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use crate::input::decompressor::VitalDataDecompressor;
use crate::output::health::GateHealthState;
use crate::processor::{VitalDataCleaner, VitalDataTransformer};
use futures_util::{SinkExt, StreamExt};
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify, RwLock};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

/// ID SRS: SRS-MOD-SOCKETIO-001
/// Title: SocketIOServer
///
/// Description: VRConnect shall implement a Socket.IO v4 compatible WebSocket
/// server receiving vital data, with automatic decompression and processing.
///
/// Version: V1.0
pub struct SocketIOServer {
    host: String,
    port: u16,
    debug_enabled: bool,
    debug_file: Arc<RwLock<Option<File>>>,
    decompressor: VitalDataDecompressor,
    cleaner: VitalDataCleaner,
    transformer: VitalDataTransformer,
    /// Optional health hooks — wired up by processor.rs when BLE is enabled.
    /// sio_connected is set true after a successful WebSocket handshake (not on raw
    /// TCP connect — port probes must not flip it), false on disconnect; notify fires
    /// immediately.
    health_state: Option<Arc<RwLock<GateHealthState>>>,
    health_notify: Option<Arc<Notify>>,
}

impl SocketIOServer {
    /// ID SRS: SRS-FN-SOCKETIO-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a SocketIOServer instance with
    /// host, port, debug configuration, and data processing components.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `host` - Server bind address
    /// * `port` - Server port
    /// * `debug_enabled` - Enable debug logging
    /// * `debug_file` - Debug file handle
    ///
    /// # Returns
    /// New SocketIOServer instance
    pub fn new(
        host: String,
        port: u16,
        debug_enabled: bool,
        debug_file: Arc<RwLock<Option<File>>>,
    ) -> Self {
        Self {
            host,
            port,
            debug_enabled,
            debug_file,
            decompressor: VitalDataDecompressor::new(),
            cleaner: VitalDataCleaner::new(),
            transformer: VitalDataTransformer::new(),
            health_state: None,
            health_notify: None,
        }
    }

    /// ID SRS: SRS-FN-SOCKETIO-005
    /// Title: set_health_hooks
    ///
    /// Description: VRConnect shall wire the health state updater into the Socket.IO
    /// server. When set, sio_connected is toggled on connect/disconnect and
    /// health_notify fires immediately so the health task pushes a fresh payload.
    /// Called by processor.rs when BLE output is enabled; no-op otherwise.
    ///
    /// Version: V1.0
    pub fn set_health_hooks(
        &mut self,
        health_state: Arc<RwLock<GateHealthState>>,
        health_notify: Arc<Notify>,
    ) {
        self.health_state = Some(health_state);
        self.health_notify = Some(health_notify);
    }

    /// ID SRS: SRS-FN-SOCKETIO-002
    /// Title: start
    ///
    /// Description: VRConnect shall start the Socket.IO WebSocket server,
    /// accepting connections and processing incoming vital data.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `tx` - Channel sender for processed data
    ///
    /// # Returns
    /// Result indicating success or error
    #[cfg(not(tarpaulin_include))] // Requires real TCP server, integration test only
    pub async fn start(&self, tx: mpsc::UnboundedSender<ProcessedData>) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| VitalError::Io(e))?;

        log::info!("Socket.IO v4 WebSocket server listening on {}", addr);
        log::info!("✓ Socket.IO server started");

        let tx = Arc::new(tx);
        let decompressor = Arc::new(self.decompressor.clone());
        let cleaner = Arc::new(self.cleaner.clone());
        let transformer = Arc::new(self.transformer.clone());
        let debug_file = self.debug_file.clone();
        let debug_enabled = self.debug_enabled;
        let health_state = self.health_state.clone();
        let health_notify = self.health_notify.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let tx = tx.clone();
                    let decompressor = decompressor.clone();
                    let cleaner = cleaner.clone();
                    let transformer = transformer.clone();
                    let debug_file = debug_file.clone();
                    let health_state = health_state.clone();
                    let health_notify = health_notify.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            stream,
                            addr,
                            tx,
                            decompressor,
                            cleaner,
                            transformer,
                            debug_enabled,
                            debug_file,
                            health_state,
                            health_notify,
                        )
                        .await
                        {
                            log::error!("Connection error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    log::error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// ID SRS: SRS-FN-SOCKETIO-003
    /// Title: handle_connection
    ///
    /// Description: VRConnect shall handle individual WebSocket connection,
    /// performing Socket.IO handshake and processing messages.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `stream` - TCP stream
    /// * `addr` - Client address
    /// * `tx` - Data channel sender
    /// * `decompressor` - Decompressor instance
    /// * `cleaner` - Data cleaner instance
    /// * `transformer` - Data transformer instance
    /// * `debug_enabled` - Debug mode flag
    /// * `debug_file` - Debug file handle
    ///
    /// # Returns
    /// Result indicating success or error
    #[cfg(not(tarpaulin_include))] // Requires real WebSocket connection, integration test only
    async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        tx: Arc<mpsc::UnboundedSender<ProcessedData>>,
        decompressor: Arc<VitalDataDecompressor>,
        cleaner: Arc<VitalDataCleaner>,
        transformer: Arc<VitalDataTransformer>,
        debug_enabled: bool,
        debug_file: Arc<RwLock<Option<File>>>,
        health_state: Option<Arc<RwLock<GateHealthState>>>,
        health_notify: Option<Arc<Notify>>,
    ) -> Result<()> {
        // Handshake FIRST. Port probes and aborted connections (watchdog checks,
        // client retry storms — os error 10053) open a TCP socket without ever
        // speaking WebSocket; they must not flip sio_connected nor push health.
        // Returning Ok keeps them out of the per-connection error log path.
        let ws_stream = match accept_async(stream).await {
            Ok(ws) => ws,
            Err(e) => {
                Self::log_handshake_failure_throttled(addr, &e);
                return Ok(());
            }
        };

        log::info!("New Socket.IO v4 connection from {}", addr);

        // sio connected → immediate health push
        if let (Some(ref hs), Some(ref hn)) = (&health_state, &health_notify) {
            let mut g = hs.write().await;
            g.sio_connection_count = g.sio_connection_count.saturating_add(1);
            g.sio_connected = true;
            drop(g);
            hn.notify_one();
        }

        // Run the connection body separately from the increment/decrement above so
        // that a `?`-propagated send failure (e.g. connection response, pong) still
        // reaches the decrement below instead of skipping it via early return — that
        // used to leave sio_connection_count stuck above 0 and the BLE health payload
        // permanently reporting sio=1 even after the client was gone.
        let result = Self::run_connection(
            ws_stream,
            addr,
            tx,
            decompressor,
            cleaner,
            transformer,
            debug_enabled,
            &debug_file,
        )
        .await;

        // sio disconnected → decrement counter; clear sio only when last connection closes
        if let (Some(ref hs), Some(ref hn)) = (&health_state, &health_notify) {
            let mut g = hs.write().await;
            g.sio_connection_count = g.sio_connection_count.saturating_sub(1);
            g.sio_connected = g.sio_connection_count > 0;
            drop(g);
            hn.notify_one();
        }

        log::info!("Connection handler finished for {}", addr);
        result
    }

    /// ID SRS: SRS-FN-SOCKETIO-007
    /// Title: run_connection
    ///
    /// Description: VRConnect shall run the Socket.IO message loop for an
    /// already-upgraded WebSocket connection, split out of `handle_connection`
    /// so the sio_connection_count decrement in the caller always runs on the
    /// way out, regardless of which `?` (if any) ends this function early.
    ///
    /// Version: V1.0
    #[cfg(not(tarpaulin_include))] // Requires real WebSocket connection, integration test only
    #[allow(clippy::too_many_arguments)]
    async fn run_connection(
        ws_stream: WebSocketStream<TcpStream>,
        addr: SocketAddr,
        tx: Arc<mpsc::UnboundedSender<ProcessedData>>,
        decompressor: Arc<VitalDataDecompressor>,
        cleaner: Arc<VitalDataCleaner>,
        transformer: Arc<VitalDataTransformer>,
        debug_enabled: bool,
        debug_file: &Arc<RwLock<Option<File>>>,
    ) -> Result<()> {
        let (mut write, mut read) = ws_stream.split();

        // Send Socket.IO connection response (Engine.IO v4)
        let sid = uuid::Uuid::new_v4().to_string();
        let connection_response = format!(
            "0{{\"sid\":\"{}\",\"upgrades\":[],\"pingInterval\":25000,\"pingTimeout\":5000}}",
            sid
        );

        write
            .send(Message::Text(connection_response.clone()))
            .await
            .map_err(|e| {
                VitalError::SocketIo(format!("Failed to send connection response: {}", e))
            })?;

        log::debug!("Sent connection response to {}", addr);

        // Debug log
        if debug_enabled {
            if let Some(ref mut file) = *debug_file.write().await {
                let _ = writeln!(
                    file,
                    "\n=== SOCKETIO CONNECTION ===\nClient: {}\nSID: {}\n",
                    addr, sid
                );
            }
        }

        let mut pending_binary_event: Option<String> = None;

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    log::debug!("Received text message from {}: {}", addr, text);

                    // Debug log
                    if debug_enabled {
                        if let Some(ref mut file) = *debug_file.write().await {
                            let _ = writeln!(file, "\n=== TEXT MESSAGE ===\n{}\n", text);
                        }
                    }

                    if text.starts_with("2") {
                        // Engine.IO ping
                        log::debug!("Handling ping from {}", addr);
                        write
                            .send(Message::Text("3".to_string()))
                            .await
                            .map_err(|e| {
                                VitalError::SocketIo(format!("Failed to send pong: {}", e))
                            })?;
                    } else if text.starts_with("40") {
                        // Socket.IO connect
                        log::debug!("Socket.IO namespace connected: {}", addr);
                    } else if text.starts_with("42") {
                        // Socket.IO event
                        let event_data = &text[2..];

                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(event_data) {
                            if let Some(arr) = parsed.as_array() {
                                if let Some(event_name) = arr.get(0).and_then(|v| v.as_str()) {
                                    log::info!("Event '{}' received from {}", event_name, addr);

                                    if event_name == "join_vr" {
                                        if let Some(vr_code) = arr.get(1).and_then(|v| v.as_str()) {
                                            log::info!("VR joined: {}", vr_code);
                                        }
                                    }
                                }
                            }
                        }
                    } else if text.starts_with("451-") {
                        // Binary event placeholder
                        let placeholder_data = &text[4..];
                        log::debug!(
                            "Binary event placeholder from {}: {}",
                            addr,
                            placeholder_data
                        );
                        pending_binary_event = Some(placeholder_data.to_string());
                    }
                }
                Ok(Message::Binary(data)) => {
                    log::debug!(
                        "Received binary message from {}, length: {}",
                        addr,
                        data.len()
                    );

                    // Debug log raw binary
                    if debug_enabled {
                        if let Some(ref mut file) = *debug_file.write().await {
                            let _ = writeln!(
                                file,
                                "\n=== BINARY MESSAGE ===\nLength: {} bytes\nFirst 16 bytes: {:02X?}\n",
                                data.len(),
                                &data[..data.len().min(16)]
                            );
                        }
                    }

                    if pending_binary_event.take().is_some() {
                        match Self::process_data(
                            &data,
                            &decompressor,
                            &cleaner,
                            &transformer,
                            debug_enabled,
                            debug_file,
                        )
                        .await
                        {
                            Ok(processed_data) => {
                                log::info!(
                                    "Successfully processed vital data: {} rooms, {} tracks",
                                    processed_data.rooms.len(),
                                    processed_data.all_tracks.len()
                                );

                                if let Err(e) = tx.send(processed_data) {
                                    log::error!("Failed to send processed data: {}", e);
                                }
                            }
                            Err(e) => {
                                log::error!("Error processing data from {}: {}", addr, e);
                            }
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    log::info!("Socket.IO connection closed: {}", addr);
                    break;
                }
                Ok(Message::Ping(data)) => {
                    write
                        .send(Message::Pong(data))
                        .await
                        .map_err(|e| VitalError::SocketIo(format!("Failed to send pong: {}", e)))?;
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Frame(_)) => {}
                Err(e) => {
                    log::warn!("WebSocket error from {}: {}", addr, e);
                    break;
                }
            }
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-SOCKETIO-006
    /// Title: log_handshake_failure_throttled
    ///
    /// Description: VRConnect shall rate-limit WebSocket handshake failure logs to one
    ///              line per 5 s window, reporting the number of suppressed occurrences.
    ///              Aborted pre-handshake connections (port probes, client retry storms —
    ///              e.g. os error 10053) arrive in bursts of hundreds; logging each one at
    ///              error level saturated the tokio runtime and starved the BLE grace
    ///              timer (observed: 10 s grace firing after 2 min 22 s).
    ///
    /// Version: V1.0
    fn log_handshake_failure_throttled(addr: SocketAddr, err: &dyn std::fmt::Display) {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::OnceLock;

        static START: OnceLock<std::time::Instant> = OnceLock::new();
        static LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);
        static SUPPRESSED: AtomicU64 = AtomicU64::new(0);
        const WINDOW_MS: u64 = 5_000;

        let start = *START.get_or_init(std::time::Instant::now);
        // +1 keeps 0 as the "never logged yet" sentinel.
        let now_ms = start.elapsed().as_millis() as u64 + 1;
        let last = LAST_LOG_MS.load(Ordering::Relaxed);
        let due = last == 0 || now_ms.saturating_sub(last) >= WINDOW_MS;
        if due
            && LAST_LOG_MS
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let suppressed = SUPPRESSED.swap(0, Ordering::Relaxed);
            if suppressed == 0 {
                log::warn!("WebSocket handshake failed from {}: {}", addr, err);
            } else {
                log::warn!(
                    "WebSocket handshake failed from {}: {} ({} similar failures suppressed in the last 5s)",
                    addr,
                    err,
                    suppressed
                );
            }
        } else {
            SUPPRESSED.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// ID SRS: SRS-FN-SOCKETIO-004
    /// Title: process_data
    ///
    /// Description: VRConnect shall process binary data through decompression,
    /// cleaning, and transformation pipeline, with optional debug logging.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `data` - Raw binary data
    /// * `decompressor` - Decompressor instance
    /// * `cleaner` - Data cleaner instance
    /// * `transformer` - Data transformer instance
    /// * `debug_enabled` - Debug mode flag
    /// * `debug_file` - Debug file handle
    ///
    /// # Returns
    /// Processed vital data or error
    async fn process_data(
        data: &[u8],
        decompressor: &VitalDataDecompressor,
        cleaner: &VitalDataCleaner,
        transformer: &VitalDataTransformer,
        debug_enabled: bool,
        debug_file: &Arc<RwLock<Option<File>>>,
    ) -> Result<ProcessedData> {
        // Step 1: Decompress
        let decompressed = decompressor.decompress(data)?;
        log::debug!("Decompressed data length: {}", decompressed.len());

        // Debug log decompressed
        if debug_enabled {
            if let Some(ref mut file) = *debug_file.write().await {
                let _ = writeln!(
                    file,
                    "\n=== DECOMPRESSED DATA ===\nLength: {} bytes\n",
                    decompressed.len()
                );
            }
        }

        // Step 2: Convert to string
        let json_str = String::from_utf8(decompressed)
            .map_err(|e| VitalError::Processing(format!("UTF-8 conversion failed: {}", e)))?;

        // Debug log raw JSON
        if debug_enabled {
            if let Some(ref mut file) = *debug_file.write().await {
                let _ = writeln!(file, "\n=== RAW JSON ===\n{}\n", json_str);
            }
        }

        // Step 3: Clean JSON
        let cleaned_json = cleaner.clean(&json_str)?;

        // Debug log cleaned JSON
        if debug_enabled {
            if let Some(ref mut file) = *debug_file.write().await {
                let _ = writeln!(file, "\n=== CLEANED JSON ===\n{}\n", cleaned_json);
            }
        }

        // Step 4: Parse to VitalData
        let vital_data: crate::domain::VitalData = serde_json::from_str(&cleaned_json)?;

        // Step 5: Transform to ProcessedData
        let processed_data = transformer.transform(vital_data);

        // Debug log processed structure
        if debug_enabled {
            if let Some(ref mut file) = *debug_file.write().await {
                let _ = writeln!(
                    file,
                    "\n=== TRANSFORMATION COMPLETE ===\nDevice: {}\nRooms: {}\nTracks: {}\n",
                    processed_data.device_id,
                    processed_data.rooms.len(),
                    processed_data.all_tracks.len()
                );
            }
        }

        Ok(processed_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TrackType;
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    /// ID SRS: SRS-TEST-SOCKETIO-001
    /// Title: Test SocketIOServer creation
    ///
    /// Description: VRConnect shall create SocketIOServer with configuration.
    ///
    /// Version: V1.0
    #[test]
    fn test_socketio_server_creation() {
        let debug_file = Arc::new(RwLock::new(None));

        let server = SocketIOServer::new("127.0.0.1".to_string(), 3000, false, debug_file);

        assert_eq!(server.host, "127.0.0.1");
        assert_eq!(server.port, 3000);
        assert!(!server.debug_enabled);
    }

    /// ID SRS: SRS-TEST-SOCKETIO-002
    /// Title: Test SocketIOServer creation with debug enabled
    ///
    /// Description: VRConnect shall create SocketIOServer with debug mode.
    ///
    /// Version: V1.0
    #[test]
    fn test_socketio_server_creation_with_debug() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let debug_file = Arc::new(RwLock::new(Some(file)));

        let server = SocketIOServer::new("0.0.0.0".to_string(), 5000, true, debug_file);

        assert_eq!(server.host, "0.0.0.0");
        assert_eq!(server.port, 5000);
        assert!(server.debug_enabled);
    }

    /// ID SRS: SRS-TEST-SOCKETIO-003
    /// Title: Test process_data without debug
    ///
    /// Description: VRConnect shall process valid vital data without debug logging.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_pipeline() {
        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();
        let debug_file = Arc::new(RwLock::new(None));

        // Valid VitalData JSON
        let json_data = r#"{
            "vrcode": "VR-TEST",
            "rooms": [{
                "seqid": 0,
                "roomname": "BED_01",
                "trks": [{
                    "id": "1",
                    "name": "HR",
                    "type": "num",
                    "unit": "bpm",
                    "recs": [{
                        "val": 75,
                        "dt": 1234567890
                    }]
                }],
                "evts": []
            }]
        }"#;

        let data = json_data.as_bytes();

        let result = SocketIOServer::process_data(
            data,
            &decompressor,
            &cleaner,
            &transformer,
            false,
            &debug_file,
        )
        .await;

        assert!(result.is_ok());
        let processed = result.unwrap();
        assert_eq!(processed.device_id, "VR-TEST");
        assert_eq!(processed.rooms.len(), 1);
    }

    /// ID SRS: SRS-TEST-SOCKETIO-004
    /// Title: Test process_data with debug enabled
    ///
    /// Description: VRConnect shall process data and write debug logs.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_with_debug() {
        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();

        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();
        let debug_file = Arc::new(RwLock::new(Some(file)));

        let json_data = r#"{
            "vrcode": "VR-DEBUG",
            "rooms": [{
                "seqid": 0,
                "roomname": "BED_01",
                "trks": [{
                    "id": "1",
                    "name": "SpO2",
                    "type": "num",
                    "unit": "%",
                    "recs": [{
                        "val": 98,
                        "dt": 1234567890
                    }]
                }],
                "evts": []
            }]
        }"#;

        let data = json_data.as_bytes();

        let result = SocketIOServer::process_data(
            data,
            &decompressor,
            &cleaner,
            &transformer,
            true,
            &debug_file,
        )
        .await;

        assert!(result.is_ok());
        let processed = result.unwrap();
        assert_eq!(processed.device_id, "VR-DEBUG");

        // Verify debug file was written
        drop(debug_file);
        let mut file = temp_file.reopen().unwrap();
        let mut contents = String::new();
        std::io::Read::read_to_string(&mut file, &mut contents).unwrap();

        assert!(contents.contains("DECOMPRESSED DATA"));
        assert!(contents.contains("RAW JSON"));
        assert!(contents.contains("CLEANED JSON"));
        assert!(contents.contains("TRANSFORMATION COMPLETE"));
    }

    /// ID SRS: SRS-TEST-SOCKETIO-005
    /// Title: Test process_data with compressed data
    ///
    /// Description: VRConnect shall decompress zlib compressed data.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_compressed() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;

        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();
        let debug_file = Arc::new(RwLock::new(None));

        let json_data = r#"{"vrcode":"VR-COMPRESSED","rooms":[{"seqid":0,"roomname":"BED_01","trks":[{"id":"1","name":"HR","type":"num","unit":"bpm","recs":[{"val":80,"dt":1234567890}]}],"evts":[]}]}"#;

        // Compress data
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json_data.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let result = SocketIOServer::process_data(
            &compressed,
            &decompressor,
            &cleaner,
            &transformer,
            false,
            &debug_file,
        )
        .await;

        assert!(result.is_ok());
        let processed = result.unwrap();
        assert_eq!(processed.device_id, "VR-COMPRESSED");
    }

    /// ID SRS: SRS-TEST-SOCKETIO-006
    /// Title: Test process_data with invalid UTF-8
    ///
    /// Description: VRConnect shall return error for invalid UTF-8 data.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_invalid_utf8() {
        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();
        let debug_file = Arc::new(RwLock::new(None));

        // Invalid UTF-8 bytes
        let invalid_data = vec![0xFF, 0xFE, 0xFD];

        let result = SocketIOServer::process_data(
            &invalid_data,
            &decompressor,
            &cleaner,
            &transformer,
            false,
            &debug_file,
        )
        .await;

        assert!(result.is_err());
    }

    /// ID SRS: SRS-TEST-SOCKETIO-007
    /// Title: Test process_data with invalid JSON
    ///
    /// Description: VRConnect shall return error for invalid JSON.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_invalid_json() {
        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();
        let debug_file = Arc::new(RwLock::new(None));

        let invalid_json = b"{ this is not valid json }";

        let result = SocketIOServer::process_data(
            invalid_json,
            &decompressor,
            &cleaner,
            &transformer,
            false,
            &debug_file,
        )
        .await;

        assert!(result.is_err());
    }

    /// ID SRS: SRS-TEST-SOCKETIO-008
    /// Title: Test process_data with empty data
    ///
    /// Description: VRConnect shall handle empty data gracefully.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_empty() {
        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();
        let debug_file = Arc::new(RwLock::new(None));

        let empty_data = b"";

        let result = SocketIOServer::process_data(
            empty_data,
            &decompressor,
            &cleaner,
            &transformer,
            false,
            &debug_file,
        )
        .await;

        assert!(result.is_err());
    }

    /// ID SRS: SRS-TEST-SOCKETIO-009
    /// Title: Test process_data with waveform data
    ///
    /// Description: VRConnect shall process waveform tracks correctly.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_waveform() {
        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();
        let debug_file = Arc::new(RwLock::new(None));

        let json_data = r#"{
            "vrcode": "VR-WAVE",
            "rooms": [{
                "seqid": 0,
                "roomname": "BED_01",
                "trks": [{
                    "id": "1",
                    "name": "ECG",
                    "type": "wav",
                    "unit": "mV",
                    "recs": [{
                        "val": [0.1, 0.2, 0.3, 0.4, 0.5],
                        "dt": 1234567890
                    }]
                }],
                "evts": []
            }]
        }"#;

        let result = SocketIOServer::process_data(
            json_data.as_bytes(),
            &decompressor,
            &cleaner,
            &transformer,
            false,
            &debug_file,
        )
        .await;

        assert!(result.is_ok());
        let processed = result.unwrap();
        assert_eq!(processed.all_tracks.len(), 1);
        assert_eq!(processed.all_tracks[0].track_type, TrackType::Waveform);
        assert!(processed.all_tracks[0].waveform_points.is_some());
    }

    /// ID SRS: SRS-TEST-SOCKETIO-010
    /// Title: Test process_data with multiple rooms
    ///
    /// Description: VRConnect shall process multiple rooms correctly.
    ///
    /// Version: V1.0
    #[tokio::test]
    async fn test_process_data_multiple_rooms() {
        let decompressor = VitalDataDecompressor::new();
        let cleaner = VitalDataCleaner::new();
        let transformer = VitalDataTransformer::new();
        let debug_file = Arc::new(RwLock::new(None));

        let json_data = r#"{
            "vrcode": "VR-MULTI",
            "rooms": [
                {
                    "seqid": 0,
                    "roomname": "BED_01",
                    "trks": [{
                        "id": "1",
                        "name": "HR",
                        "type": "num",
                        "unit": "bpm",
                        "recs": [{"val": 75, "dt": 1234567890}]
                    }],
                    "evts": []
                },
                {
                    "seqid": 1,
                    "roomname": "BED_02",
                    "trks": [{
                        "id": "2",
                        "name": "SpO2",
                        "type": "num",
                        "unit": "%",
                        "recs": [{"val": 98, "dt": 1234567890}]
                    }],
                    "evts": []
                }
            ]
        }"#;

        let result = SocketIOServer::process_data(
            json_data.as_bytes(),
            &decompressor,
            &cleaner,
            &transformer,
            false,
            &debug_file,
        )
        .await;

        assert!(result.is_ok());
        let processed = result.unwrap();
        assert_eq!(processed.rooms.len(), 2);
        assert_eq!(processed.all_tracks.len(), 2);
    }

    /// ID SRS: SRS-TEST-SOCKETIO-011
    /// Title: Test set_health_hooks wiring
    ///
    /// Description: VRConnect shall leave health hooks unset at construction and
    /// wire health state + notify into the server when set_health_hooks is called.
    ///
    /// Version: V1.0
    #[test]
    fn test_set_health_hooks() {
        let debug_file = Arc::new(RwLock::new(None));
        let mut server = SocketIOServer::new("127.0.0.1".to_string(), 3000, false, debug_file);

        assert!(server.health_state.is_none());
        assert!(server.health_notify.is_none());

        let health_state = Arc::new(RwLock::new(GateHealthState::default()));
        let health_notify = Arc::new(Notify::new());
        server.set_health_hooks(health_state, health_notify);

        assert!(server.health_state.is_some());
        assert!(server.health_notify.is_some());
    }

    /// ID SRS: SRS-TEST-SOCKETIO-012
    /// Title: Test handshake failure log throttling
    ///
    /// Description: VRConnect shall log the first handshake failure of a 5 s window
    /// and route rapid follow-up failures through the suppression counter instead of
    /// logging each one. Regression test for the log-storm incident that starved the
    /// BLE grace timer (os error 10053 bursts).
    ///
    /// Version: V1.0
    #[test]
    fn test_log_handshake_failure_throttled() {
        let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let err = "Connection reset without closing handshake (os error 10053)";

        // First call of the window takes the logging branch; the rapid follow-ups
        // take the suppression branch. Neither must panic.
        for _ in 0..4 {
            SocketIOServer::log_handshake_failure_throttled(addr, &err);
        }
    }
}
