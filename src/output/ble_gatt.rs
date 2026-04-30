// /src/output/ble_gatt.rs
// Module: output.ble_gatt
// Purpose: Custom Windows BLE GATT server with Read + Write + Notify support.
//
// The ble-windows-server crate v0.2.1 only supports Read + Notify characteristics.
// This module adds Write callback support required by the PDF protocol spec:
//
//   Catalog   (0x90ae) → Read    (static value)
//   Data_IN   (0x90ac) → Write   (ACK frames from client)
//   Data_OUT  (0x90ad) → Notify  (data frames to client)
//   Subscribe (0x90af) → Write   (client subscribes to signals)
//   Control   (0x90b0) → Notify  (session control events)
//   Unsubscribe(0x90b1)→ Write   (client unsubscribes from signals)
//
// Write events are delivered to the caller via an async mpsc channel (WriteEvent).

use crate::error::{Result, VitalError};
use std::collections::HashMap;
use tokio::sync::mpsc;

use windows::{
    core::{IInspectable, GUID},
    Devices::Bluetooth::{
        BluetoothError,
        GenericAttributeProfile::{
            GattCharacteristicProperties, GattLocalCharacteristic,
            GattLocalCharacteristicParameters, GattServiceProvider,
            GattServiceProviderAdvertisingParameters, GattWriteRequestedEventArgs,
        },
    },
    Foundation::TypedEventHandler,
    Storage::Streams::{DataReader, DataWriter},
};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Event emitted when a BLE client writes to a characteristic.
#[derive(Debug, Clone)]
pub struct WriteEvent {
    /// Name of the characteristic that was written to (e.g. "Data_IN", "Subscribe")
    pub characteristic_name: String,
    /// Raw bytes written by the client
    pub data: Vec<u8>,
}

/// Supported BLE characteristic property flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CharProperty {
    Read,
    Write,
    WriteWithoutResponse,
    Notify,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Pre-start configuration for a single characteristic.
struct CharConfig {
    name: String,
    uuid: uuid::Uuid,
    properties: Vec<CharProperty>,
    /// Optional static read value (set via `set_read_value`)
    read_value: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// GattServer
// ---------------------------------------------------------------------------

/// Custom Windows BLE GATT server with full Read / Write / Notify support.
///
/// # Lifecycle
/// 1. `new()` → stores device name + service UUID, creates write channel
/// 2. `add_characteristic()` → registers characteristic configs
/// 3. `set_read_value()` → sets static read payload for Read chars (e.g. Catalog)
/// 4. `take_write_receiver()` → caller takes the receiving end of write events
/// 5. `start()` → creates the Windows GATT service, registers handlers, advertises
/// 6. `notify()` → pushes data on a Notify characteristic
pub struct GattServer {
    device_name: String,
    service_uuid: uuid::Uuid,
    chars: Vec<CharConfig>,
    write_tx: mpsc::UnboundedSender<WriteEvent>,
    write_rx: Option<mpsc::UnboundedReceiver<WriteEvent>>,
    /// Fires `()` when the Central's CCCD subscription on Data_OUT drops to 0 (disconnect).
    disconnect_tx: mpsc::UnboundedSender<()>,
    disconnect_rx: Option<mpsc::UnboundedReceiver<()>>,
    /// Runtime: GATT local characteristics (populated after `start()`)
    local_chars: HashMap<String, GattLocalCharacteristic>,
    /// Runtime: GATT service provider (populated after `start()`)
    provider: Option<GattServiceProvider>,
    running: bool,
}

impl GattServer {
    /// Create a new GATT server (does not start it).
    pub fn new(device_name: String, service_uuid: uuid::Uuid) -> Self {
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let (disconnect_tx, disconnect_rx) = mpsc::unbounded_channel();
        Self {
            device_name,
            service_uuid,
            chars: Vec::new(),
            write_tx,
            write_rx: Some(write_rx),
            disconnect_tx,
            disconnect_rx: Some(disconnect_rx),
            local_chars: HashMap::new(),
            provider: None,
            running: false,
        }
    }

    /// Register a characteristic to be created when `start()` is called.
    pub fn add_characteristic(
        &mut self,
        name: &str,
        uuid: uuid::Uuid,
        properties: &[CharProperty],
    ) -> &mut Self {
        self.chars.push(CharConfig {
            name: name.to_string(),
            uuid,
            properties: properties.to_vec(),
            read_value: None,
        });
        self
    }

    /// Set a static read value for a Read characteristic (e.g. Catalog bytes).
    /// Must be called before `start()`.
    pub fn set_read_value(&mut self, name: &str, value: Vec<u8>) {
        if let Some(cfg) = self.chars.iter_mut().find(|c| c.name == name) {
            cfg.read_value = Some(value);
        }
    }

    /// Take the write-event receiver. Must be called exactly once before `start()`.
    pub fn take_write_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<WriteEvent>> {
        self.write_rx.take()
    }

    /// Take the disconnect receiver. Fires `()` when Data_OUT CCCD subscriber count drops to 0.
    /// Must be called exactly once before `start()`.
    pub fn take_disconnect_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<()>> {
        self.disconnect_rx.take()
    }

    /// Start the GATT server: create the Windows BLE service, register characteristic
    /// handlers, and begin advertising.
    pub async fn start(&mut self) -> Result<()> {
        let service_guid = uuid_to_guid(&self.service_uuid);

        // 1. Create the GATT service provider
        log::info!(
            "Creating GATT service provider (UUID: {})",
            self.service_uuid
        );
        let provider_result = GattServiceProvider::CreateAsync(service_guid)
            .map_err(|e| VitalError::Config(format!("CreateAsync call failed: {}", e)))?
            .get()
            .map_err(|e| VitalError::Config(format!("CreateAsync await failed: {}", e)))?;

        let bt_error = provider_result
            .Error()
            .map_err(|e| VitalError::Config(format!("Error() failed: {}", e)))?;
        if bt_error != BluetoothError::Success {
            return Err(VitalError::Config(format!(
                "Bluetooth error creating service: {:?}",
                bt_error
            )));
        }

        let provider = provider_result
            .ServiceProvider()
            .map_err(|e| VitalError::Config(format!("ServiceProvider() failed: {}", e)))?;

        let service = provider
            .Service()
            .map_err(|e| VitalError::Config(format!("Service() failed: {}", e)))?;

        // 2. Create each characteristic
        for cfg in &self.chars {
            let char_guid = uuid_to_guid(&cfg.uuid);
            let params = GattLocalCharacteristicParameters::new()
                .map_err(|e| VitalError::Config(format!("CharParams::new failed: {}", e)))?;

            // Build property flags
            let mut props = GattCharacteristicProperties::None;
            for p in &cfg.properties {
                props |= match p {
                    CharProperty::Read => GattCharacteristicProperties::Read,
                    CharProperty::Write => GattCharacteristicProperties::Write,
                    CharProperty::WriteWithoutResponse => {
                        GattCharacteristicProperties::WriteWithoutResponse
                    }
                    CharProperty::Notify => GattCharacteristicProperties::Notify,
                };
            }
            params
                .SetCharacteristicProperties(props)
                .map_err(|e| VitalError::Config(format!("SetProperties failed: {}", e)))?;

            // Set static read value if provided (e.g. Catalog)
            if let Some(ref read_val) = cfg.read_value {
                let writer = DataWriter::new()
                    .map_err(|e| VitalError::Config(format!("DataWriter::new failed: {}", e)))?;
                writer
                    .WriteBytes(read_val)
                    .map_err(|e| VitalError::Config(format!("WriteBytes failed: {}", e)))?;
                let buffer = writer
                    .DetachBuffer()
                    .map_err(|e| VitalError::Config(format!("DetachBuffer failed: {}", e)))?;
                params
                    .SetStaticValue(&buffer)
                    .map_err(|e| VitalError::Config(format!("SetStaticValue failed: {}", e)))?;
            }

            // Create the characteristic on the service
            let char_result = service
                .CreateCharacteristicAsync(char_guid, &params)
                .map_err(|e| VitalError::Config(format!("CreateCharAsync call failed: {}", e)))?
                .get()
                .map_err(|e| VitalError::Config(format!("CreateCharAsync await failed: {}", e)))?;

            let local_char = char_result
                .Characteristic()
                .map_err(|e| VitalError::Config(format!("Characteristic() failed: {}", e)))?;

            // Register write handler if this characteristic is writable
            let has_write = cfg
                .properties
                .iter()
                .any(|p| matches!(p, CharProperty::Write | CharProperty::WriteWithoutResponse));

            if has_write {
                let tx = self.write_tx.clone();
                let char_name = cfg.name.clone();

                local_char
                    .WriteRequested(&TypedEventHandler::<
                        GattLocalCharacteristic,
                        GattWriteRequestedEventArgs,
                    >::new(move |_sender, args| {
                        if let Some(args) = args {
                            let deferral = args.GetDeferral()?;
                            let request = args.GetRequestAsync()?.get()?;
                            let value = request.Value()?;
                            let reader = DataReader::FromBuffer(&value)?;
                            let len = reader.UnconsumedBufferLength()? as usize;

                            // Minimum-length guard: IDT/ACK frames need ≥ 2 bytes to determine frame type.
                            // Control writes are pull requests — any length (including empty) is valid.
                            if len < 2 && char_name != "Control" {
                                log::warn!(
                                    "BLE write on '{}': {} byte(s) — too short for any \
                                         IDT/ACK frame, discarded",
                                    char_name,
                                    len
                                );
                                let _ = request.Respond();
                                deferral.Complete()?;
                                return Ok(());
                            }

                            let mut data = vec![0u8; len];
                            reader.ReadBytes(&mut data)?;

                            log::debug!(
                                "BLE write received on '{}': {} bytes",
                                char_name,
                                data.len()
                            );

                            let _ = tx.send(WriteEvent {
                                characteristic_name: char_name.clone(),
                                data,
                            });

                            // Respond for WriteWithResponse requests
                            let _ = request.Respond();

                            deferral.Complete()?;
                        }
                        Ok(())
                    }))
                    .map_err(|e| {
                        VitalError::Config(format!(
                            "WriteRequested handler failed for '{}': {}",
                            cfg.name, e
                        ))
                    })?;
            }

            // Register SubscribedClientsChanged on Data_OUT to detect Central disconnection.
            // Fires when the CCCD subscriber count changes; we act only when it drops to 0.
            if cfg.name == "Data_OUT" {
                let disc_tx = self.disconnect_tx.clone();
                local_char
                    .SubscribedClientsChanged(&TypedEventHandler::<
                        GattLocalCharacteristic,
                        IInspectable,
                    >::new(move |sender, _args| {
                        if let Some(char_ref) = sender {
                            let n = char_ref
                                .SubscribedClients()
                                .and_then(|c| c.Size())
                                .unwrap_or(1);
                            if n == 0 {
                                log::info!(
                                    "[BLE] Data_OUT: CCCD subscriber count → 0 (Central disconnected)"
                                );
                                let _ = disc_tx.send(());
                            }
                        }
                        Ok(())
                    }))
                    .map_err(|e| {
                        VitalError::Config(format!(
                            "SubscribedClientsChanged handler failed for '{}': {}",
                            cfg.name, e
                        ))
                    })?;
            }

            log::info!(
                "  Characteristic '{}' created -> {} ({:?})",
                cfg.name,
                cfg.uuid,
                cfg.properties
            );

            self.local_chars.insert(cfg.name.clone(), local_char);
        }

        // 3. Start advertising (connectable + discoverable)
        let adv_params = GattServiceProviderAdvertisingParameters::new()
            .map_err(|e| VitalError::Config(format!("AdvParams::new failed: {}", e)))?;
        adv_params
            .SetIsConnectable(true)
            .map_err(|e| VitalError::Config(format!("SetIsConnectable failed: {}", e)))?;
        adv_params
            .SetIsDiscoverable(true)
            .map_err(|e| VitalError::Config(format!("SetIsDiscoverable failed: {}", e)))?;
        provider
            .StartAdvertisingWithParameters(&adv_params)
            .map_err(|e| VitalError::Config(format!("StartAdvertising failed: {}", e)))?;

        self.provider = Some(provider);
        self.running = true;

        log::info!(
            "GATT server '{}' started and advertising (service {})",
            self.device_name,
            self.service_uuid
        );

        Ok(())
    }

    /// Send a notification on a Notify characteristic.
    pub async fn notify(&self, name: &str, data: &[u8]) -> Result<()> {
        let local_char = self
            .local_chars
            .get(name)
            .ok_or_else(|| VitalError::Config(format!("Unknown characteristic '{}'", name)))?;

        // Warn early if no client has enabled CCCD — NotifyValueAsync silently drops in that case.
        match local_char.SubscribedClients() {
            Ok(clients) => {
                let n = clients.Size().unwrap_or(0);
                if n == 0 {
                    log::warn!(
                        "notify '{}': 0 CCCD subscribers — tablet has not enabled notifications on this characteristic",
                        name
                    );
                } else {
                    log::debug!("notify '{}': {} CCCD subscriber(s)", name, n);
                }
            }
            Err(e) => log::warn!("notify '{}': SubscribedClients() failed: {}", name, e),
        }

        let writer =
            DataWriter::new().map_err(|e| VitalError::Config(format!("DataWriter::new: {}", e)))?;
        writer
            .WriteBytes(data)
            .map_err(|e| VitalError::Config(format!("WriteBytes: {}", e)))?;
        let buffer = writer
            .DetachBuffer()
            .map_err(|e| VitalError::Config(format!("DetachBuffer: {}", e)))?;

        local_char
            .NotifyValueAsync(&buffer)
            .map_err(|e| VitalError::Config(format!("NotifyValueAsync call: {}", e)))?
            .get()
            .map_err(|e| VitalError::Config(format!("NotifyValueAsync await: {}", e)))?;

        Ok(())
    }

    /// Whether the server is currently running.
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Stop advertising and tear down the GATT service.
    pub fn stop(&mut self) {
        if let Some(ref provider) = self.provider {
            let _ = provider.StopAdvertising();
        }
        self.local_chars.clear();
        self.provider = None;
        self.running = false;
        log::info!("GATT server stopped");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `uuid::Uuid` to a Windows `GUID`.
fn uuid_to_guid(uuid: &uuid::Uuid) -> GUID {
    let b = uuid.as_bytes();
    GUID {
        data1: u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
        data2: u16::from_be_bytes([b[4], b[5]]),
        data3: u16::from_be_bytes([b[6], b[7]]),
        data4: [b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]],
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_to_guid() {
        let uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let guid = uuid_to_guid(&uuid);
        assert_eq!(guid.data1, 0x12345678);
        assert_eq!(guid.data2, 0x1234);
        assert_eq!(guid.data3, 0x1234);
        assert_eq!(guid.data4, [0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB]);
    }

    #[test]
    fn test_gatt_server_new() {
        let uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let server = GattServer::new("TestDevice".to_string(), uuid);
        assert!(!server.is_running());
        assert_eq!(server.device_name, "TestDevice");
    }

    #[test]
    fn test_add_characteristics() {
        let uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let mut server = GattServer::new("Test".to_string(), uuid);

        let cat_uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-12345678ffff").unwrap();
        server.add_characteristic("Catalog", cat_uuid, &[CharProperty::Read]);

        let din_uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-12345678fffe").unwrap();
        server.add_characteristic(
            "Data_IN",
            din_uuid,
            &[CharProperty::Write, CharProperty::WriteWithoutResponse],
        );

        assert_eq!(server.chars.len(), 2);
        assert_eq!(server.chars[0].name, "Catalog");
        assert_eq!(server.chars[1].name, "Data_IN");
    }

    #[test]
    fn test_set_read_value() {
        let uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let mut server = GattServer::new("Test".to_string(), uuid);

        let cat_uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-12345678ffff").unwrap();
        server.add_characteristic("Catalog", cat_uuid, &[CharProperty::Read]);

        let catalog_data = vec![1, 2, 3, 4, 5];
        server.set_read_value("Catalog", catalog_data.clone());

        assert_eq!(server.chars[0].read_value.as_ref().unwrap(), &catalog_data);
    }

    #[test]
    fn test_take_write_receiver() {
        let uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let mut server = GattServer::new("Test".to_string(), uuid);

        // First take should succeed
        assert!(server.take_write_receiver().is_some());
        // Second take should return None
        assert!(server.take_write_receiver().is_none());
    }

    #[test]
    fn test_write_event_channel() {
        let uuid = uuid::Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let mut server = GattServer::new("Test".to_string(), uuid);
        let mut rx = server.take_write_receiver().unwrap();

        // Simulate a write event via the internal sender
        let event = WriteEvent {
            characteristic_name: "Data_IN".to_string(),
            data: vec![0x01, 0x02],
        };
        server.write_tx.send(event.clone()).unwrap();

        // Receive it
        let received = rx.try_recv().unwrap();
        assert_eq!(received.characteristic_name, "Data_IN");
        assert_eq!(received.data, vec![0x01, 0x02]);
    }

    #[test]
    fn test_char_properties() {
        assert_ne!(CharProperty::Read, CharProperty::Write);
        assert_ne!(CharProperty::Write, CharProperty::WriteWithoutResponse);
        assert_ne!(CharProperty::Notify, CharProperty::Read);
    }
}
