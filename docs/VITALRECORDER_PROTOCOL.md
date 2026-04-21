# VitalRecorder Protocol — Reverse-Engineering Notes

> VitalRecorder is **closed-source**. Everything here was obtained by observation,
> traffic analysis, and code inspection. No official documentation exists for the
> Socket.IO interface. Handle with care.

---

## 1. Connection basics

VitalRecorder acts as a **Socket.IO v4 client**. VRConnect acts as the **server**.

| Parameter | Default | Env var |
|-----------|---------|---------|
| Bind address | `127.0.0.1` | `SOCKETIO_HOST` |
| Port | `3000` | `SOCKETIO_PORT` |

VitalRecorder connects automatically when started. No authentication, no API key.
The connection URL VitalRecorder targets is configured in its `vr.conf` file
(location: VitalRecorder install directory). Relevant field:

```
gate_server = ws://127.0.0.1:3000
```

### Handshake sequence

```
VR  →  VRConnect  :  WS upgrade request
VRConnect  →  VR  :  "0{"sid":"<uuid>","upgrades":[],"pingInterval":25000,"pingTimeout":5000}"
VR  →  VRConnect  :  "40"            (Socket.IO namespace connect)
VR  →  VRConnect  :  "42["join_vr","<vr_code>"]"   (device registration)
VR  →  VRConnect  :  "451-[...]"     (text frame announcing binary payload)
VR  →  VRConnect  :  <binary data>   (zlib-compressed JSON)
... repeats every ~1 second ...
VR  →  VRConnect  :  "2"             (Engine.IO ping)
VRConnect  →  VR  :  "3"             (Engine.IO pong)
```

---

## 2. Wire format — binary payload

Each data push is a **two-frame sequence**:

### Frame 1 — text placeholder (Socket.IO v4 binary event)

```
"451-["vitaldata",{"_placeholder":true,"num":0}]"
 ^^^
 └── "45" = Socket.IO binary event type
```

### Frame 2 — binary payload

```
[0x04] [0x78] [0x9C / 0xDA / 0x01] [...zlib body...]
  │      └──────────────────────────── zlib header
  └── Socket.IO v4 binary indicator (strip before decompressing)
```

**Detection logic** (`decompressor.rs`):

```
byte[0] == 0x04 && byte[1] == 0x78  →  Socket.IO binary + zlib  →  decompress byte[1..]
byte[0] == 0x78 && byte[1] in {0x9C, 0xDA, 0x01}  →  raw zlib  →  decompress byte[0..]
otherwise  →  plain JSON, use as-is
```

**Rust crate used:** `flate2` (ZlibDecoder)

After decompression: UTF-8 JSON string.

---

## 3. JSON structure

```json
{
  "device_id": "x7c6kkgsy",
  "rooms": [
    {
      "room_index": 0,
      "room_name": "x7c6kkgsy",
      "tracks": [
        {
          "name": "HR",
          "srate": null,
          "recs": [
            { "val": 65.0, "dt": 1776672194 },
            { "val": 65.0, "dt": 1776672194 },
            { "val": 65.0, "dt": 1776672194 }
          ]
        }
      ]
    }
  ],
  "all_tracks": [ /* same tracks, flattened */ ],
  "timestamp": 1776672194783
}
```

### Key fields

| Field | Type | Notes |
|-------|------|-------|
| `device_id` / `vr_code` | string | Device identifier, sent in `join_vr` |
| `rooms[].tracks[].name` | string | Signal name — see aliases below |
| `recs[].val` | number / array | Scalar or waveform array |
| `recs[].dt` | int (seconds) | Unix timestamp **in seconds** |
| `timestamp` | int (ms) | Message emission time in **milliseconds** |

---

## 4. Timestamp unit — critical gotcha

`recs[].dt` is in **seconds** (10 digits, e.g. `1776672194`).
`timestamp` (top-level) is in **milliseconds** (13 digits, e.g. `1776672194783`).

VRConnect auto-detects:
```rust
let ts_ms = if ts < 10_000_000_000 { ts * 1000 } else { ts };
```
Flutter simulators send `dt` in milliseconds — this heuristic handles both cases.

---

## 5. Signal name aliases

VitalRecorder uses its own naming. VRConnect maps them to IDT signal IDs:

| IDT Signal | IDT ID | VitalRecorder names accepted |
|------------|--------|------------------------------|
| HR | `0x0101` | `HR` |
| SpO2 | `0x0102` | `SPO2`, `PLETH`, `PLETH_SPO2` |
| Temperature | `0x0103` | `TEMP`, `TEMPERATURE`, `BT`, `BT1`, `BT1_TEMP` |

Only `room_index = 0` (BED_01) is forwarded to BLE.

---

## 6. Known behavioral quirks

### 6.1 — Multiple records per scalar signal per second

**Observed:** for every numeric signal (HR, SpO2, Temp), VitalRecorder sends
**2 to 3 records with the same `dt` and the same `val`** in a single message.

```
recs: [
  { val: 65.0, dt: 1776672194 },   ← same
  { val: 65.0, dt: 1776672194 },   ← same
  { val: 65.0, dt: 1776672194 }    ← same
]
```

**Why:** VitalRecorder uses the same `recs` array structure for waveform signals
(ECG, PLETH) where multiple samples per second are legitimate. Scalar signals
inherit this structure but all records within a second carry the same value.

**Impact without deduplication:**
- BLE: MyPredi receives 3 identical DATA_FRAMEs → server stores 3× the measures
- JSON recording: file is ~3× larger than necessary for scalar signals

**Fix (BLE path):** deduplicate on `(signal_id, t0_ms)` inside `ble_reliable.rs::output()`
before calling `add_data()`. See `TODO` in that function.

---

### 6.2 — Demo mode is a random generator, not a file replay

**Observed** on a 27-minute recording (`vrconnect_20260420_100305_ongoing.json`):

| Signal | Range | Unique values | Distribution |
|--------|-------|---------------|--------------|
| HR | 50–70 bpm | 21 integers | Approx. uniform |
| SpO2 | ~80–100% | 36 floats | Discrete set |
| Temp | 36.0–38.0°C | 21 values (0.1 steps) | Discrete set |

- No loop detected (first 10 values do not repeat in order)
- No timestamp gaps or resets
- All HR values are exact integers

**Conclusion:** demo mode is a **programmatic random generator** drawing from
finite discrete sets, not a pre-recorded patient file replayed in a loop.
Values repeat frequently due to the small set sizes, not due to looping.

---

### 6.3 — `all_tracks` duplicates `rooms[].tracks`

The top-level `all_tracks` array is a flat copy of all tracks across all rooms.
It is redundant with `rooms[].tracks[]`. VRConnect uses `all_tracks` for
simplicity. Both contain the same data with the same quirks (6.1).

---

## 7. VRConnect processing pipeline

```
VitalRecorder (Socket.IO client)
    ↓  WS binary frame (Engine.IO v4)
SocketIOServer::handle_connection()
    ↓  strip 0x04 prefix + zlib decompress (flate2)
VitalDataDecompressor::decompress()
    ↓  JSON → VitalData struct (serde_json)
VitalDataCleaner::clean()         ← removes null/invalid records
    ↓
VitalDataTransformer::transform() ← 1 ProcessedTrack per record (see §6.1)
    ↓  ProcessedData (channel)
processor.rs
    ├→ file.rs::output()          ← JSON recording (no dedup)
    └→ ble_reliable.rs::output()  ← BLE notify (dedup needed, see §6.1)
```
