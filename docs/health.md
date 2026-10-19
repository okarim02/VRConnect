# Health monitoring

The MiniPC has no hospital network access, so its health is reported over BLE. The payload is sent
as raw JSON on the **Control** characteristic (`…90b0`) every `HEALTH_CHECK_INTERVAL_SEC` (30 s),
and immediately whenever a state changes:

```json
{"ts":1711497600,"ver":1,"ok":1,"gate":1,"sio":1,"ble":1,"flow":1,"vr":1,"disk":1,"wd_vr":1,"wd_gate":1,"adv":1}
```

| Field | Source | Meaning |
|---|---|---|
| `ts` | GATE | Unix timestamp (seconds) |
| `ver` | GATE | Schema version, bumped only on breaking changes |
| `ok` | GATE | 1 only if all indicators are 1 (`adv` excluded) |
| `gate` | GATE | VRConnect process alive |
| `sio` | GATE | Socket.IO connected to VitalRecorder |
| `ble` | GATE | At least one IDT stream active |
| `flow` | GATE | Data received within `HEALTH_BLE_FLOW_TIMEOUT_SEC` |
| `adv` | GATE | BLE advertising running (informational) |
| `vr` | `HealthWriter.ps1` | VitalRecorder process alive |
| `disk` | `HealthWriter.ps1` | Enough free disk space |
| `wd_vr`, `wd_gate` | `HealthWriter.ps1` | Watchdogs running |

`HealthWriter.ps1` (part of the deployment package) writes the OS-side fields to
`logs/health.json`. GATE reads that file and merges it with its own state. If the file is missing
or stale, the OS-side fields are forced to 0 (fail-safe).

The full specification is in the Alison documentation repository (`docs/health/GATE-Health-Spec.md`).
