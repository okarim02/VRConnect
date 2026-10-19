# Configuration

VRConnect reads a `.env` file passed with `--config-file`. For development, start from
`example.env`, which lists every parameter with its default.

!!! info "Production"
    On a deployed MiniPC, the single source of truth is `config/config.toml` in the deployment
    package. `Install.ps1` **generates** `config/.env` from it: never edit the `.env` by hand.

| Variable | Purpose | Default |
|---|---|---|
| `SOCKETIO_HOST` / `SOCKETIO_PORT` | Socket.IO listener (VitalRecorder) | `127.0.0.1:3000` |
| `OUTPUT_BLE_ENABLED`, `OUTPUT_BLE_DEVICE_NAME`, `OUTPUT_BLE_SERVICE_UUID` | BLE output | see `example.env` |
| `BLE_VALUES` | Signals streamed over BLE | `HR,SPO2,NIBP_SYS,NIBP_DIA` |
| `BLE_GRACE_PERIOD_SEC` | Resume window after a clean disconnect | `10` |
| `BLE_SUPERVISION_TIMEOUT_SEC` | Session reset when frames are pending with no ACK | `30` (`300` in hospital production) |
| `HISTORY_RETENTION_SEC` | Ring-buffer window | `21600` (6 h) |
| `HISTORY_CHECKPOINT_*` | Periodic history snapshot | 30 s, max age 6 h |
| `WAL_ENABLED`, `WAL_FSYNC_INTERVAL_SEC`, `WAL_COMPACTION_INTERVAL_SEC` | Write-ahead log (opt-in) | `false`, 2 s, 3600 s |
| `FORCE_BACKLOG_REPLAY` | Forces `BACKLOG_THEN_LIVE` on reconnect | `false` |
| `HOURLY_CATCHUP` | Re-sends one value per missed period on re-subscribe | `false` |
| `HEALTH_CHECK_INTERVAL_SEC` | Health heartbeat | `30` |
| `HEALTH_BLE_FLOW_TIMEOUT_SEC` | Time without data before `flow=0` | `60` |
| `LOG_LEVEL`, `LOG_DIR`, `DEBUG_ENABLED` | Logging | `INFO` |
