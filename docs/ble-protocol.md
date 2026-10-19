# BLE protocol — IDT v1.1

**GATT service:** `12345678-1234-1234-1234-1234567890ab`

| Characteristic | UUID suffix | Property | Usage |
|---|---|---|---|
| Catalog | `…90ae` | Read | Available signals (TLV) |
| Data IN | `…90ac` | Write | ACK / NACK from the tablet |
| Data OUT | `…90ad` | Notify | DATA_FRAME, SUBSCRIBE_RSP |
| Subscribe | `…90af` | Write | SUBSCRIBE_REQ |
| Control | `…90b0` | Notify | MiniPC health payload (see [Health monitoring](health.md)) |
| Unsubscribe | `…90b1` | Write | Unsubscribe |

## DATA_FRAME (34 bytes)

```
magic 0xD17A | version | msg_type=0x10 | flags | session_id | stream_id | seq
| t0_ms (8 B) | count=1 | payloadLen=6 | dt_ms | value f32 | CRC32C
```

Example signals: HR `0x0101`, SpO2 `0x0102`, temperature `0x0103`, AMB_PRES `0x0501`.
The actual list comes from the configuration (`BLE_VALUES`).

## Resilience

| Mechanism | What it covers |
|---|---|
| **Grace period** (10 s) | Clean disconnect (CCCD → 0): the session is kept and resumes transparently |
| **Supervision timeout** (30 s, 300 s in production) | Silent loss (tablet out of range, no ACK): session reset |
| **Ring buffer + checkpoint** | 6 h of history per signal, reloaded on restart |
| **WAL** (opt-in) | Data-loss window of 2 s or less on crash, less disk wear |
| **`period_ms` throttle** | Cadence requested by the tablet (e.g. 1 h), kept across re-subscribes |
| **Hourly catch-up** (opt-in) | On re-subscribe, one value per missed period, flagged `FLAG_BACKLOG` |


The app reconnects automatically and identifies the MiniPC by **service UUID**, not by advertising name. The UUID is the same on every MiniPC, so with two MiniPCs in radio range the tablet may reconnect to the wrong one.
