# DeltaSync: Delta Exchange 24-Hour Settlement ETL & Validation Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0%20external-brightgreen.svg)](#zero-external-dependencies)
[![Environment: Termux & Linux](https://img.shields.io/badge/platform-Termux%20%7C%20Linux%20%7C%20Windows-orange.svg)](#termux-mobile-setup)

**DeltaSync** is an ultra-lightweight, zero-external-dependency Python pipeline engineered to extract 24-hour settlement-aligned market data from **Delta Exchange (India)**, store records idempotently across isolated SQLite databases with Write-Ahead Logging (WAL), audit mathematical data continuity, and dispatch instant WhatsApp alerts.

---

## Key Highlights

- **Zero External Dependencies:** Built 100% on standard library modules (`urllib.request`, `sqlite3`, `dataclasses`, `threading`, etc.). Zero `pip install` required.
- **Ultra-Low Memory Footprint (`__slots__`):** Intermediate candle objects use `@dataclass(slots=True, frozen=True)` for minimal RAM overhead on Termux (Android) and low-memory VPS environments.
- **Dual SQLite Database Isolation:**
  - `futures_data.db`: Stores perpetual futures OHLCV candles (`futures_candles` table).
  - `options_data.db`: Stores complete options chain OHLCV candles (`options_candles` table).
  - Tuned with `PRAGMA journal_mode=WAL`, `synchronous=NORMAL`, `temp_store=MEMORY`, and a 64MB cache.
- **Dynamic 24-Hour Settlement Cycle:** Aligned with Delta Exchange daily settlement at **17:30:00 IST (12:00:00 UTC)**. Automatically detects closed cycles:
  - If current time is past 12:00 UTC: captures previous day 12:00 UTC to today 12:00 UTC.
  - If executed before 12:00 UTC: shifts window by -24 hours to target the latest finalized settlement.
- **Complete Options Chain Discovery:** Automatically scans live & expired contracts via `GET /v2/products`, tracks 328+ contracts across 5 expiries (including today's settled session), and captures historical candles.
- **Mathematical Integrity Auditor:** Verifies bar counts, monotonicity, and continuity, with an automated exponential backoff retry loop.
- **CallMeBot WhatsApp Alerts:** Dispatches execution and integrity metrics to your phone upon completion.

---

## Architectural Overview

```
                      +-----------------------------------+
                      |   Dynamic Settlement Generator    |
                      |  12:00 UTC (17:30 IST) Alignment  |
                      +-----------------+-----------------+
                                        |
                   [ start_time: -24h | end_time: latest 12:00 UTC ]
                                        |
                      +-----------------v-----------------+
                      |    Resilient Delta REST Client    |
                      |  (urllib.request + RateLimiter)   |
                      +--------+-----------------+--------+
                               |                 |
            +------------------v--+           +--v------------------+
            |  Futures Ingestion  |           |   Options Catalog   |
            |   3 Symbols / 6 TFs |           | 5 Expiries / 328+ C |
            +----------+----------+           +----------+----------+
                       |                                 |
            +----------v----------+           +----------v----------+
            |   futures_data.db   |           |   options_data.db   |
            |  (WAL / Idempotent) |           |  (WAL / Idempotent) |
            +----------+----------+           +----------+----------+
                       |                                 |
                       +----------------+----------------+
                                        |
                      +-----------------v-----------------+
                      |   Integrity Auditor & Retry Loop  |
                      |   (Bar Count, Gaps, Boundaries)   |
                      +-----------------+-----------------+
                                        |
                      +-----------------v-----------------+
                      |  WhatsApp Notifier & JSON Report  |
                      |  (CallMeBot + Structured Telemetry|
                      +-----------------------------------+
```

---

## Pipeline Components

| Module | Purpose |
| :--- | :--- |
| `models.py` | Memory-optimized data containers using `__slots__` (`Candle`, `OptionContract`, `SettlementWindow`). |
| `settlement.py` | Arithmetic calculations for 12:00 UTC / 17:30 IST settlement alignment. |
| `storage.py` | Thread-safe isolated SQLite engines (`futures_data.db` & `options_data.db`) in WAL mode. |
| `client.py` | REST client using pure `urllib.request`, 9.5 req/s rate limiter, and backoff retries. |
| `engine.py` | Concurrent ingestion engine for futures matrices and options contracts. |
| `auditor.py` | Bar-count validation, timestamp monotonicity verifier, and targeted retry runner. |
| `notifier.py` | Zero-dependency CallMeBot WhatsApp dispatcher with graceful skipping. |
| `main.py` | Unified CLI entrypoint with signal handling and JSON reporting. |

---

## Local Setup & Quickstart

### Prerequisites
- Python 3.10+ (Standard library only; no virtual environment or external packages needed).

### Clone & Run
```bash
git clone https://github.com/your-username/DeltaSync.git
cd DeltaSync

# Run unit test suite (16 tests, 0.2s runtime)
python -m unittest discover -s tests

# Execute end-to-end pipeline
python main.py
```

---

## Termux (Android Mobile) Setup

DeltaSync is designed to operate on Termux on Android with minimal battery and memory consumption:

```bash
# 1. Update and install Python & Git
pkg update -y && pkg install python git -y

# 2. Clone repository
git clone https://github.com/your-username/DeltaSync.git
cd DeltaSync

# 3. Verify tests
python -m unittest discover -s tests

# 4. Run pipeline
python main.py --quiet
```

### 24/7 Automated Scheduling (Crontab on Termux / Linux)
Delta Exchange settles daily at **17:30 IST**. Schedule the pipeline to run at **17:35 IST** (5 minutes after settlement):

```bash
# Edit crontab
crontab -e

# Add cron entry (17:35 IST = 12:05 UTC)
5 12 * * * cd /path/to/DeltaSync && python main.py >> deltasync.log 2>&1
```

---

## WhatsApp Notification Configuration

DeltaSync integrates with [CallMeBot](https://www.callmebot.com/) for instant WhatsApp reports.

### Getting a Free CallMeBot API Key:
1. Add the phone number `+34 644 59 13 89` (or the number listed on callmebot.com) to your WhatsApp contacts.
2. Send the message: `I allow callmebot to send me messages` to that contact.
3. You will receive an API key.

### Supplying Credentials:
**Method A: Environment Variables (Recommended for cron/systemd)**
```bash
export WHATSAPP_PHONE="+919876543210"
export WHATSAPP_APIKEY="123456"
python main.py
```

**Method B: Command-Line Flags**
```bash
python main.py --whatsapp-phone "+919876543210" --whatsapp-key "123456"
```

*Note: If credentials are not supplied, the pipeline logs a clean skip message and completes without errors.*

---

## CLI Reference & Flags

```text
usage: main.py [-h] [--futures-symbols FUTURES_SYMBOLS]
               [--futures-timeframes FUTURES_TIMEFRAMES]
               [--options-expiries-count OPTIONS_EXPIRIES_COUNT]
               [--options-resolution OPTIONS_RESOLUTION]
               [--futures-db FUTURES_DB] [--options-db OPTIONS_DB]
               [--base-url BASE_URL] [--max-rate MAX_RATE]
               [--max-retries MAX_RETRIES]
               [--max-retry-rounds MAX_RETRY_ROUNDS]
               [--max-workers MAX_WORKERS] [--quiet]
               [--whatsapp-phone WHATSAPP_PHONE] [--whatsapp-key WHATSAPP_KEY]
```

### Examples:
- **Custom Futures & Resoluton:**
  ```bash
  python main.py --futures-symbols BTCUSDT,ETHUSDT --futures-timeframes 5m,15m,1h
  ```
- **Track 7 Option Expiries with 15m resolution:**
  ```bash
  python main.py --options-expiries-count 7 --options-resolution 15m
  ```

---

## Verification JSON Output Example

```json
{
  "status": "SUCCESS",
  "timestamp_utc": "2026-11-01T13:47:15.549247+00:00",
  "elapsed_seconds": 43.68,
  "settlement_window": {
    "start_time": 1793448000,
    "end_time": 1793534400,
    "duration_seconds": 86400,
    "start_utc": "2026-10-31 12:00:00 UTC",
    "end_utc": "2026-11-01 12:00:00 UTC",
    "start_ist": "2026-10-31 17:30:00 IST",
    "end_ist": "2026-11-01 17:30:00 IST"
  },
  "databases": {
    "futures_db": {
      "file": "futures_data.db",
      "table": "futures_candles",
      "total_candles": 5724,
      "series_passed": 18,
      "series_total": 18
    },
    "options_db": {
      "file": "options_data.db",
      "table": "options_candles",
      "expiries_tracked": [
        "2026-11-01T12:00:00Z",
        "2026-11-02T12:00:00Z",
        "2026-11-03T12:00:00Z",
        "2026-11-04T12:00:00Z",
        "2026-11-06T12:00:00Z"
      ],
      "total_contracts_scanned": 328,
      "contracts_with_activity": 185,
      "total_candles": 19417
    }
  },
  "futures_integrity": {
    "overall_status": "PASSED"
  },
  "network_telemetry": {
    "total_requests": 347,
    "total_retries": 0,
    "total_failures": 0
  },
  "whatsapp_notification": {
    "sent": true,
    "status_code": 200
  }
}
```

---

## License

MIT License. Free for personal, commercial, and research use.
