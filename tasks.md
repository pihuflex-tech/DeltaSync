# TASK: Delta Exchange 24-Hour Settlement ETL & Validation Pipeline

## 1. Project Overview & Constraints
Build a zero-third-party-dependency Python 3 pipeline to extract 24-hour settlement-aligned market data from Delta Exchange (India), store records idempotently across two isolated SQLite databases, and validate data integrity with an automated retry loop.

### Strict Architectural Invariants:
1. **Zero External Dependencies:** Only standard library modules are permitted (`urllib.request`, `urllib.parse`, `urllib.error`, `sqlite3`, `json`, `time`, `datetime`, `timezone`, `sys`, `os`, `signal`, `math`, `typing`, `dataclasses`, `logging`, `argparse`, `concurrent.futures`, `threading`). No `requests`, `pandas`, or third-party packages.
2. **Memory Optimization:** Implement `__slots__` on intermediate data containers (`@dataclass(slots=True, frozen=True)`).
3. **Dual Database Isolation:**
   - `futures_data.db`: Stores perpetual futures OHLCV candles (`futures_candles` table).
   - `options_data.db`: Stores complete options chain OHLCV candles (`options_candles` table).
   - Both databases operate completely isolated with WAL mode and memory-optimized pragmas.
4. **Target Execution Environment:** Must run seamlessly on Termux (Android), standard Linux, and Windows environments with minimal memory footprint.

---

## 2. Settlement Time Window Arithmetic
Delta Exchange daily options settle at **17:30:00 IST (12:00:00 UTC)**.
The script must dynamically calculate the latest closed 24-hour cycle:
- `end_time`: Most recent 12:00:00 UTC timestamp. If executed before 12:00:00 UTC on the current day, shift `end_time` by -24 hours.
- `start_time`: Exactly `end_time - 86400` seconds (12:00:00 UTC previous day).
- Both timestamps must be integer Unix epoch seconds.

---

## 3. Database Schema Specifications

### `futures_data.db`
```sql
CREATE TABLE IF NOT EXISTS futures_candles (
    symbol TEXT NOT NULL,
    resolution TEXT NOT NULL,
    time INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    PRIMARY KEY (symbol, resolution, time)
);
CREATE INDEX IF NOT EXISTS idx_futures_lookup 
ON futures_candles (symbol, resolution, time DESC);
```

### `options_data.db`
```sql
CREATE TABLE IF NOT EXISTS options_candles (
    symbol TEXT NOT NULL,
    resolution TEXT NOT NULL,
    time INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    PRIMARY KEY (symbol, resolution, time)
);
CREATE INDEX IF NOT EXISTS idx_options_lookup 
ON options_candles (symbol, resolution, time DESC);
```

---

## 4. Phase-by-Phase Execution Plan

### Phase 1: Models, Storage & Dynamic Settlement Window
- **Task 1.1:** Canonical Candle model with `__slots__` (`symbol`, `resolution`, `time`, `open`, `high`, `low`, `close`, `volume`).
- **Task 1.2:** Dynamic Settlement Window generator computing `start_time` and `end_time` strictly aligned with 12:00:00 UTC / 17:30:00 IST.
- **Task 1.3:** Atomic SQLite database managers for `futures_data.db` and `options_data.db` with PRAGMAs: `journal_mode=WAL;`, `synchronous=NORMAL;`, `temp_store=MEMORY;`, `cache_size=-64000;`.
- **Task 1.4:** Idempotent batch insertion using `INSERT OR IGNORE`.

### Phase 2: Resilient REST Ingestion Engine
- **Task 2.1:** Thread-safe rate limiter pacing requests within the 10 req/s limit.
- **Task 2.2:** REST client using pure `urllib.request` with User-Agent header, timeout handling, and exponential backoff on HTTP 429 / 5xx.
- **Task 2.3:** Futures Ingestion: 3 perpetual symbols (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`) across 6 timeframes (`1m`, `5m`, `15m`, `30m`, `1h`, `4h`).
- **Task 2.4:** Options Ingestion: Query `GET /v2/products?contract_types=call_options,put_options&states=live,expired`, discover 5 expiries starting from today's settled session (`YYYY-MM-DDT12:00:00Z`) plus 4 upcoming forward expiries, and fetch OHLCV data for all contracts in the window.

### Phase 3: Integrity Auditor & Retry Loop
- **Task 3.1:** Bar-count verification: Verify expected bar count for each timeframe over the 86,400-second window.
- **Task 3.2:** Gap detection and monotonic timestamp ordering verification.
- **Task 3.3:** Targeted retry loop for any missing ranges or temporary network errors.
- **Task 3.4:** Options integrity report: total contracts scanned, contracts with market activity, total bars stored.

### Phase 4: CLI Interface & Structured Reporting
- **Task 4.1:** Clean CLI entrypoint (`main.py`) with configurable arguments and graceful `SIGINT`/`Ctrl+C` handling.
- **Task 4.2:** Real-time terminal logging of execution progress and batch commits.
- **Task 4.3:** Output final structured JSON verification summary to stdout.

