# TASK: Delta Exchange 24-Hour Settlement ETL & Validation Pipeline

## 1. Project Overview & Constraints
Build a zero-third-party-dependency Python 3 pipeline to extract 24-hour settlement-aligned market data from Delta Exchange (India), store records idempotently across two SQLite databases, and validate data integrity with an automated retry loop.

### Strict Architectural Invariants:
1. **Zero External Dependencies:** Only standard library modules are permitted (`urllib.request`, `urllib.parse`, `urllib.error`, `sqlite3`, `json`, `time`, `datetime`, `timezone`, `sys`, `os`, `signal`, `math`, `typing`, `dataclasses`, `logging`, `argparse`, `concurrent.futures`, `threading`). No `requests`, `pandas`, or third-party packages.
2. **Memory Optimization:** Implement `__slots__` on intermediate data containers.
3. **Database Separation:**
   - `futures_data.db`: Stores perpetual futures OHLCV candles (`futures_candles` table).
   - `options_data.db`: Stores complete options chain OHLCV candles (`options_candles` table).
4. **Target Execution Environment:** Must run on Termux (Android) and standard Linux/Windows environments with minimal memory footprint.

---

## 2. Settlement Time Window Arithmetic
Delta Exchange daily options settle at **17:30:00 IST (12:00:00 UTC)**.
The script must dynamically calculate the latest closed 24-hour cycle:
- `end_time`: Most recent 12:00:00 UTC timestamp. If executed before 12:00 UTC, shift `end_time` by -24 hours.
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