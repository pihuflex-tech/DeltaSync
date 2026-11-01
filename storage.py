"""Dual SQLite database storage engine with WAL mode and atomic isolation.

Enforces strict separation between futures_data.db and options_data.db.
Applies performance pragmas (WAL mode, NORMAL synchronous, MEMORY temp_store,
and 64MB cache) and idempotent batch operations with INSERT OR IGNORE.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from typing import Optional, Sequence

from models import Candle


class BaseCandleDatabase:
    """Thread-safe SQLite storage engine for OHLCV candlestick data."""

    def __init__(self, db_path: str, table_name: str, index_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self.index_name = index_name
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Establish connection and apply performance pragmas."""
        if self._conn is None:
            # Ensure parent directory exists
            parent_dir = os.path.dirname(self.db_path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0,
            )
            conn.row_factory = sqlite3.Row
            # Enforce high-performance, crash-safe pragmas
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-64000;")
            self._conn = conn
        return self._conn

    def _init_db(self) -> None:
        """Create tables and composite indexes idempotently."""
        with self._lock:
            conn = self._get_connection()
            with conn:
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
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
                    """
                )
                conn.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {self.index_name} 
                    ON {self.table_name} (symbol, resolution, time DESC);
                    """
                )

    def save_candles(self, candles: Sequence[Candle], batch_size: int = 5000) -> int:
        """Insert a batch of candles idempotently using INSERT OR IGNORE.

        Args:
            candles: Sequence of Candle instances.
            batch_size: Chunk size for executemany commits.

        Returns:
            Number of newly inserted rows.
        """
        if not candles:
            return 0

        rows = [c.to_tuple() for c in candles]
        sql = f"""
            INSERT OR IGNORE INTO {self.table_name} 
            (symbol, resolution, time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        total_inserted = 0
        with self._lock:
            conn = self._get_connection()
            for i in range(0, len(rows), batch_size):
                chunk = rows[i : i + batch_size]
                initial_changes = conn.total_changes
                with conn:
                    conn.executemany(sql, chunk)
                total_inserted += (conn.total_changes - initial_changes)

        return total_inserted

    def get_candles(
        self,
        symbol: str,
        resolution: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> list[Candle]:
        """Fetch candles for a symbol and resolution within a time window ordered by time ASC."""
        query = f"SELECT symbol, resolution, time, open, high, low, close, volume FROM {self.table_name} WHERE symbol = ? AND resolution = ?"
        params: list[object] = [symbol, resolution]

        if start_time is not None:
            query += " AND time >= ?"
            params.append(start_time)
        if end_time is not None:
            query += " AND time <= ?"
            params.append(end_time)

        query += " ORDER BY time ASC"

        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(query, params)
            results = []
            for row in cursor.fetchall():
                results.append(
                    Candle(
                        symbol=row["symbol"],
                        resolution=row["resolution"],
                        time=int(row["time"]),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    )
                )
            return results

    def get_candle_count(
        self,
        symbol: Optional[str] = None,
        resolution: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> int:
        """Count candles matching optional filters."""
        query = f"SELECT COUNT(*) AS cnt FROM {self.table_name} WHERE 1=1"
        params: list[object] = []

        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if resolution:
            query += " AND resolution = ?"
            params.append(resolution)
        if start_time is not None:
            query += " AND time >= ?"
            params.append(start_time)
        if end_time is not None:
            query += " AND time <= ?"
            params.append(end_time)

        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            return int(row["cnt"]) if row else 0

    def get_distinct_symbols(self) -> list[str]:
        """Return list of distinct symbols present in the database."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(f"SELECT DISTINCT symbol FROM {self.table_name} ORDER BY symbol ASC")
            return [row["symbol"] for row in cursor.fetchall()]

    def get_distinct_resolutions(self, symbol: Optional[str] = None) -> list[str]:
        """Return list of distinct resolutions for a symbol or across all symbols."""
        with self._lock:
            conn = self._get_connection()
            if symbol:
                cursor = conn.execute(
                    f"SELECT DISTINCT resolution FROM {self.table_name} WHERE symbol = ? ORDER BY resolution ASC",
                    (symbol,),
                )
            else:
                cursor = conn.execute(f"SELECT DISTINCT resolution FROM {self.table_name} ORDER BY resolution ASC")
            return [row["resolution"] for row in cursor.fetchall()]

    def get_time_bounds(self, symbol: str, resolution: str) -> tuple[Optional[int], Optional[int]]:
        """Return min and max timestamps for given symbol and resolution."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(
                f"SELECT MIN(time) as min_ts, MAX(time) as max_ts FROM {self.table_name} WHERE symbol = ? AND resolution = ?",
                (symbol, resolution),
            )
            row = cursor.fetchone()
            if row and row["min_ts"] is not None:
                return int(row["min_ts"]), int(row["max_ts"])
            return None, None

    def close(self) -> None:
        """Close database connection cleanly."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None


class FuturesStorage(BaseCandleDatabase):
    """Storage manager specifically isolated for perpetual futures candles (futures_data.db)."""

    def __init__(self, db_path: str = "futures_data.db"):
        super().__init__(
            db_path=db_path,
            table_name="futures_candles",
            index_name="idx_futures_lookup",
        )


class OptionsStorage(BaseCandleDatabase):
    """Storage manager specifically isolated for options chain candles (options_data.db)."""

    def __init__(self, db_path: str = "options_data.db"):
        super().__init__(
            db_path=db_path,
            table_name="options_candles",
            index_name="idx_options_lookup",
        )

