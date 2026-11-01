"""Unit test suite for DeltaSync ETL & Validation Pipeline.

Uses Python standard library unittest to guarantee zero external dependency requirement.
"""
from __future__ import annotations

from datetime import datetime, timezone
import os
import sqlite3
import tempfile
import unittest

from auditor import compute_expected_bars, IntegrityAuditor
from client import DeltaClient, RateLimiter
from models import Candle, OptionContract, SettlementWindow
from settlement import get_settlement_window
from storage import FuturesStorage, OptionsStorage


class TestModels(unittest.TestCase):
    """Test data container models and memory optimization invariants."""

    def test_candle_slots_enforcement(self):
        c = Candle(
            symbol="BTCUSDT",
            resolution="5m",
            time=1700000000,
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0,
            volume=12.5,
        )
        self.assertEqual(c.symbol, "BTCUSDT")
        self.assertEqual(c.time, 1700000000)
        # Attempting to assign or mutate must raise AttributeError or TypeError (slots + frozen)
        with self.assertRaises((AttributeError, TypeError)):
            c.symbol = "ETHUSDT"  # type: ignore
        with self.assertRaises((AttributeError, TypeError)):
            c.extra_field = "invalid"  # type: ignore

    def test_candle_to_tuple(self):
        c = Candle("ETHUSDT", "15m", 1700000000, 3000.0, 3050.0, 2990.0, 3020.0, 100.0)
        t = c.to_tuple()
        self.assertEqual(t, ("ETHUSDT", "15m", 1700000000, 3000.0, 3050.0, 2990.0, 3020.0, 100.0))


class TestSettlementWindow(unittest.TestCase):
    """Test 24-hour settlement window arithmetic."""

    def test_settlement_before_12_utc(self):
        # 2026-11-01 08:30:00 UTC (before 12:00 UTC)
        ref_dt = datetime(2026, 11, 1, 8, 30, 0, tzinfo=timezone.utc)
        win = get_settlement_window(ref_dt)

        # Expected end_time is 2026-10-31 12:00:00 UTC
        expected_end = int(datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc).timestamp())
        expected_start = expected_end - 86400

        self.assertEqual(win.end_time, expected_end)
        self.assertEqual(win.start_time, expected_start)
        self.assertEqual(win.duration_seconds, 86400)
        self.assertIn("2026-10-31 12:00:00 UTC", win.end_utc)
        self.assertIn("17:30:00 IST", win.end_ist)

    def test_settlement_after_12_utc(self):
        # 2026-11-01 14:00:00 UTC (after 12:00 UTC)
        ref_dt = datetime(2026, 11, 1, 14, 0, 0, tzinfo=timezone.utc)
        win = get_settlement_window(ref_dt)

        # Expected end_time is 2026-11-01 12:00:00 UTC
        expected_end = int(datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
        expected_start = expected_end - 86400

        self.assertEqual(win.end_time, expected_end)
        self.assertEqual(win.start_time, expected_start)
        self.assertEqual(win.duration_seconds, 86400)
        self.assertIn("2026-11-01 12:00:00 UTC", win.end_utc)
        self.assertIn("17:30:00 IST", win.end_ist)


class TestStorageEngine(unittest.TestCase):
    """Test SQLite storage engine, WAL pragmas, and idempotent batch ingestion."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.futures_db = os.path.join(self.temp_dir.name, "futures_data.db")
        self.options_db = os.path.join(self.temp_dir.name, "options_data.db")
        self.f_store = FuturesStorage(db_path=self.futures_db)
        self.o_store = OptionsStorage(db_path=self.options_db)

    def tearDown(self):
        self.f_store.close()
        self.o_store.close()
        self.temp_dir.cleanup()

    def test_pragmas_and_schemas(self):
        # Verify WAL mode in futures_db
        conn = sqlite3.connect(self.futures_db)
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        self.assertEqual(mode.lower(), "wal")

        # Check futures table schema
        schema = conn.execute("SELECT sql FROM sqlite_master WHERE name='futures_candles';").fetchone()[0]
        self.assertIn("PRIMARY KEY (symbol, resolution, time)", schema)
        conn.close()

        # Check options table schema
        conn_opt = sqlite3.connect(self.options_db)
        opt_schema = conn_opt.execute("SELECT sql FROM sqlite_master WHERE name='options_candles';").fetchone()[0]
        self.assertIn("PRIMARY KEY (symbol, resolution, time)", opt_schema)
        conn_opt.close()

    def test_idempotent_batch_insertion(self):
        candles = [
            Candle("BTCUSDT", "5m", 1700000000, 50000.0, 50100.0, 49900.0, 50050.0, 10.0),
            Candle("BTCUSDT", "5m", 1700000300, 50050.0, 50150.0, 50000.0, 50120.0, 15.0),
        ]
        inserted = self.f_store.save_candles(candles)
        self.assertEqual(inserted, 2)
        self.assertEqual(self.f_store.get_candle_count(), 2)

        # Re-inserting exact duplicates must be ignored (idempotent)
        inserted_dup = self.f_store.save_candles(candles)
        self.assertEqual(inserted_dup, 0)
        self.assertEqual(self.f_store.get_candle_count(), 2)

    def test_query_filtering(self):
        candles = [
            Candle("BTCUSDT", "5m", 1000, 1.0, 1.0, 1.0, 1.0, 1.0),
            Candle("BTCUSDT", "5m", 2000, 2.0, 2.0, 2.0, 2.0, 2.0),
            Candle("BTCUSDT", "15m", 1500, 3.0, 3.0, 3.0, 3.0, 3.0),
        ]
        self.f_store.save_candles(candles)

        res_5m = self.f_store.get_candles("BTCUSDT", "5m", start_time=1000, end_time=1500)
        self.assertEqual(len(res_5m), 1)
        self.assertEqual(res_5m[0].time, 1000)


class TestAuditor(unittest.TestCase):
    """Test integrity calculation logic and gap detection."""

    def test_compute_expected_bars(self):
        self.assertEqual(compute_expected_bars("1m", 86400), (1440, 1441))
        self.assertEqual(compute_expected_bars("5m", 86400), (288, 289))
        self.assertEqual(compute_expected_bars("15m", 86400), (96, 97))
        self.assertEqual(compute_expected_bars("30m", 86400), (48, 49))
        self.assertEqual(compute_expected_bars("1h", 86400), (24, 25))
        self.assertEqual(compute_expected_bars("4h", 86400), (6, 7))
        self.assertEqual(compute_expected_bars("1d", 86400), (1, 1))


if __name__ == "__main__":
    unittest.main()
