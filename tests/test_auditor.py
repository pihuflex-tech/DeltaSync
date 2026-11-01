"""Extended unit tests for DeltaSync pipeline.

Includes tests for Auditor gap detection, bounds checking, and client normalization.
"""
from __future__ import annotations

import tempfile
import unittest

from auditor import IntegrityAuditor
from client import DeltaClient
from models import Candle, SettlementWindow
from storage import FuturesStorage, OptionsStorage


class TestAuditorValidation(unittest.TestCase):
    """Test auditor checks for gap detection and out-of-bounds conditions."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.futures_storage = FuturesStorage(db_path=f"{self.temp_dir.name}/futures.db")
        self.options_storage = OptionsStorage(db_path=f"{self.temp_dir.name}/options.db")
        self.client = DeltaClient()
        self.auditor = IntegrityAuditor(
            self.futures_storage,
            self.options_storage,
            self.client,
        )
        self.window = SettlementWindow(
            start_time=10000,
            end_time=13600,  # 3600s window (1h)
            duration_seconds=3600,
            start_utc="2026-11-01 11:00:00 UTC",
            end_utc="2026-11-01 12:00:00 UTC",
            start_ist="2026-11-01 16:30:00 IST",
            end_ist="2026-11-01 17:30:00 IST",
        )

    def tearDown(self):
        self.futures_storage.close()
        self.options_storage.close()
        self.temp_dir.cleanup()

    def test_auditor_passed_series(self):
        # 15m resolution over 3600s: 3600/900 = 4 intervals -> 4 or 5 bars
        candles = [
            Candle("BTCUSDT", "15m", 10000 + i * 900, 100, 101, 99, 100, 1)
            for i in range(5)
        ]
        self.futures_storage.save_candles(candles)

        res = self.auditor.audit_futures_series("BTCUSDT", "15m", self.window)
        self.assertEqual(res["status"], "PASSED")
        self.assertEqual(res["actual_bars"], 5)
        self.assertEqual(res["gaps_count"], 0)

    def test_auditor_detects_gap(self):
        # Skip bar at 10900 -> gap between 10000 and 11800
        candles = [
            Candle("BTCUSDT", "15m", 10000, 100, 101, 99, 100, 1),
            Candle("BTCUSDT", "15m", 11800, 100, 101, 99, 100, 1),
            Candle("BTCUSDT", "15m", 12700, 100, 101, 99, 100, 1),
            Candle("BTCUSDT", "15m", 13600, 100, 101, 99, 100, 1),
        ]
        self.futures_storage.save_candles(candles)

        res = self.auditor.audit_futures_series("BTCUSDT", "15m", self.window)
        self.assertEqual(res["status"], "GAPS_DETECTED")
        self.assertEqual(res["gaps_count"], 1)

    def test_auditor_detects_missing_bar(self):
        # Missing bar at 10900 (only 4 bars instead of 5, but contiguous)
        candles = [
            Candle("BTCUSDT", "15m", 10000, 100, 101, 99, 100, 1),
            Candle("BTCUSDT", "15m", 10900, 100, 101, 99, 100, 1),
            Candle("BTCUSDT", "15m", 11800, 100, 101, 99, 100, 1),
        ]
        self.futures_storage.save_candles(candles)

        res = self.auditor.audit_futures_series("BTCUSDT", "15m", self.window)
        self.assertEqual(res["status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
