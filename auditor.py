"""Integrity Auditor and Automated Retry Loop for DeltaSync pipeline.

Audits bar counts against theoretical expectations over the 86,400s settlement window,
detects gaps, verifies chronological ordering and time boundaries, tracks network errors,
and executes targeted retries for any incomplete series.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional, Sequence

from client import DeltaClient, TIMEFRAME_SECONDS
from models import Candle, SettlementWindow
from storage import FuturesStorage, OptionsStorage

logger = logging.getLogger("DeltaSync.Auditor")


def compute_expected_bars(resolution: str, window_seconds: int = 86400) -> tuple[int, int]:
    """Compute min and max expected bar counts for a resolution over a window.

    Delta Exchange /v2/chart/history includes both endpoints inclusive,
    so for 86400s at 5m: 86400 / 300 = 288 intervals, yielding 288 to 289 bars.
    For 1d: exactly 1 bar.
    """
    interval = TIMEFRAME_SECONDS.get(resolution)
    if not interval:
        return 0, 0

    if resolution in ("1d", "D"):
        return 1, 1

    base_count = window_seconds // interval
    return base_count, base_count + 1


class IntegrityAuditor:
    """Verifies data completeness, gap-free continuity, and runs automated retries."""

    def __init__(
        self,
        futures_storage: FuturesStorage,
        options_storage: OptionsStorage,
        client: DeltaClient,
    ):
        self.futures_storage = futures_storage
        self.options_storage = options_storage
        self.client = client

    def audit_futures_series(
        self,
        symbol: str,
        resolution: str,
        window: SettlementWindow,
    ) -> dict[str, Any]:
        """Audit a single futures series for count, boundaries, and continuity."""
        # Query candles within the settlement window
        candles = self.futures_storage.get_candles(
            symbol=symbol,
            resolution=resolution,
            start_time=window.start_time,
            end_time=window.end_time,
        )

        min_expected, max_expected = compute_expected_bars(resolution, window.duration_seconds)
        actual_count = len(candles)
        status = "PASSED"
        issues = []

        if actual_count < min_expected:
            status = "INCOMPLETE"
            issues.append(f"Bar count {actual_count} below expected range [{min_expected}, {max_expected}]")

        # Verify time bounds & monotonicity & gaps
        interval = TIMEFRAME_SECONDS.get(resolution, 60)
        gaps = []
        for i in range(len(candles)):
            ts = candles[i].time
            if ts < window.start_time or ts > window.end_time:
                status = "OUT_OF_BOUNDS"
                issues.append(f"Candle timestamp {ts} out of window bounds [{window.start_time}, {window.end_time}]")
                break

            if i > 0:
                prev_ts = candles[i - 1].time
                diff = ts - prev_ts
                if diff <= 0:
                    status = "NON_MONOTONIC"
                    issues.append(f"Duplicate or decreasing timestamp at {ts}")
                elif resolution not in ("1d", "D") and diff > interval:
                    gaps.append((prev_ts, ts, diff))

        if gaps:
            status = "GAPS_DETECTED"
            issues.append(f"Found {len(gaps)} internal timestamp gaps")

        first_ts = candles[0].time if candles else None
        last_ts = candles[-1].time if candles else None

        return {
            "symbol": symbol,
            "resolution": resolution,
            "status": status,
            "actual_bars": actual_count,
            "expected_range": [min_expected, max_expected],
            "first_timestamp": first_ts,
            "last_timestamp": last_ts,
            "gaps_count": len(gaps),
            "issues": issues,
        }

    def audit_all_futures(
        self,
        symbols: Sequence[str],
        timeframes: Sequence[str],
        window: SettlementWindow,
    ) -> dict[str, Any]:
        """Audit all futures series."""
        series_results = []
        total_passed = 0
        total_checked = 0
        total_bars = 0

        for sym in symbols:
            for tf in timeframes:
                res = self.audit_futures_series(sym, tf, window)
                series_results.append(res)
                total_checked += 1
                total_bars += res["actual_bars"]
                if res["status"] == "PASSED":
                    total_passed += 1

        all_passed = (total_passed == total_checked)
        return {
            "overall_status": "PASSED" if all_passed else "FAILED",
            "passed_series": total_passed,
            "total_series": total_checked,
            "total_bars": total_bars,
            "series": series_results,
        }

    def retry_incomplete_futures(
        self,
        audit_report: dict[str, Any],
        window: SettlementWindow,
        max_retry_rounds: int = 3,
    ) -> dict[str, Any]:
        """Targeted automated retry loop for incomplete or failed futures series."""
        current_report = audit_report
        for round_num in range(1, max_retry_rounds + 1):
            incomplete = [
                s for s in current_report.get("series", [])
                if s["status"] != "PASSED"
            ]
            if not incomplete:
                logger.info("All futures series passed integrity checks. No retry needed.")
                break

            logger.warning(
                "Integrity audit retry round %d/%d for %d incomplete series...",
                round_num,
                max_retry_rounds,
                len(incomplete),
            )

            for item in incomplete:
                sym = item["symbol"]
                tf = item["resolution"]
                logger.info("Retrying fetch for %s [%s]...", sym, tf)
                try:
                    candles = self.client.fetch_candles(
                        symbol=sym,
                        resolution=tf,
                        start_time=window.start_time,
                        end_time=window.end_time,
                    )
                    if candles:
                        self.futures_storage.save_candles(candles)
                except Exception as exc:
                    logger.error("Retry failed for %s [%s]: %s", sym, tf, exc)

            time.sleep(1.0)
            symbols = list({s["symbol"] for s in incomplete})
            timeframes = list({s["resolution"] for s in incomplete})
            current_report = self.audit_all_futures(symbols, timeframes, window)

        return current_report

    def audit_options(self, window: SettlementWindow) -> dict[str, Any]:
        """Audit stored options data."""
        total_bars = self.options_storage.get_candle_count(
            start_time=window.start_time,
            end_time=window.end_time,
        )
        symbols = self.options_storage.get_distinct_symbols()

        return {
            "status": "PASSED" if total_bars > 0 or len(symbols) == 0 else "NO_DATA",
            "unique_option_symbols_stored": len(symbols),
            "total_option_bars": total_bars,
        }
