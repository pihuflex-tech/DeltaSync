"""Ingestion Engine orchestrating data collection for Futures and Options across Delta Exchange.

Fetches 3 futures symbols across 6 timeframes and complete options chain across 5 expiries
(including today's settled session via states=live,expired).
Writes idempotently to isolated databases (futures_data.db and options_data.db).
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import logging
from typing import Callable, Optional, Sequence

from client import DeltaClient, TIMEFRAME_TO_DELTA_CODE
from models import Candle, OptionContract, SettlementWindow
from storage import FuturesStorage, OptionsStorage

logger = logging.getLogger("DeltaSync.Engine")

DEFAULT_FUTURES_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
DEFAULT_FUTURES_TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "4h")
DEFAULT_OPTIONS_RESOLUTION = "5m"


class IngestionEngine:
    """ETL orchestrator for Delta Exchange futures and options historical data."""

    def __init__(
        self,
        client: Optional[DeltaClient] = None,
        futures_storage: Optional[FuturesStorage] = None,
        options_storage: Optional[OptionsStorage] = None,
        max_workers: int = 4,
    ):
        self.client = client or DeltaClient()
        self.futures_storage = futures_storage or FuturesStorage()
        self.options_storage = options_storage or OptionsStorage()
        self.max_workers = max_workers

    def fetch_futures_series(
        self,
        symbol: str,
        timeframe: str,
        window: SettlementWindow,
    ) -> list[Candle]:
        """Fetch and store candles for a single futures symbol and timeframe."""
        logger.info("Fetching futures %s [%s]...", symbol, timeframe)
        candles = self.client.fetch_candles(
            symbol=symbol,
            resolution=timeframe,
            start_time=window.start_time,
            end_time=window.end_time,
        )
        if candles:
            inserted = self.futures_storage.save_candles(candles)
            logger.info("Saved %d/%d candles for %s [%s]", inserted, len(candles), symbol, timeframe)
        else:
            logger.warning("No candles returned for %s [%s]", symbol, timeframe)
        return candles

    def run_futures_ingestion(
        self,
        window: SettlementWindow,
        symbols: Sequence[str] = DEFAULT_FUTURES_SYMBOLS,
        timeframes: Sequence[str] = DEFAULT_FUTURES_TIMEFRAMES,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
    ) -> dict[str, Any]:
        """Ingest all configured futures symbols across the specified timeframes.

        Returns:
            Dictionary with ingested counts per symbol and timeframe.
        """
        logger.info("Starting futures ingestion for %s across %s", symbols, timeframes)
        total_tasks = len(symbols) * len(timeframes)
        completed_tasks = 0
        results: dict[str, dict[str, int]] = {}

        tasks = []
        for symbol in symbols:
            results[symbol] = {}
            for tf in timeframes:
                tasks.append((symbol, tf))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(self.fetch_futures_series, symbol, tf, window): (symbol, tf)
                for symbol, tf in tasks
            }

            for future in as_completed(future_to_task):
                symbol, tf = future_to_task[future]
                completed_tasks += 1
                try:
                    candles = future.result()
                    results[symbol][tf] = len(candles)
                except Exception as exc:
                    logger.error("Error fetching %s [%s]: %s", symbol, tf, exc)
                    results[symbol][tf] = 0

                if progress_cb:
                    progress_cb(f"Futures: {symbol} [{tf}]", completed_tasks, total_tasks)

        return results

    def discover_target_options_expiries(
        self,
        window: SettlementWindow,
        num_expiries: int = 5,
    ) -> tuple[list[str], list[OptionContract]]:
        """Discover 5 expiries anchored at today's settled session plus upcoming expiries.

        Returns:
            Tuple of (list of 5 selected settlement_time strings, list of all contracts in those expiries).
        """
        logger.info("Discovering options catalog via /v2/products (states=live,expired)...")
        all_contracts = self.client.fetch_option_products(states="live,expired")
        logger.info("Retrieved %d total option contracts from catalog", len(all_contracts))

        # Group contracts by settlement_time
        grouped: dict[str, list[OptionContract]] = {}
        for c in all_contracts:
            grouped.setdefault(c.settlement_time, []).append(c)

        # Parse settlement times into datetimes for precise chronological anchoring
        def parse_iso(dt_str: str) -> datetime:
            # Handle ISO string like '2026-11-01T12:00:00Z'
            clean = dt_str.replace("Z", "+00:00")
            return datetime.fromisoformat(clean)

        settlement_times = sorted(grouped.keys(), key=parse_iso)

        # Target today's settled session anchor timestamp
        target_settlement_iso = datetime.fromtimestamp(window.end_time, tz=timezone.utc).strftime("%Y-%m-%dT12:00:00Z")

        # Find the index of today's settled session
        anchor_idx = -1
        for idx, st in enumerate(settlement_times):
            if st == target_settlement_iso or parse_iso(st).timestamp() == window.end_time:
                anchor_idx = idx
                break

        if anchor_idx != -1:
            # Select today's settled session and up to 4 forward expiries
            selected_expiries = settlement_times[anchor_idx : anchor_idx + num_expiries]
            # If not enough forward expiries, include prior closed expiries to ensure exactly num_expiries
            if len(selected_expiries) < num_expiries and anchor_idx > 0:
                deficit = num_expiries - len(selected_expiries)
                start_slice = max(0, anchor_idx - deficit)
                selected_expiries = settlement_times[start_slice : start_slice + num_expiries]
        else:
            # Fallback: find nearest expiry to window.end_time
            logger.warning(
                "Exact anchor %s not matched in expiries. Anchoring to closest settlement date.",
                target_settlement_iso,
            )
            anchor_idx = min(
                range(len(settlement_times)),
                key=lambda i: abs(parse_iso(settlement_times[i]).timestamp() - window.end_time),
            )
            selected_expiries = settlement_times[anchor_idx : anchor_idx + num_expiries]

        selected_contracts: list[OptionContract] = []
        for exp in selected_expiries:
            selected_contracts.extend(grouped[exp])

        logger.info(
            "Selected %d expiries (%s) containing %d contracts",
            len(selected_expiries),
            selected_expiries,
            len(selected_contracts),
        )
        return selected_expiries, selected_contracts

    def fetch_option_candle_series(
        self,
        symbol: str,
        resolution: str,
        window: SettlementWindow,
    ) -> list[Candle]:
        """Fetch and store candles for a single option contract."""
        candles = self.client.fetch_candles(
            symbol=symbol,
            resolution=resolution,
            start_time=window.start_time,
            end_time=window.end_time,
        )
        if candles:
            self.options_storage.save_candles(candles)
        return candles

    def run_options_ingestion(
        self,
        window: SettlementWindow,
        contracts: Sequence[OptionContract],
        resolution: str = DEFAULT_OPTIONS_RESOLUTION,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
    ) -> dict[str, Any]:
        """Ingest complete options chain across selected contracts for the settlement window.

        Returns:
            Dictionary with options ingestion summary.
        """
        logger.info(
            "Starting options ingestion for %d contracts at resolution %s...",
            len(contracts),
            resolution,
        )
        total_contracts = len(contracts)
        completed_contracts = 0
        contracts_with_bars = 0
        total_bars_saved = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_sym = {
                executor.submit(
                    self.fetch_option_candle_series,
                    c.symbol,
                    resolution,
                    window,
                ): c.symbol
                for c in contracts
            }

            for future in as_completed(future_to_sym):
                symbol = future_to_sym[future]
                completed_contracts += 1
                try:
                    candles = future.result()
                    if candles:
                        contracts_with_bars += 1
                        total_bars_saved += len(candles)
                except Exception as exc:
                    logger.warning("Error fetching option %s: %s", symbol, exc)

                if progress_cb:
                    progress_cb(f"Options: {symbol}", completed_contracts, total_contracts)

        return {
            "total_contracts_scanned": total_contracts,
            "contracts_with_activity": contracts_with_bars,
            "total_bars_saved": total_bars_saved,
            "resolution": resolution,
        }

