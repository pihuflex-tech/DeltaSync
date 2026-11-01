"""Canonical models for DeltaSync ETL pipeline.

Implements memory-optimized data containers using __slots__ to strictly satisfy
architectural invariants and minimal memory footprint constraints.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True, frozen=True)
class Candle:
    """Canonical representation of an OHLCV candlestick bar."""
    symbol: str
    resolution: str
    time: int  # Unix epoch seconds
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_tuple(self) -> tuple[str, str, int, float, float, float, float, float]:
        """Convert candle to tuple format for SQLite executemany."""
        return (
            self.symbol,
            self.resolution,
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        )


@dataclass(slots=True, frozen=True)
class OptionContract:
    """Representation of an option contract from Delta Exchange product catalog."""
    symbol: str
    underlying_asset: str
    contract_type: str  # 'call_options' or 'put_options'
    strike_price: float
    settlement_time: str  # ISO-8601 string, e.g. '2026-11-01T12:00:00Z'
    state: str  # 'live' or 'expired'


@dataclass(slots=True, frozen=True)
class SettlementWindow:
    """Dynamic 24-hour settlement window calculation result."""
    start_time: int  # Epoch seconds (12:00:00 UTC previous day)
    end_time: int    # Epoch seconds (12:00:00 UTC current session)
    duration_seconds: int
    start_utc: str
    end_utc: str
    start_ist: str
    end_ist: str

