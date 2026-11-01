"""Resilient REST client for Delta Exchange India historical candlestick and products API.

Enforces zero third-party dependencies (pure standard library urllib.request),
thread-safe rate limiting (10 req/s ceiling), exponential backoff with jitter,
HTTP 429 Retry-After handling, and network error tracking.
"""
from __future__ import annotations

import json
import logging
import math
import random
import threading
import time
from typing import Any, Mapping, Optional, Sequence
import urllib.error
import urllib.parse
import urllib.request

from models import Candle, OptionContract

logger = logging.getLogger("DeltaSync.Client")

DEFAULT_BASE_URL = "https://api.delta.exchange"
DEFAULT_TIMEOUT = 15.0
MAX_REQUESTS_PER_SECOND = 9.5  # Safe margin under 10.0 req/s ceiling

# Canonical resolution mappings
TIMEFRAME_TO_DELTA_CODE: dict[str, str] = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "4h": "240",
    "1d": "D",
}

DELTA_CODE_TO_TIMEFRAME: dict[str, str] = {
    "1": "1m",
    "3": "3m",
    "5": "5m",
    "15": "15m",
    "30": "30m",
    "60": "1h",
    "240": "4h",
    "D": "1d",
    "1d": "1d",
}

# Timeframe duration in seconds
TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


class RateLimiter:
    """Thread-safe rate limiter pacing requests to strictly adhere to rate limits."""

    def __init__(self, max_rate: float = MAX_REQUESTS_PER_SECOND):
        self._min_interval = 1.0 / max_rate if max_rate > 0 else 0.1
        self._lock = threading.Lock()
        self._last_request_time = 0.0

    def acquire(self) -> None:
        """Block until the minimum interval since the last request has elapsed."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            sleep_needed = self._min_interval - elapsed
            if sleep_needed > 0:
                time.sleep(sleep_needed)
            self._last_request_time = time.monotonic()


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert value to float, handling None and type errors."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


class DeltaClient:
    """Pure standard-library HTTP client for Delta Exchange API."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_rate: float = MAX_REQUESTS_PER_SECOND,
        max_retries: int = 4,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limiter = RateLimiter(max_rate)
        self._lock = threading.Lock()

        # Telemetry / Network error tracking
        self.total_requests = 0
        self.total_retries = 0
        self.total_failures = 0
        self.status_code_counts: dict[int, int] = {}
        self.error_log: list[dict[str, Any]] = []

    def _record_error(self, url: str, error_type: str, status: Optional[int], message: str) -> None:
        """Record network telemetry for the integrity audit."""
        with self._lock:
            if status is not None:
                self.status_code_counts[status] = self.status_code_counts.get(status, 0) + 1
            self.error_log.append({
                "timestamp": int(time.time()),
                "url": url,
                "type": error_type,
                "status": status,
                "message": message,
            })

    def request_json(
        self,
        endpoint: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        """Execute HTTP GET request with rate limiting and exponential backoff.

        Catches HTTP 429 / 5xx, URLError, and timeouts.
        Clamps backoff delay between 1.0s and 30.0s.
        """
        query_string = urllib.parse.urlencode(params) if params else ""
        url = f"{self.base_url}{endpoint}"
        if query_string:
            url = f"{url}?{query_string}"

        headers = {
            "User-Agent": "DeltaSync/1.0 (StandardLibrary; MemoryOptimized)",
            "Accept": "application/json",
        }

        req = urllib.request.Request(url, headers=headers, method="GET")

        for attempt in range(self.max_retries + 1):
            self.rate_limiter.acquire()
            with self._lock:
                self.total_requests += 1

            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    raw_body = response.read()
                    data = json.loads(raw_body.decode("utf-8"))
                    return data

            except urllib.error.HTTPError as http_err:
                status = http_err.code
                error_body = http_err.read().decode("utf-8", errors="replace")
                self._record_error(url, "HTTPError", status, error_body)

                # Retry on 429 (Rate Limit) or 5xx (Server Errors)
                if (status == 429 or status >= 500) and attempt < self.max_retries:
                    with self._lock:
                        self.total_retries += 1

                    # Check Retry-After header
                    retry_after = http_err.headers.get("Retry-After") if http_err.headers else None
                    if retry_after:
                        try:
                            sleep_duration = float(retry_after)
                        except ValueError:
                            sleep_duration = 2.0 ** attempt
                    else:
                        base_backoff = 1.0 * (2 ** attempt)
                        jitter = random.uniform(0.1, 0.5)
                        sleep_duration = base_backoff + jitter

                    sleep_duration = max(1.0, min(30.0, sleep_duration))
                    logger.warning(
                        "HTTP %d on %s (Attempt %d/%d). Backing off for %.2fs...",
                        status,
                        endpoint,
                        attempt + 1,
                        self.max_retries,
                        sleep_duration,
                    )
                    time.sleep(sleep_duration)
                    continue
                else:
                    with self._lock:
                        self.total_failures += 1
                    logger.error("HTTP %d error requesting %s: %s", status, url, error_body)
                    raise

            except (urllib.error.URLError, TimeoutError, OSError) as net_err:
                self._record_error(url, type(net_err).__name__, None, str(net_err))
                if attempt < self.max_retries:
                    with self._lock:
                        self.total_retries += 1
                    sleep_duration = min(30.0, (1.5 ** attempt) + random.uniform(0.2, 0.8))
                    logger.warning(
                        "Network error %s on %s. Retrying in %.2fs (Attempt %d/%d)...",
                        net_err,
                        endpoint,
                        sleep_duration,
                        attempt + 1,
                        self.max_retries,
                    )
                    time.sleep(sleep_duration)
                    continue
                else:
                    with self._lock:
                        self.total_failures += 1
                    logger.error("Network failure requesting %s: %s", url, net_err)
                    raise

        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts.")

    def fetch_candles(
        self,
        symbol: str,
        resolution: str,
        start_time: int,
        end_time: int,
    ) -> list[Candle]:
        """Fetch OHLCV candlestick data from Delta Exchange /v2/chart/history.

        Args:
            symbol: Symbol name, e.g. 'BTCUSDT' or 'C-BTC-80800-041126'.
            resolution: Canonical timeframe string ('1m', '5m', '15m', '30m', '1h', '4h', '1d').
            start_time: Unix epoch start timestamp in seconds.
            end_time: Unix epoch end timestamp in seconds.

        Returns:
            List of Candle instances.
        """
        delta_res = TIMEFRAME_TO_DELTA_CODE.get(resolution, resolution)
        params = {
            "symbol": symbol,
            "resolution": delta_res,
            "from": start_time,
            "to": end_time,
        }

        data = self.request_json("/v2/chart/history", params=params)
        if not data:
            return []

        result = data.get("result", {})
        if not isinstance(result, dict) or result.get("s") == "no_data":
            return []

        timestamps = result.get("t", [])
        opens = result.get("o", [])
        highs = result.get("h", [])
        lows = result.get("l", [])
        closes = result.get("c", [])
        volumes = result.get("v", [])

        if not timestamps:
            return []

        candles: list[Candle] = []
        count = len(timestamps)
        for i in range(count):
            ts = int(timestamps[i])
            # Normalize millisecond timestamps if encountered
            if ts > 100_000_000_000:
                ts = ts // 1000

            raw_c = closes[i] if i < len(closes) else 0.0
            raw_o = opens[i] if i < len(opens) else raw_c
            raw_h = highs[i] if i < len(highs) else max(raw_o, raw_c)
            raw_l = lows[i] if i < len(lows) else min(raw_o, raw_c)
            raw_v = volumes[i] if i < len(volumes) else 0.0

            c = _safe_float(raw_c)
            o = _safe_float(raw_o, default=c)
            h = _safe_float(raw_h, default=max(o, c))
            l = _safe_float(raw_l, default=min(o, c))
            v = _safe_float(raw_v, default=0.0)

            candles.append(
                Candle(
                    symbol=symbol,
                    resolution=resolution,
                    time=ts,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=v,
                )
            )

        return candles

    def fetch_option_products(self, states: str = "live,expired") -> list[OptionContract]:
        """Fetch all option products matching live and expired states from /v2/products."""
        params = {
            "contract_types": "call_options,put_options",
            "states": states,
        }
        data = self.request_json("/v2/products", params=params)
        raw_products = data.get("result", []) if isinstance(data, dict) else []

        contracts: list[OptionContract] = []
        for p in raw_products:
            if not isinstance(p, dict):
                continue
            sym = p.get("symbol", "")
            st = p.get("settlement_time")
            if not sym or not st:
                continue

            contracts.append(
                OptionContract(
                    symbol=sym,
                    underlying_asset=p.get("underlying_asset", {}).get("symbol", "")
                    if isinstance(p.get("underlying_asset"), dict)
                    else str(p.get("underlying_asset") or ""),
                    contract_type=str(p.get("contract_type", "")),
                    strike_price=_safe_float(p.get("strike_price")),
                    settlement_time=str(st),
                    state=str(p.get("state", "")),
                )
            )

        return contracts

    def get_network_telemetry(self) -> dict[str, Any]:
        """Return network summary metrics for integrity auditing."""
        with self._lock:
            return {
                "total_requests": self.total_requests,
                "total_retries": self.total_retries,
                "total_failures": self.total_failures,
                "status_code_counts": dict(self.status_code_counts),
                "recent_errors": list(self.error_log[-10:]),
            }

