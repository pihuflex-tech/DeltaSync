"""Main CLI entrypoint for DeltaSync: 24-Hour Settlement ETL & Validation Pipeline.

Orchestrates Phase 1 through Phase 4:
- Computes dynamic 24-hour settlement window (12:00 UTC previous day to 12:00 UTC current session).
- Initializes isolated SQLite databases in WAL mode (futures_data.db & options_data.db).
- Fetches 3 futures symbols across 6 timeframes and complete options chain across 5 expiries.
- Runs integrity auditor with automated retry loop.
- Outputs structured JSON verification summary to stdout.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import signal
import sys
import time
from typing import Any

from auditor import IntegrityAuditor
from client import DeltaClient
from engine import (
    DEFAULT_FUTURES_SYMBOLS,
    DEFAULT_FUTURES_TIMEFRAMES,
    DEFAULT_OPTIONS_RESOLUTION,
    IngestionEngine,
)
from models import SettlementWindow
from notifier import (
    format_settlement_message,
    format_telegram_markdown_message,
    send_telegram_notification,
    send_whatsapp_notification,
)
from settlement import get_settlement_window
from storage import FuturesStorage, OptionsStorage

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DeltaSync")


def format_progress(prefix: str, current: int, total: int) -> None:
    """Print terminal progress update."""
    pct = (current / total * 100) if total > 0 else 100
    sys.stderr.write(f"\r\033[K[{current}/{total}] ({pct:5.1f}%) {prefix}")
    sys.stderr.flush()
    if current == total:
        sys.stderr.write("\n")
        sys.stderr.flush()


def run_pipeline(args: argparse.Namespace) -> int:
    """Execute complete end-to-end ETL and validation pipeline."""
    start_wall_time = time.time()
    logger.info("Initializing DeltaSync ETL & Validation Pipeline...")

    # Phase 1: Storage & Settlement Window
    window = get_settlement_window()
    logger.info(
        "Settlement Window calculated: %s (%s) -> %s (%s) [Duration: %d seconds]",
        window.start_utc,
        window.start_ist,
        window.end_utc,
        window.end_ist,
        window.duration_seconds,
    )

    futures_storage = FuturesStorage(db_path=args.futures_db)
    options_storage = OptionsStorage(db_path=args.options_db)

    # Register graceful exit handlers
    def handle_exit(sig: int, frame: Any) -> None:
        logger.warning("\nReceived signal %d. Closing databases cleanly...", sig)
        futures_storage.close()
        options_storage.close()
        sys.exit(130)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    client = DeltaClient(
        base_url=args.base_url,
        max_rate=args.max_rate,
        max_retries=args.max_retries,
    )

    engine = IngestionEngine(
        client=client,
        futures_storage=futures_storage,
        options_storage=options_storage,
        max_workers=args.max_workers,
    )

    auditor = IntegrityAuditor(
        futures_storage=futures_storage,
        options_storage=options_storage,
        client=client,
    )

    # Parse symbols and timeframes
    futures_symbols = [s.strip() for s in args.futures_symbols.split(",") if s.strip()]
    futures_timeframes = [t.strip() for t in args.futures_timeframes.split(",") if t.strip()]

    # Phase 2: Ingestion Engine
    logger.info("--- PHASE 2.1: Futures Ingestion ---")
    futures_results = engine.run_futures_ingestion(
        window=window,
        symbols=futures_symbols,
        timeframes=futures_timeframes,
        progress_cb=format_progress if not args.quiet else None,
    )

    logger.info("--- PHASE 2.2: Options Chain Discovery & Ingestion ---")
    selected_expiries, target_contracts = engine.discover_target_options_expiries(
        window=window,
        num_expiries=args.options_expiries_count,
    )

    options_results = engine.run_options_ingestion(
        window=window,
        contracts=target_contracts,
        resolution=args.options_resolution,
        progress_cb=format_progress if not args.quiet else None,
    )

    # Phase 3: Integrity Auditor & Retry Loop
    logger.info("--- PHASE 3: Integrity Auditor & Validation ---")
    futures_audit = auditor.audit_all_futures(
        symbols=futures_symbols,
        timeframes=futures_timeframes,
        window=window,
    )

    # If incomplete, trigger automated retry loop
    if futures_audit["overall_status"] != "PASSED":
        logger.warning("Integrity check flagged incomplete series. Triggering retry loop...")
        futures_audit = auditor.retry_incomplete_futures(
            audit_report=futures_audit,
            window=window,
            max_retry_rounds=args.max_retry_rounds,
        )

    options_audit = auditor.audit_options(window=window)
    network_telemetry = client.get_network_telemetry()

    elapsed = round(time.time() - start_wall_time, 2)
    logger.info("Pipeline execution completed in %.2f seconds.", elapsed)

    # Close databases cleanly
    futures_storage.close()
    options_storage.close()

    # Phase 4: Output structured JSON verification summary
    verification_summary = {
        "status": "SUCCESS" if futures_audit["overall_status"] == "PASSED" else "PARTIAL_SUCCESS",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed,
        "settlement_window": {
            "start_time": window.start_time,
            "end_time": window.end_time,
            "duration_seconds": window.duration_seconds,
            "start_utc": window.start_utc,
            "end_utc": window.end_utc,
            "start_ist": window.start_ist,
            "end_ist": window.end_ist,
        },
        "databases": {
            "futures_db": {
                "file": args.futures_db,
                "table": "futures_candles",
                "total_candles": futures_audit["total_bars"],
                "series_passed": futures_audit["passed_series"],
                "series_total": futures_audit["total_series"],
            },
            "options_db": {
                "file": args.options_db,
                "table": "options_candles",
                "expiries_tracked": selected_expiries,
                "total_contracts_scanned": options_results["total_contracts_scanned"],
                "contracts_with_activity": options_results["contracts_with_activity"],
                "total_candles": options_audit["total_option_bars"],
            },
        },
        "futures_integrity": {
            "overall_status": futures_audit["overall_status"],
            "series_details": futures_audit["series"],
        },
        "network_telemetry": network_telemetry,
    }

    # Dispatch Telegram notification if configured (or gracefully skip)
    telegram_msg = format_telegram_markdown_message(verification_summary)
    telegram_result = send_telegram_notification(
        token=args.telegram_token,
        chat_id=args.telegram_chat_id,
        message=telegram_msg,
    )
    verification_summary["telegram_notification"] = telegram_result

    # Dispatch WhatsApp notification if configured (or gracefully skip)
    whatsapp_msg = format_settlement_message(verification_summary)
    whatsapp_result = send_whatsapp_notification(
        phone=args.whatsapp_phone,
        apikey=args.whatsapp_key,
        message=whatsapp_msg,
    )
    verification_summary["whatsapp_notification"] = whatsapp_result

    print(json.dumps(verification_summary, indent=2))
    return 0 if futures_audit["overall_status"] == "PASSED" else 1


def main() -> None:
    """Parse command line arguments and execute pipeline."""
    parser = argparse.ArgumentParser(
        description="DeltaSync: Zero-Dependency 24-Hour Settlement ETL & Validation Pipeline",
    )
    parser.add_argument(
        "--futures-symbols",
        default=",".join(DEFAULT_FUTURES_SYMBOLS),
        help=f"Comma-separated futures symbols (default: {','.join(DEFAULT_FUTURES_SYMBOLS)})",
    )
    parser.add_argument(
        "--futures-timeframes",
        default=",".join(DEFAULT_FUTURES_TIMEFRAMES),
        help=f"Comma-separated futures timeframes (default: {','.join(DEFAULT_FUTURES_TIMEFRAMES)})",
    )
    parser.add_argument(
        "--options-expiries-count",
        type=int,
        default=5,
        help="Number of option expiries to ingest starting from today's settled session (default: 5)",
    )
    parser.add_argument(
        "--options-resolution",
        default=DEFAULT_OPTIONS_RESOLUTION,
        help=f"Candle resolution for options ingestion (default: {DEFAULT_OPTIONS_RESOLUTION})",
    )
    parser.add_argument(
        "--futures-db",
        default="futures_data.db",
        help="SQLite database path for futures data (default: futures_data.db)",
    )
    parser.add_argument(
        "--options-db",
        default="options_data.db",
        help="SQLite database path for options data (default: options_data.db)",
    )
    parser.add_argument(
        "--base-url",
        default="https://api.delta.exchange",
        help="Delta Exchange base API URL (default: https://api.delta.exchange)",
    )
    parser.add_argument(
        "--max-rate",
        type=float,
        default=9.5,
        help="Maximum requests per second (default: 9.5)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="Max retries per HTTP request on 429/5xx (default: 4)",
    )
    parser.add_argument(
        "--max-retry-rounds",
        type=int,
        default=3,
        help="Max rounds of auditor retries for incomplete series (default: 3)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of worker threads (default: 4)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress stderr progress bar updates",
    )
    parser.add_argument(
        "--telegram-token",
        default=os.environ.get("TELEGRAM_BOT_TOKEN"),
        help="Telegram Bot Token (e.g. 123456:ABC-DEF...). Defaults to TELEGRAM_BOT_TOKEN",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default=os.environ.get("TELEGRAM_CHAT_ID"),
        help="Telegram Chat ID (e.g. 123456789 or @channel). Defaults to TELEGRAM_CHAT_ID",
    )
    parser.add_argument(
        "--whatsapp-phone",
        default=os.environ.get("WHATSAPP_PHONE"),
        help="Destination WhatsApp phone number with country code (e.g. +919876543210)",
    )
    parser.add_argument(
        "--whatsapp-key",
        default=os.environ.get("WHATSAPP_APIKEY"),
        help="CallMeBot WhatsApp API key",
    )

    args = parser.parse_args()
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()

