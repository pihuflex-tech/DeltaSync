"""Notification module for DeltaSync pipeline supporting Telegram Bot and CallMeBot WhatsApp.

Uses pure Python standard library (urllib.request, urllib.parse, and json).
Gracefully skips when credentials are not configured or on network failures.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Mapping, Optional
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger("DeltaSync.Notifier")

CALLMEBOT_URL = "https://api.callmebot.com/whatsapp.php"
DEFAULT_TIMEOUT = 10.0


def format_settlement_message(summary: Mapping[str, Any]) -> str:
    """Format concise plaintext settlement message template from verification summary."""
    window = summary.get("settlement_window", {})
    start_ist = window.get("start_ist", "").replace(" IST", "")
    end_ist = window.get("end_ist", "").replace(" IST", "")
    elapsed = summary.get("elapsed_seconds", 0.0)

    f_db = summary.get("databases", {}).get("futures_db", {})
    o_db = summary.get("databases", {}).get("options_db", {})

    passed_series = f_db.get("series_passed", 0)
    total_series = f_db.get("series_total", 0)
    futures_bars = f"{f_db.get('total_candles', 0):,}"

    # Sum total gaps across series details
    series_details = summary.get("futures_integrity", {}).get("series_details", [])
    total_gaps = sum(s.get("gaps_count", 0) for s in series_details)

    contracts_scanned = o_db.get("total_contracts_scanned", 0)
    active_contracts = o_db.get("contracts_with_activity", 0)
    options_bars = f"{o_db.get('total_candles', 0):,}"

    status_line = "All DB checks passed." if summary.get("status") == "SUCCESS" else "Warnings during verification."

    lines = [
        "DeltaSync Settlement Report (Success)" if summary.get("status") == "SUCCESS" else "DeltaSync Settlement Report (Partial)",
        f"Window: {start_ist} to {end_ist} IST",
        f"Elapsed: {elapsed:.1f}s",
        f"Futures: {passed_series}/{total_series} series ({futures_bars} bars, {total_gaps} gaps)",
        f"Options: {contracts_scanned} contracts scanned ({options_bars} bars across {active_contracts} active)",
        f"Status: {status_line}",
    ]
    return "\n".join(lines)


def format_telegram_markdown_message(summary: Mapping[str, Any]) -> str:
    """Format Markdown-styled settlement message for Telegram Bot.

    Expected template:
    *DeltaSync Settlement Report (Success)*
    *Window:* `2026-10-31 17:30 to 2026-11-01 17:30 IST`
    *Elapsed:* `43.7s`
    *Futures:* 18/18 series (5,724 bars, 0 gaps)
    *Options:* 328 contracts scanned (19,417 bars across 185 active)
    *Status:* All DB checks passed.
    """
    window = summary.get("settlement_window", {})
    start_ist = window.get("start_ist", "").replace(" IST", "")
    end_ist = window.get("end_ist", "").replace(" IST", "")
    elapsed = summary.get("elapsed_seconds", 0.0)

    f_db = summary.get("databases", {}).get("futures_db", {})
    o_db = summary.get("databases", {}).get("options_db", {})

    passed_series = f_db.get("series_passed", 0)
    total_series = f_db.get("series_total", 0)
    futures_bars = f"{f_db.get('total_candles', 0):,}"

    series_details = summary.get("futures_integrity", {}).get("series_details", [])
    total_gaps = sum(s.get("gaps_count", 0) for s in series_details)

    contracts_scanned = o_db.get("total_contracts_scanned", 0)
    active_contracts = o_db.get("contracts_with_activity", 0)
    options_bars = f"{o_db.get('total_candles', 0):,}"

    status_line = "All DB checks passed." if summary.get("status") == "SUCCESS" else "Warnings during verification."
    title = "*DeltaSync Settlement Report (Success)*" if summary.get("status") == "SUCCESS" else "*DeltaSync Settlement Report (Partial)*"

    lines = [
        title,
        f"*Window:* `{start_ist} to {end_ist} IST`",
        f"*Elapsed:* `{elapsed:.1f}s`",
        f"*Futures:* {passed_series}/{total_series} series ({futures_bars} bars, {total_gaps} gaps)",
        f"*Options:* {contracts_scanned} contracts scanned ({options_bars} bars across {active_contracts} active)",
        f"*Status:* {status_line}",
    ]
    return "\n".join(lines)


def send_telegram_notification(
    token: Optional[str] = None,
    chat_id: Optional[str] = None,
    message: str = "",
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Send Telegram message via Telegram Bot API using urllib.request.

    Args:
        token: Telegram Bot Token (e.g. 123456:ABC-DEF...). Defaults to TELEGRAM_BOT_TOKEN env var.
        chat_id: Telegram Chat ID (e.g. 123456789 or @channel). Defaults to TELEGRAM_CHAT_ID env var.
        message: Markdown formatted message string to send.
        timeout: HTTP request timeout in seconds.

    Returns:
        Dictionary with status, sent boolean, and reason/error description.
    """
    resolved_token = token or os.environ.get("TELEGRAM_BOT_TOKEN")
    resolved_chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")

    if not resolved_token or not resolved_chat_id:
        logger.info("Telegram credentials not provided (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Skipping notification.")
        return {
            "sent": False,
            "skipped": True,
            "reason": "Missing Telegram bot token or chat ID",
        }

    if not message.strip():
        logger.warning("Telegram notification message is empty. Skipping.")
        return {
            "sent": False,
            "skipped": True,
            "reason": "Empty message body",
        }

    url = f"https://api.telegram.org/bot{resolved_token.strip()}/sendMessage"
    payload = {
        "chat_id": resolved_chat_id.strip(),
        "text": message.strip(),
        "parse_mode": "Markdown",
    }
    json_data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=json_data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "DeltaSync/1.0 (Telegram Bot Notifier)",
        },
        method="POST",
    )

    try:
        logger.info("Dispatching Telegram notification to chat %s...", resolved_chat_id)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
            logger.info("Telegram notification sent successfully.")
            return {
                "sent": True,
                "skipped": False,
                "status_code": resp.status,
                "response": resp_body.strip(),
            }
    except urllib.error.HTTPError as http_err:
        err_msg = http_err.read().decode("utf-8", errors="replace")
        logger.warning("Telegram HTTP %d error: %s", http_err.code, err_msg)
        return {
            "sent": False,
            "skipped": False,
            "error": f"HTTP {http_err.code}: {err_msg}",
        }
    except Exception as exc:
        logger.warning("Failed to send Telegram notification: %s", exc)
        return {
            "sent": False,
            "skipped": False,
            "error": str(exc),
        }


def send_whatsapp_notification(
    phone: Optional[str] = None,
    apikey: Optional[str] = None,
    message: str = "",
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Send WhatsApp message via CallMeBot API.

    Args:
        phone: Destination phone number (e.g. +919876543210). Defaults to WHATSAPP_PHONE env var.
        apikey: CallMeBot API key. Defaults to WHATSAPP_APIKEY env var.
        message: Plaintext message string to send.
        timeout: HTTP request timeout in seconds.

    Returns:
        Dictionary with status, sent boolean, and reason/error description.
    """
    resolved_phone = phone or os.environ.get("WHATSAPP_PHONE")
    resolved_apikey = apikey or os.environ.get("WHATSAPP_APIKEY")

    if not resolved_phone or not resolved_apikey:
        logger.info("WhatsApp credentials not provided (WHATSAPP_PHONE / WHATSAPP_APIKEY). Skipping notification.")
        return {
            "sent": False,
            "skipped": True,
            "reason": "Missing phone number or API key",
        }

    if not message.strip():
        logger.warning("WhatsApp notification message is empty. Skipping.")
        return {
            "sent": False,
            "skipped": True,
            "reason": "Empty message body",
        }

    params = {
        "phone": resolved_phone.strip(),
        "text": message.strip(),
        "apikey": resolved_apikey.strip(),
    }
    encoded_query = urllib.parse.urlencode(params)
    url = f"{CALLMEBOT_URL}?{encoded_query}"

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "DeltaSync/1.0 (CallMeBot WhatsApp Notifier)"},
        method="GET",
    )

    try:
        logger.info("Dispatching WhatsApp notification via CallMeBot to %s...", resolved_phone)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
            logger.info("WhatsApp notification sent successfully. Response: %s", resp_body.strip())
            return {
                "sent": True,
                "skipped": False,
                "status_code": resp.status,
                "response": resp_body.strip(),
            }
    except urllib.error.HTTPError as http_err:
        err_msg = http_err.read().decode("utf-8", errors="replace")
        logger.warning("CallMeBot HTTP %d error: %s", http_err.code, err_msg)
        return {
            "sent": False,
            "skipped": False,
            "error": f"HTTP {http_err.code}: {err_msg}",
        }
    except Exception as exc:
        logger.warning("Failed to send WhatsApp notification via CallMeBot: %s", exc)
        return {
            "sent": False,
            "skipped": False,
            "error": str(exc),
        }
