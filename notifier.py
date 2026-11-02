"""Notification module for DeltaSync pipeline supporting Telegram Bot API.

Uses pure Python standard library (urllib.request, json, os, logging).
Gracefully skips when credentials are not configured or on network failures.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Mapping, Optional
import urllib.error
import urllib.request

logger = logging.getLogger("DeltaSync.Notifier")

DEFAULT_TIMEOUT = 10.0


def format_settlement_message(summary: Mapping[str, Any]) -> str:
    """Format settlement summary into clean Telegram Markdown.

    Template format:
    *✅/❌ DeltaSync Settlement Report ({status})*
    *Settlement Window:* {start_ist} to {end_ist}
    *Elapsed:* {elapsed:.1f}s
    *Futures:* {passed}/{total} series ({candles:,} bars)
    *Options:* {scanned} scanned ({candles:,} bars across {active} active)
    *Status:* All DB checks passed. (or integrity issues if any)
    """
    raw_status = summary.get("status", "SUCCESS")
    status = raw_status if raw_status else "SUCCESS"
    is_success = str(status).upper() in ("SUCCESS", "PASSED")
    icon = "✅" if is_success else "❌"
    header = f"*{icon} DeltaSync Settlement Report ({status})*"

    window = summary.get("settlement_window", {})
    start_ist = str(window.get("start_ist", "")).strip()
    end_ist = str(window.get("end_ist", "")).strip()

    if start_ist.endswith(" IST") and end_ist.endswith(" IST"):
        window_str = f"{start_ist[:-4]} to {end_ist}"
    elif start_ist and end_ist:
        window_str = f"{start_ist} to {end_ist}"
    else:
        window_str = f"{start_ist or end_ist or 'N/A'}"

    elapsed = summary.get("elapsed_seconds", 0.0)

    f_db = summary.get("databases", {}).get("futures_db", {})
    o_db = summary.get("databases", {}).get("options_db", {})

    passed = f_db.get("series_passed", 0)
    total = f_db.get("series_total", 0)
    candles = f_db.get("total_candles", 0)

    scanned = o_db.get("total_contracts_scanned", 0)
    active = o_db.get("contracts_with_activity", 0)
    opt_candles = o_db.get("total_candles", 0)

    # Detect integrity issues
    issues: list[str] = []
    futures_integrity = summary.get("futures_integrity", {})
    if isinstance(futures_integrity, dict):
        for s in futures_integrity.get("series_details", []):
            sym = s.get("symbol", "")
            s_issues = s.get("issues", [])
            for iss in s_issues:
                issues.append(f"{sym}: {iss}" if sym else str(iss))
            if not s_issues and s.get("gaps_count", 0) > 0:
                issues.append(f"{sym}: {s.get('gaps_count')} gaps" if sym else f"{s.get('gaps_count')} gaps")
            elif not s_issues and s.get("status") not in (None, "PASSED"):
                issues.append(f"{sym}: {s.get('status')}" if sym else str(s.get("status")))

    if issues:
        status_line = f"*Integrity Issues:* {'; '.join(issues)}"
    elif not is_success:
        status_line = f"*Status:* Warnings during verification ({status})."
    else:
        status_line = "*Status:* All DB checks passed."

    lines = [
        header,
        f"*Settlement Window:* {window_str}",
        f"*Elapsed:* {elapsed:.1f}s",
        f"*Futures:* {passed}/{total} series ({candles:,} bars)",
        f"*Options:* {scanned} scanned ({opt_candles:,} bars across {active} active)",
        status_line,
    ]
    return "\n".join(lines)


# Backward compatibility alias
format_telegram_markdown_message = format_settlement_message


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
        Structured status dict {"sent": bool, "skipped": bool, "status_code": int, ...}.
    """
    resolved_token = token or os.environ.get("TELEGRAM_BOT_TOKEN")
    resolved_chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")

    if not resolved_token or not resolved_chat_id:
        logger.info("Telegram credentials not provided (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Skipping notification.")
        return {
            "sent": False,
            "skipped": True,
            "status_code": 0,
            "reason": "Missing Telegram bot token or chat ID",
        }

    if not message.strip():
        logger.warning("Telegram notification message is empty. Skipping.")
        return {
            "sent": False,
            "skipped": True,
            "status_code": 0,
            "reason": "Empty message body",
        }

    url = f"https://api.telegram.org/bot{resolved_token.strip()}/sendMessage"
    payload = {
        "chat_id": str(resolved_chat_id).strip(),
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
                "status_code": getattr(resp, "status", 200),
                "response": resp_body.strip(),
            }
    except urllib.error.HTTPError as http_err:
        try:
            err_msg = http_err.read().decode("utf-8", errors="replace")
        finally:
            http_err.close()
        logger.warning("Telegram HTTP %d error: %s", http_err.code, err_msg)
        return {
            "sent": False,
            "skipped": False,
            "status_code": http_err.code,
            "error": f"HTTP {http_err.code}: {err_msg}",
        }
    except Exception as exc:
        logger.warning("Failed to send Telegram notification: %s", exc)
        return {
            "sent": False,
            "skipped": False,
            "status_code": 0,
            "error": str(exc),
        }
