"""Dynamic 24-hour settlement window arithmetic.

Delta Exchange daily options settle at 17:30:00 IST (12:00:00 UTC).
Dynamically calculates the most recent closed 24-hour settlement cycle:
- end_time: Most recent 12:00:00 UTC timestamp (shifted by -24h if now < 12:00 UTC).
- start_time: Exactly end_time - 86400 seconds (12:00:00 UTC previous day).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from models import SettlementWindow

# IST offset is UTC+5:30
IST_OFFSET = timezone(timedelta(hours=5, minutes=30), name="IST")


def get_settlement_window(reference_time: Optional[datetime] = None) -> SettlementWindow:
    """Calculate the latest closed 24-hour settlement window.

    Args:
        reference_time: Optional reference datetime (defaults to current UTC time).

    Returns:
        SettlementWindow instance with integer epoch timestamps and formatted representations.
    """
    if reference_time is None:
        now_utc = datetime.now(timezone.utc)
    else:
        if reference_time.tzinfo is None:
            now_utc = reference_time.replace(tzinfo=timezone.utc)
        else:
            now_utc = reference_time.astimezone(timezone.utc)

    # Today's settlement checkpoint at 12:00:00 UTC
    today_settlement = now_utc.replace(hour=12, minute=0, second=0, microsecond=0)

    if now_utc < today_settlement:
        # If current session is before 12:00 UTC, the latest closed settlement was yesterday
        end_dt = today_settlement - timedelta(days=1)
    else:
        # If current session is at or after 12:00 UTC, today's settlement has closed
        end_dt = today_settlement

    start_dt = end_dt - timedelta(days=1)

    end_epoch = int(end_dt.timestamp())
    start_epoch = int(start_dt.timestamp())

    # Formatted strings in UTC and IST
    start_ist = start_dt.astimezone(IST_OFFSET).strftime("%Y-%m-%d %H:%M:%S IST")
    end_ist = end_dt.astimezone(IST_OFFSET).strftime("%Y-%m-%d %H:%M:%S IST")

    start_utc_str = start_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    end_utc_str = end_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    return SettlementWindow(
        start_time=start_epoch,
        end_time=end_epoch,
        duration_seconds=end_epoch - start_epoch,
        start_utc=start_utc_str,
        end_utc=end_utc_str,
        start_ist=start_ist,
        end_ist=end_ist,
    )

