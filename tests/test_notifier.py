"""Unit tests for CallMeBot WhatsApp notifier module.

Tests message template formatting, credential resolution, graceful skipping,
and standard library HTTP dispatch.
"""
from __future__ import annotations

import io
import unittest
from unittest.mock import MagicMock, patch
import urllib.error

from notifier import format_settlement_message, send_whatsapp_notification


class TestWhatsAppNotifier(unittest.TestCase):
    """Test suite for WhatsApp CallMeBot notifier."""

    def setUp(self):
        self.sample_summary = {
            "status": "SUCCESS",
            "elapsed_seconds": 43.68,
            "settlement_window": {
                "start_ist": "2026-10-31 17:30:00 IST",
                "end_ist": "2026-11-01 17:30:00 IST",
            },
            "databases": {
                "futures_db": {
                    "total_candles": 5724,
                    "series_passed": 18,
                    "series_total": 18,
                },
                "options_db": {
                    "total_contracts_scanned": 328,
                    "contracts_with_activity": 185,
                    "total_candles": 19417,
                },
            },
            "futures_integrity": {
                "overall_status": "PASSED",
                "series_details": [
                    {"symbol": "BTCUSDT", "gaps_count": 0},
                    {"symbol": "ETHUSDT", "gaps_count": 0},
                ],
            },
        }

    def test_format_settlement_message(self):
        msg = format_settlement_message(self.sample_summary)
        expected_lines = [
            "DeltaSync Settlement Report (Success)",
            "Window: 2026-10-31 17:30:00 to 2026-11-01 17:30:00 IST",
            "Elapsed: 43.7s",
            "Futures: 18/18 series (5,724 bars, 0 gaps)",
            "Options: 328 contracts scanned (19,417 bars across 185 active)",
            "Status: All DB checks passed.",
        ]
        for line in expected_lines:
            self.assertIn(line, msg)

    def test_skip_when_credentials_missing(self):
        # When neither phone nor apikey is provided
        res = send_whatsapp_notification(phone=None, apikey=None, message="Test")
        self.assertFalse(res["sent"])
        self.assertTrue(res["skipped"])
        self.assertIn("Missing phone number or API key", res["reason"])

    def test_skip_when_message_empty(self):
        res = send_whatsapp_notification(phone="+919876543210", apikey="123456", message="   ")
        self.assertFalse(res["sent"])
        self.assertTrue(res["skipped"])

    @patch("urllib.request.urlopen")
    def test_successful_dispatch(self, mock_urlopen):
        # Mock successful HTTP 200 response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b"Message queued"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        res = send_whatsapp_notification(
            phone="+919876543210",
            apikey="secret123",
            message="Hello WhatsApp",
        )
        self.assertTrue(res["sent"])
        self.assertFalse(res["skipped"])
        self.assertEqual(res["status_code"], 200)

        # Verify request URL format
        called_req = mock_urlopen.call_args[0][0]
        self.assertIn("phone=%2B919876543210", called_req.full_url)
        self.assertIn("apikey=secret123", called_req.full_url)
        self.assertIn("text=Hello+WhatsApp", called_req.full_url)

    @patch("urllib.request.urlopen")
    def test_http_error_graceful_handling(self, mock_urlopen):
        # Mock HTTP 401 error
        mock_err = urllib.error.HTTPError(
            url="https://api.callmebot.com/whatsapp.php",
            code=401,
            msg="Unauthorized",
            hdrs={},
            fp=io.BytesIO(b"Invalid API Key"),
        )
        mock_urlopen.side_effect = mock_err

        res = send_whatsapp_notification(
            phone="+919876543210",
            apikey="wrong_key",
            message="Hello WhatsApp",
        )
        self.assertFalse(res["sent"])
        self.assertIn("HTTP 401", res["error"])


if __name__ == "__main__":
    unittest.main()

