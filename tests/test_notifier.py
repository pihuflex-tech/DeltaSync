"""Unit tests for Telegram Bot notifier module.

Tests message template formatting, credential resolution, graceful skipping,
and standard library HTTP dispatch for Telegram Bot API.
"""
from __future__ import annotations

import io
import json
import os
import unittest
from unittest.mock import MagicMock, patch
import urllib.error

from notifier import (
    format_settlement_message,
    format_telegram_markdown_message,
    send_telegram_notification,
)


class TestMockedTelegramNotifier(unittest.TestCase):
    """Test suite for mocked Telegram notifier dispatch and formatting."""

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
            "*✅ DeltaSync Settlement Report (SUCCESS)*",
            "*Settlement Window:* 2026-10-31 17:30:00 to 2026-11-01 17:30:00 IST",
            "*Elapsed:* 43.7s",
            "*Futures:* 18/18 series (5,724 bars)",
            "*Options:* 328 scanned (19,417 bars across 185 active)",
            "*Status:* All DB checks passed.",
        ]
        for line in expected_lines:
            self.assertIn(line, msg)

    def test_format_settlement_message_with_integrity_issues(self):
        summary_with_issues = {
            "status": "PARTIAL_SUCCESS",
            "elapsed_seconds": 32.1,
            "settlement_window": {
                "start_ist": "2026-10-31 17:30:00 IST",
                "end_ist": "2026-11-01 17:30:00 IST",
            },
            "databases": {
                "futures_db": {
                    "total_candles": 5000,
                    "series_passed": 17,
                    "series_total": 18,
                },
                "options_db": {
                    "total_contracts_scanned": 300,
                    "contracts_with_activity": 150,
                    "total_candles": 18000,
                },
            },
            "futures_integrity": {
                "overall_status": "FAILED",
                "series_details": [
                    {"symbol": "SOLUSDT", "issues": ["Found 3 internal timestamp gaps"]},
                ],
            },
        }
        msg = format_settlement_message(summary_with_issues)
        self.assertIn("*❌ DeltaSync Settlement Report (PARTIAL_SUCCESS)*", msg)
        self.assertIn("*Integrity Issues:* SOLUSDT: Found 3 internal timestamp gaps", msg)

    def test_skip_when_credentials_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            res = send_telegram_notification(token=None, chat_id=None, message="Test")
            self.assertFalse(res["sent"])
            self.assertTrue(res["skipped"])
            self.assertEqual(res["status_code"], 0)
            self.assertIn("Missing Telegram bot token or chat ID", res["reason"])

    def test_skip_when_message_empty(self):
        res = send_telegram_notification(token="123456:ABC", chat_id="998877", message="   ")
        self.assertFalse(res["sent"])
        self.assertTrue(res["skipped"])
        self.assertEqual(res["status_code"], 0)
        self.assertIn("Empty message body", res["reason"])

    @patch("urllib.request.urlopen")
    def test_successful_dispatch(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b'{"ok": true, "result": {"message_id": 101}}'
        mock_urlopen.return_value.__enter__.return_value = mock_response

        res = send_telegram_notification(
            token="123456:TEST_TOKEN",
            chat_id="998877",
            message="Hello Telegram",
        )
        self.assertTrue(res["sent"])
        self.assertFalse(res["skipped"])
        self.assertEqual(res["status_code"], 200)

        called_req = mock_urlopen.call_args[0][0]
        self.assertEqual(called_req.full_url, "https://api.telegram.org/bot123456:TEST_TOKEN/sendMessage")
        self.assertEqual(called_req.method, "POST")
        self.assertEqual(called_req.headers.get("Content-type"), "application/json")

        payload = json.loads(called_req.data.decode("utf-8"))
        self.assertEqual(payload["chat_id"], "998877")
        self.assertEqual(payload["text"], "Hello Telegram")
        self.assertEqual(payload["parse_mode"], "Markdown")

    @patch("urllib.request.urlopen")
    def test_http_error_graceful_handling(self, mock_urlopen):
        mock_err = urllib.error.HTTPError(
            url="https://api.telegram.org/bot123456:TEST_TOKEN/sendMessage",
            code=401,
            msg="Unauthorized",
            hdrs={},
            fp=io.BytesIO(b'{"ok": false, "description": "Invalid token"}'),
        )
        mock_urlopen.side_effect = mock_err

        res = send_telegram_notification(
            token="123456:WRONG_TOKEN",
            chat_id="998877",
            message="Hello Telegram",
        )
        self.assertFalse(res["sent"])
        self.assertEqual(res["status_code"], 401)
        self.assertIn("HTTP 401", res["error"])


# Backward compatibility alias for test suite discovery
TestWhatsAppNotifier = TestMockedTelegramNotifier


class TestTelegramNotifier(unittest.TestCase):
    """Test suite for Telegram Bot notifier functions and fallbacks."""

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

    def test_format_telegram_markdown_message(self):
        msg = format_telegram_markdown_message(self.sample_summary)
        self.assertIn("*✅ DeltaSync Settlement Report (SUCCESS)*", msg)
        self.assertIn("*Settlement Window:* 2026-10-31 17:30:00 to 2026-11-01 17:30:00 IST", msg)
        self.assertIn("*Elapsed:* 43.7s", msg)
        self.assertIn("*Futures:* 18/18 series (5,724 bars)", msg)
        self.assertIn("*Options:* 328 scanned (19,417 bars across 185 active)", msg)
        self.assertIn("*Status:* All DB checks passed.", msg)

    def test_telegram_skip_when_credentials_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            res = send_telegram_notification(token=None, chat_id=None, message="Test")
            self.assertFalse(res["sent"])
            self.assertTrue(res["skipped"])
            self.assertIn("Missing Telegram bot token or chat ID", res["reason"])

    def test_telegram_skip_when_token_only(self):
        with patch.dict(os.environ, {}, clear=True):
            res = send_telegram_notification(token="123456:ABC", chat_id=None, message="Test")
            self.assertFalse(res["sent"])
            self.assertTrue(res["skipped"])

    def test_telegram_skip_when_message_empty(self):
        res = send_telegram_notification(token="123456:ABC", chat_id="987654", message="   ")
        self.assertFalse(res["sent"])
        self.assertTrue(res["skipped"])

    @patch("urllib.request.urlopen")
    def test_telegram_successful_dispatch(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b'{"ok": true, "result": {"message_id": 101}}'
        mock_urlopen.return_value.__enter__.return_value = mock_response

        res = send_telegram_notification(
            token="123456:TEST_TOKEN",
            chat_id="998877",
            message="*Hello Telegram*",
        )
        self.assertTrue(res["sent"])
        self.assertFalse(res["skipped"])
        self.assertEqual(res["status_code"], 200)

        # Inspect request
        called_req = mock_urlopen.call_args[0][0]
        self.assertEqual(called_req.full_url, "https://api.telegram.org/bot123456:TEST_TOKEN/sendMessage")
        self.assertEqual(called_req.method, "POST")
        self.assertEqual(called_req.headers.get("Content-type"), "application/json")

        payload = json.loads(called_req.data.decode("utf-8"))
        self.assertEqual(payload["chat_id"], "998877")
        self.assertEqual(payload["text"], "*Hello Telegram*")
        self.assertEqual(payload["parse_mode"], "Markdown")

    @patch("urllib.request.urlopen")
    def test_telegram_http_error_graceful_handling(self, mock_urlopen):
        mock_err = urllib.error.HTTPError(
            url="https://api.telegram.org/bot123456:TEST_TOKEN/sendMessage",
            code=400,
            msg="Bad Request",
            hdrs={},
            fp=io.BytesIO(b'{"ok": false, "description": "Chat not found"}'),
        )
        mock_urlopen.side_effect = mock_err

        res = send_telegram_notification(
            token="123456:TEST_TOKEN",
            chat_id="invalid_chat",
            message="Test msg",
        )
        self.assertFalse(res["sent"])
        self.assertEqual(res["status_code"], 400)
        self.assertIn("HTTP 400", res["error"])
        self.assertIn("Chat not found", res["error"])

    @patch("urllib.request.urlopen")
    def test_telegram_network_error_graceful_handling(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Connection timed out")

        res = send_telegram_notification(
            token="123456:TEST_TOKEN",
            chat_id="998877",
            message="Test msg",
        )
        self.assertFalse(res["sent"])
        self.assertEqual(res["status_code"], 0)
        self.assertIn("Connection timed out", res["error"])

    @patch("urllib.request.urlopen")
    def test_telegram_env_var_fallback(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b'{"ok": true}'
        mock_urlopen.return_value.__enter__.return_value = mock_response

        env_vars = {
            "TELEGRAM_BOT_TOKEN": "ENV_TOKEN_789",
            "TELEGRAM_CHAT_ID": "ENV_CHAT_456",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            res = send_telegram_notification(message="Env Test")
            self.assertTrue(res["sent"])
            self.assertEqual(res["status_code"], 200)

            called_req = mock_urlopen.call_args[0][0]
            self.assertEqual(called_req.full_url, "https://api.telegram.org/botENV_TOKEN_789/sendMessage")
            payload = json.loads(called_req.data.decode("utf-8"))
            self.assertEqual(payload["chat_id"], "ENV_CHAT_456")


if __name__ == "__main__":
    unittest.main()
