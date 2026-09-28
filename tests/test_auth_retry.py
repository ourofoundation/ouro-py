"""Auth retry: 401 / legacy 500 auth failures re-exchange the PAT once."""

from __future__ import annotations

import json
import threading
import time
import unittest
from unittest.mock import MagicMock

import httpx

from ouro.client import AutoRefreshClient, Ouro, response_needs_auth_retry


def _response(status: int, body: dict | None = None) -> httpx.Response:
    return httpx.Response(
        status,
        request=httpx.Request("POST", "https://api.ouro.foundation/datasets/x/data"),
        content=json.dumps(body or {}).encode(),
        headers={"Content-Type": "application/json"},
    )


def _single_use_exchange_client() -> Ouro:
    """An Ouro client whose API-key exchange fails if run concurrently."""
    ouro = Ouro.__new__(Ouro)
    ouro.api_key = "pat"
    ouro.access_token = "token-0"
    ouro.last_token_refresh_expiration = None
    ouro._refresh_lock = threading.RLock()
    ouro._raw_client = MagicMock(headers={})
    ouro.websocket = MagicMock(is_connected=False)
    ouro.exchange_count = 0
    in_flight = threading.Lock()

    def exchange() -> None:
        if not in_flight.acquire(blocking=False):
            raise RuntimeError("Email link is invalid or has expired")
        try:
            time.sleep(0.01)
            ouro.exchange_count += 1
            ouro.access_token = f"token-{ouro.exchange_count}"
        finally:
            in_flight.release()

    ouro.exchange_api_key = exchange
    return ouro


class AuthRetryTests(unittest.TestCase):
    def test_retries_401_after_refresh(self) -> None:
        first = _response(401, {"data": None, "error": {"message": "No user context"}})
        second = _response(200, {"data": {"ok": True}})
        raw = MagicMock()
        raw.post.side_effect = [first, second]
        ouro = MagicMock()
        ouro._token_needs_refresh.return_value = False
        client = AutoRefreshClient(raw, ouro)

        result = client.post("/datasets/x/data", json={"rows": []})
        self.assertEqual(result.status_code, 200)
        ouro._refresh_unless_changed.assert_called_once()
        self.assertEqual(raw.post.call_count, 2)

    def test_retries_legacy_500_no_user_context(self) -> None:
        first = _response(
            500, {"data": None, "error": {"message": "No user context"}}
        )
        second = _response(200, {"data": {"ok": True}})
        raw = MagicMock()
        raw.post.side_effect = [first, second]
        ouro = MagicMock()
        ouro._token_needs_refresh.return_value = False
        client = AutoRefreshClient(raw, ouro)

        result = client.post("/datasets/x/data")
        self.assertEqual(result.status_code, 200)
        ouro._refresh_unless_changed.assert_called_once()

    def test_retries_legacy_500_no_user(self) -> None:
        first = _response(500, {"data": None, "error": {"message": "No user"}})
        second = _response(200, {"data": {"ok": True}})
        raw = MagicMock()
        raw.post.side_effect = [first, second]
        ouro = MagicMock()
        ouro._token_needs_refresh.return_value = False
        client = AutoRefreshClient(raw, ouro)

        result = client.post("/comments/create")
        self.assertEqual(result.status_code, 200)
        ouro._refresh_unless_changed.assert_called_once()

    def test_does_not_retry_other_500s(self) -> None:
        raw = MagicMock()
        raw.post.return_value = _response(
            500, {"data": None, "error": {"message": "insert failed"}}
        )
        ouro = MagicMock()
        ouro._token_needs_refresh.return_value = False
        client = AutoRefreshClient(raw, ouro)

        result = client.post("/datasets/x/data")
        self.assertEqual(result.status_code, 500)
        ouro._refresh_unless_changed.assert_not_called()
        self.assertEqual(raw.post.call_count, 1)

    def test_concurrent_refreshes_exchange_once(self) -> None:
        ouro = _single_use_exchange_client()
        stale = ouro.access_token
        threads = [
            threading.Thread(target=ouro._refresh_unless_changed, args=(stale,))
            for _ in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(ouro.exchange_count, 1)
        self.assertEqual(ouro.access_token, "token-1")

    def test_concurrent_proactive_refreshes_exchange_once(self) -> None:
        ouro = _single_use_exchange_client()
        ouro._token_needs_refresh = lambda: ouro.exchange_count == 0
        threads = [threading.Thread(target=ouro.ensure_valid_token) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(ouro.exchange_count, 1)

    def test_classifier(self) -> None:
        self.assertTrue(response_needs_auth_retry(_response(401)))
        self.assertTrue(
            response_needs_auth_retry(
                _response(500, {"error": {"message": "No user context"}})
            )
        )
        self.assertTrue(
            response_needs_auth_retry(
                _response(500, {"error": {"message": "No user"}})
            )
        )
        self.assertFalse(
            response_needs_auth_retry(
                _response(500, {"error": {"message": "insert failed"}})
            )
        )


if __name__ == "__main__":
    unittest.main()
