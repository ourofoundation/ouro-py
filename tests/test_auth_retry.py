"""Auth retry: 401 / legacy 500 No user context re-exchanges the PAT once."""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock

import httpx

from ouro.client import AutoRefreshClient, response_needs_auth_retry


def _response(status: int, body: dict | None = None) -> httpx.Response:
    return httpx.Response(
        status,
        request=httpx.Request("POST", "https://api.ouro.foundation/datasets/x/data"),
        content=json.dumps(body or {}).encode(),
        headers={"Content-Type": "application/json"},
    )


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
        ouro.refresh_session.assert_called_once()
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
        ouro.refresh_session.assert_called_once()

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
        ouro.refresh_session.assert_not_called()
        self.assertEqual(raw.post.call_count, 1)

    def test_classifier(self) -> None:
        self.assertTrue(response_needs_auth_retry(_response(401)))
        self.assertTrue(
            response_needs_auth_retry(
                _response(500, {"error": {"message": "No user context"}})
            )
        )
        self.assertFalse(
            response_needs_auth_retry(
                _response(500, {"error": {"message": "insert failed"}})
            )
        )


if __name__ == "__main__":
    unittest.main()
