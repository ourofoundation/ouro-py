"""Clients built from an externally issued access token (e.g. OAuth)."""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

import httpx

from ouro import OuroError
from ouro.client import AutoRefreshClient, Ouro


def _construct(**kwargs) -> tuple[Ouro, MagicMock]:
    with (
        patch.object(Ouro, "exchange_api_key") as exchange,
        patch.object(Ouro, "_bootstrap_authenticated_client"),
        patch("ouro.client.OuroWebSocket", return_value=MagicMock()),
        patch("ouro.client.httpx.Client") as mock_httpx,
    ):
        mock_httpx.return_value = MagicMock(headers={})
        return Ouro(**kwargs), exchange


class AccessTokenClientTests(unittest.TestCase):
    def test_access_token_skips_pat_exchange(self) -> None:
        ouro, exchange = _construct(access_token="jwt-from-oauth")
        exchange.assert_not_called()
        self.assertEqual(ouro.access_token, "jwt-from-oauth")
        self.assertIsNone(ouro.api_key)
        self.assertFalse(ouro.can_refresh)

    def test_api_key_client_can_refresh(self) -> None:
        ouro, exchange = _construct(api_key="pat")
        exchange.assert_called_once()
        self.assertTrue(ouro.can_refresh)

    def test_rejects_both_credentials(self) -> None:
        with self.assertRaises(OuroError):
            _construct(api_key="pat", access_token="jwt")

    def test_refresh_session_raises_without_api_key(self) -> None:
        ouro, _ = _construct(access_token="jwt")
        with self.assertRaises(OuroError):
            ouro.refresh_session()

    def test_401_is_returned_without_retry(self) -> None:
        response = httpx.Response(
            401,
            request=httpx.Request("GET", "https://api.ouro.foundation/user"),
            content=json.dumps({"error": {"message": "expired"}}).encode(),
        )
        raw = MagicMock()
        raw.get.return_value = response
        ouro = MagicMock(can_refresh=False)
        client = AutoRefreshClient(raw, ouro)

        result = client.get("/user")
        self.assertEqual(result.status_code, 401)
        ouro.refresh_session.assert_not_called()
        ouro._token_needs_refresh.assert_not_called()


if __name__ == "__main__":
    unittest.main()
