"""Unit tests for X-Ouro-Client / User-Agent identity."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from ouro.client import Ouro


class ClientIdentityTests(unittest.TestCase):
    def _construct(self, **kwargs) -> tuple[Ouro, dict]:
        """Build an Ouro instance without hitting the network."""
        with (
            patch.object(Ouro, "exchange_api_key"),
            patch.object(Ouro, "_bootstrap_authenticated_client"),
            patch("ouro.client.OuroWebSocket", return_value=MagicMock()),
            patch("ouro.client.httpx.Client") as mock_httpx,
        ):
            mock_httpx.return_value = MagicMock(headers={})
            ouro = Ouro(api_key="test-key", **kwargs)
            init_headers = mock_httpx.call_args.kwargs["headers"]
            return ouro, init_headers

    def test_direct_sdk_sets_matching_headers(self) -> None:
        with patch("ouro.client.__version__", "9.9.9"):
            ouro, headers = self._construct()
        self.assertEqual(ouro._user_agent, "ouro-py/9.9.9")
        self.assertEqual(ouro._ouro_client, "ouro-py/9.9.9")
        self.assertEqual(headers["User-Agent"], "ouro-py/9.9.9")
        self.assertEqual(headers["X-Ouro-Client"], "ouro-py/9.9.9")

    def test_wrapper_keeps_layers_separate(self) -> None:
        with patch("ouro.client.__version__", "9.9.9"):
            ouro, headers = self._construct(client="ouro-mcp/0.7.10")
        self.assertEqual(ouro._user_agent, "ouro-py/9.9.9")
        self.assertEqual(ouro._ouro_client, "ouro-mcp/0.7.10")
        self.assertEqual(headers["User-Agent"], "ouro-py/9.9.9")
        self.assertEqual(headers["X-Ouro-Client"], "ouro-mcp/0.7.10")

    def test_blank_client_falls_back_to_py(self) -> None:
        with patch("ouro.client.__version__", "9.9.9"):
            ouro, _ = self._construct(client="  ")
        self.assertEqual(ouro._ouro_client, "ouro-py/9.9.9")


if __name__ == "__main__":
    unittest.main()
