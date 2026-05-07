"""Tests for SyncAPIResource._handle_response status mapping.

Covers two cases that previously masked typed exceptions:

1. The HTTP response is a real non-2xx (e.g. 404). Backed by the real
   ``client._make_status_error`` mapping, this should already raise
   ``NotFoundError`` — guarded here against regressions.

2. The HTTP response is 200 OK but the body envelope says ``error.status:
   404``. Some Ouro endpoints historically return 200 with the semantic
   status only in the body, which had bypassed typed dispatch and
   surfaced as a generic ``APIStatusError``. ``_envelope_status`` now
   forces the typed dispatch.
"""

from __future__ import annotations

import unittest

import httpx

from ouro._exceptions import (
    APIStatusError,
    NotFoundError,
    PermissionDeniedError,
)
from ouro._resource import SyncAPIResource
from ouro.client import Ouro


class _FakeWS:
    pass


def _make_resource(client: httpx.Client | None = None) -> SyncAPIResource:
    """Build a SyncAPIResource with a real Ouro instance (no auth)."""
    ouro = Ouro.__new__(Ouro)
    ouro.client = client or object()
    ouro.websocket = _FakeWS()
    return SyncAPIResource(ouro)


def _response(body: object, *, status_code: int = 200) -> httpx.Response:
    request = httpx.Request("PUT", "https://api.example.test/posts/abc")
    return httpx.Response(status_code=status_code, json=body, request=request)


class TestEnvelopeStatusMapping(unittest.TestCase):
    def test_404_http_status_raises_not_found(self) -> None:
        resource = _make_resource()
        body = {"data": None, "error": "Asset not found"}
        with self.assertRaises(NotFoundError):
            resource._handle_response(_response(body, status_code=404))

    def test_200_with_envelope_status_404_raises_not_found(self) -> None:
        resource = _make_resource()
        body = {
            "data": None,
            "error": {
                "name": "HttpError",
                "message": "Asset abc not found",
                "status": 404,
                "code": "asset_not_found",
            },
        }
        with self.assertRaises(NotFoundError) as ctx:
            resource._handle_response(_response(body, status_code=200))
        self.assertIn("not found", str(ctx.exception).lower())

    def test_200_with_envelope_status_403_raises_permission_denied(self) -> None:
        resource = _make_resource()
        body = {
            "data": None,
            "error": {
                "name": "HttpError",
                "message": "You don't have permission to update this asset",
                "status": 403,
                "code": "update_forbidden",
            },
        }
        with self.assertRaises(PermissionDeniedError):
            resource._handle_response(_response(body, status_code=200))

    def test_200_with_envelope_statusCode_string_is_coerced(self) -> None:
        resource = _make_resource()
        body = {
            "data": None,
            "error": {"message": "Forbidden", "statusCode": "403"},
        }
        with self.assertRaises(PermissionDeniedError):
            resource._handle_response(_response(body, status_code=200))

    def test_200_with_unstructured_error_falls_back_to_generic(self) -> None:
        resource = _make_resource()
        # No status hint in the envelope — the dispatcher should produce a
        # generic APIStatusError (status=200), not promote it to a typed
        # 4xx exception.
        body = {"data": None, "error": "Something exploded"}
        with self.assertRaises(APIStatusError) as ctx:
            resource._handle_response(_response(body, status_code=200))
        self.assertNotIsInstance(ctx.exception, NotFoundError)
        self.assertNotIsInstance(ctx.exception, PermissionDeniedError)

    def test_200_with_no_error_returns_data(self) -> None:
        resource = _make_resource()
        body = {"data": {"id": "abc", "name": "ok"}, "error": None}
        result = resource._handle_response(_response(body, status_code=200))
        self.assertEqual(result, {"id": "abc", "name": "ok"})

    def test_envelope_status_helper_handles_non_dict_inputs(self) -> None:
        self.assertIsNone(SyncAPIResource._envelope_status(None))
        self.assertIsNone(SyncAPIResource._envelope_status("not a dict"))
        self.assertIsNone(SyncAPIResource._envelope_status({"error": "string"}))
        self.assertIsNone(SyncAPIResource._envelope_status({"error": {"foo": 1}}))
        self.assertEqual(
            SyncAPIResource._envelope_status({"error": {"status": 404}}),
            404,
        )


if __name__ == "__main__":
    unittest.main()
