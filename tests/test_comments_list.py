from __future__ import annotations

import unittest
from types import SimpleNamespace

import httpx

from ouro.models import Comment
from ouro.resources.comments import Comments

PARENT_ID = "00000000-0000-0000-0000-000000000002"


class _FakeResponse:
    status_code = 200
    is_success = True
    headers: dict = {}
    request = httpx.Request("GET", "https://api.example.test")

    def __init__(self, body: dict) -> None:
        self._body = body

    def json(self):
        return self._body


class _FakeClient:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response
        self.requests: list[dict] = []

    def get(self, path: str, params=None):
        self.requests.append({"path": path, "params": params})
        return self._response


class TestCommentsList(unittest.TestCase):
    def test_list_returns_a_page_and_sends_limit_and_offset(self) -> None:
        client = _FakeClient(
            _FakeResponse(
                {
                    "data": [
                        {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "asset_type": "comment",
                            "parent_id": PARENT_ID,
                            "user_id": "00000000-0000-0000-0000-000000000003",
                            "org_id": "00000000-0000-0000-0000-000000000004",
                            "team_id": "00000000-0000-0000-0000-000000000005",
                            "visibility": "public",
                            "created_at": "2026-09-28T12:00:00+00:00",
                            "last_updated": "2026-09-28T12:00:00+00:00",
                        }
                    ],
                    "pagination": {"hasMore": True, "offset": 10, "limit": 1},
                }
            )
        )
        ouro = SimpleNamespace(client=client, websocket=None)

        page = Comments(ouro).list(PARENT_ID, limit=1, offset=10)

        self.assertIsInstance(page[0], Comment)
        self.assertTrue(page.has_more)
        self.assertEqual(
            client.requests,
            [
                {
                    "path": f"/assets/{PARENT_ID}/comments",
                    "params": {"limit": 1, "offset": 10},
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
