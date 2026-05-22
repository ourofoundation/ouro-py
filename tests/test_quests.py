from __future__ import annotations

import unittest
from types import SimpleNamespace

import httpx

from ouro._exceptions import InternalServerError
from ouro.resources.quests import Quests


class _FakeResponse:
    def __init__(self, body: dict, *, status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300
        self.headers = {}
        self.request = httpx.Request("POST", "https://api.example.test")

    def json(self):
        return self._body


class _FakeClient:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self.requests: list[dict] = []

    def get(self, path: str, params=None):
        self.requests.append({"path": path, "params": params})
        return self._responses.pop(0)

    def post(self, path: str, json=None):
        self.requests.append({"path": path, "json": json})
        return self._responses.pop(0)

    def put(self, path: str, json=None):
        self.requests.append({"path": path, "json": json})
        return self._responses.pop(0)


class _FakeOuro:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.client = _FakeClient(responses)
        self.websocket = None
        self.user = SimpleNamespace(
            user_id="00000000-0000-0000-0000-000000000003"
        )

    def _make_status_error(self, err_msg: str, *, body, response, status_override=None):
        return InternalServerError(err_msg, response=response, body=body)


class TestQuestEntries(unittest.TestCase):
    def test_create_entry_returns_entry_model(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "quest_id": "00000000-0000-0000-0000-000000000002",
                            "status": "submitted",
                            "custom": "value",
                        }
                    }
                )
            ]
        )

        entry = Quests(ouro).create_entry(
            "00000000-0000-0000-0000-000000000002",
            item_id="00000000-0000-0000-0000-000000000003",
            asset_id="00000000-0000-0000-0000-000000000004",
            asset_type="dataset",
            description={"text": "done"},
        )

        self.assertEqual(entry.status, "submitted")
        self.assertEqual(entry.get("custom"), "value")
        self.assertEqual(entry["custom"], "value")
        # Backend reads `req.body.entry`, so the SDK must wrap the payload.
        # Regression: an earlier version sent the fields at the top level
        # and the controller silently dropped them.
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": "/quests/00000000-0000-0000-0000-000000000002/entries/create",
                "json": {
                    "entry": {
                        "item_id": "00000000-0000-0000-0000-000000000003",
                        "asset_id": "00000000-0000-0000-0000-000000000004",
                        "asset_type": "dataset",
                        "description": {"text": "done"},
                    }
                },
            },
        )

    def test_list_entries_returns_pagination_envelope(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": [
                            {
                                "id": "00000000-0000-0000-0000-000000000001",
                                "status": "accepted",
                                "eval_status": "passed",
                            }
                        ],
                        "pagination": {"hasMore": False, "limit": 5},
                    }
                )
            ]
        )

        page = Quests(ouro).list_entries(
            "00000000-0000-0000-0000-000000000002",
            status="accepted",
            limit=5,
            offset=10,
            with_pagination=True,
        )

        self.assertEqual(page["data"][0].eval_status, "passed")
        self.assertEqual(page["pagination"]["hasMore"], False)
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": "/quests/00000000-0000-0000-0000-000000000002/entries",
                "params": {"status": "accepted", "limit": 5, "offset": 10},
            },
        )

    def test_review_entry_returns_entry_model(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "status": "accepted",
                        }
                    }
                )
            ]
        )

        entry = Quests(ouro).review_entry(
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000001",
            status="accepted",
            review={"text": "looks good"},
        )

        self.assertEqual(entry.status, "accepted")
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": (
                    "/quests/00000000-0000-0000-0000-000000000002/entries/"
                    "00000000-0000-0000-0000-000000000001/review"
                ),
                "json": {
                    "status": "accepted",
                    "review": {"text": "looks good"},
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
