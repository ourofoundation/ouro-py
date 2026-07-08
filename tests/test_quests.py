from __future__ import annotations

import unittest
from types import SimpleNamespace

import httpx

from ouro._exceptions import ConflictError, InternalServerError
from ouro.models import Entry, Quest, QuestItem
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
        status = status_override if status_override is not None else response.status_code
        if status == 409:
            return ConflictError(err_msg, response=response, body=body)
        return InternalServerError(err_msg, response=response, body=body)


class TestQuestItems(unittest.TestCase):
    def test_quest_item_preserves_waiting_metadata(self) -> None:
        item = QuestItem(
            id="00000000-0000-0000-0000-000000000001",
            quest_id="00000000-0000-0000-0000-000000000002",
            description="Wait for reply",
            status="in_progress",
            waiting_on="reply from authors",
            waiting_until="2026-07-10T00:00:00Z",
            waiting_check_every="1d",
        )
        self.assertEqual(item.waiting_on, "reply from authors")
        self.assertEqual(item.waiting_until, "2026-07-10T00:00:00Z")
        self.assertEqual(item.waiting_check_every, "1d")
        dumped = item.model_dump(mode="json")
        self.assertEqual(dumped["waiting_on"], "reply from authors")
        self.assertEqual(dumped["waiting_until"], "2026-07-10T00:00:00Z")
        self.assertEqual(dumped["waiting_check_every"], "1d")

    def test_retrieve_preserves_waiting_metadata_on_nested_items(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000010",
                            "name": "Owned quest",
                            "user_id": "00000000-0000-0000-0000-000000000003",
                            "org_id": "00000000-0000-0000-0000-000000000011",
                            "team_id": "00000000-0000-0000-0000-000000000012",
                            "visibility": "private",
                            "asset_type": "quest",
                            "created_at": "2026-07-01T00:00:00Z",
                            "last_updated": "2026-07-01T00:00:00Z",
                            "items": [
                                {
                                    "id": "00000000-0000-0000-0000-000000000001",
                                    "quest_id": "00000000-0000-0000-0000-000000000010",
                                    "description": "Wait for reply",
                                    "status": "in_progress",
                                    "waiting_on": "reply from authors",
                                    "waiting_until": "2026-07-10T00:00:00Z",
                                    "waiting_check_every": "1d",
                                }
                            ],
                        }
                    }
                )
            ]
        )

        quest = Quests(ouro).retrieve("00000000-0000-0000-0000-000000000010")

        self.assertIsInstance(quest, Quest)
        self.assertEqual(len(quest.items or []), 1)
        item = quest.items[0]
        self.assertIsInstance(item, QuestItem)
        self.assertEqual(item.waiting_on, "reply from authors")
        self.assertEqual(item.waiting_until, "2026-07-10T00:00:00Z")
        self.assertEqual(item.waiting_check_every, "1d")


class TestQuestEntries(unittest.TestCase):
    def test_list_assigned_items_calls_assigned_items_endpoint(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": [
                            {
                                "id": "00000000-0000-0000-0000-000000000001",
                                "quest_id": "00000000-0000-0000-0000-000000000002",
                                "description": "Do the assigned work",
                            }
                        ],
                        "pagination": {"hasMore": False},
                    }
                )
            ]
        )

        result = Quests(ouro).list_assigned_items(
            status=["pending", "in_progress"],
            org_id="org-1",
            team_id="team-1",
            limit=5,
            offset=10,
        )

        self.assertEqual(result[0]["description"], "Do the assigned work")
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": "/quests/assigned-items",
                "params": {
                    "limit": 5,
                    "offset": 10,
                    "status": "pending,in_progress",
                    "org_id": "org-1",
                    "team_id": "team-1",
                },
            },
        )

    def test_entry_model_accepts_empty_assets_list(self) -> None:
        entry = Entry(id="00000000-0000-0000-0000-000000000001", assets=[])
        self.assertIsNone(entry.assets)

    def test_entry_model_preserves_keyed_submission_assets(self) -> None:
        entry = Entry(
            id="00000000-0000-0000-0000-000000000001",
            assets={
                "file": {
                    "asset_id": "00000000-0000-0000-0000-000000000004",
                    "asset_type": "file",
                }
            },
            embedded_assets=[{"id": "00000000-0000-0000-0000-000000000005"}],
        )
        self.assertEqual(entry.assets["file"]["asset_type"], "file")
        self.assertEqual(len(entry.embedded_assets), 1)

    def test_create_entry_passes_assets_through_to_api(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "status": "submitted",
                        }
                    }
                )
            ]
        )

        Quests(ouro).create_entry(
            "00000000-0000-0000-0000-000000000002",
            item_id="00000000-0000-0000-0000-000000000003",
            assets={"file": "00000000-0000-0000-0000-000000000004"},
        )

        self.assertEqual(
            ouro.client.requests[0]["json"]["entry"]["assets"],
            {"file": "00000000-0000-0000-0000-000000000004"},
        )

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
            assets={"artifact": "00000000-0000-0000-0000-000000000004"},
            description={"text": "done"},
        )

        self.assertEqual(entry.status, "submitted")
        self.assertEqual(entry.get("custom"), "value")
        self.assertEqual(entry["custom"], "value")
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": "/quests/00000000-0000-0000-0000-000000000002/entries/create",
                "json": {
                    "entry": {
                        "item_id": "00000000-0000-0000-0000-000000000003",
                        "assets": {"artifact": "00000000-0000-0000-0000-000000000004"},
                        "description": {"text": "done"},
                    }
                },
            },
        )

    def test_create_entry_surfaces_draft_quest_conflict(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": None,
                        "error": {
                            "message": (
                                "Quest is still a draft and is not accepting entries"
                            ),
                            "status": 409,
                            "code": "quest_draft_not_accepting_entries",
                        },
                    },
                    status_code=409,
                )
            ]
        )

        with self.assertRaises(ConflictError) as ctx:
            Quests(ouro).create_entry(
                "00000000-0000-0000-0000-000000000002",
                item_id="00000000-0000-0000-0000-000000000003",
                description={"text": "done"},
            )

        self.assertIn("draft", str(ctx.exception))

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
                                "assets": {
                                    "file": {
                                        "asset_id": "00000000-0000-0000-0000-000000000004",
                                        "asset_type": "file",
                                    }
                                },
                                "embedded_assets": [],
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
        self.assertEqual(page["data"][0].assets["file"]["asset_type"], "file")
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
