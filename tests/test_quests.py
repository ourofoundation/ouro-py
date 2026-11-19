from __future__ import annotations

import json
import unittest
from types import SimpleNamespace

import httpx

from ouro._exceptions import BadRequestError, ConflictError, InternalServerError
from ouro.models import Entry, EvaluationRun, Quest, QuestItem
from ouro.resources.content import Content
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
        if status == 400:
            return BadRequestError(err_msg, response=response, body=body)
        if status == 409:
            return ConflictError(err_msg, response=response, body=body)
        return InternalServerError(err_msg, response=response, body=body)


QUEST_ID = "00000000-0000-0000-0000-000000000002"
ITEM_ID = "00000000-0000-0000-0000-000000000003"
ENTRY_ID = "00000000-0000-0000-0000-000000000001"
RUN_ID = "00000000-0000-0000-0000-000000000020"
EVALUATOR_ID = "00000000-0000-0000-0000-000000000021"
HULL_ROUTE_ID = "00000000-0000-0000-0000-000000000030"
MAE_ROUTE_ID = "00000000-0000-0000-0000-000000000031"
HULL_ACTION_ID = "00000000-0000-0000-0000-000000000040"
STRUCTURE_ID = "00000000-0000-0000-0000-000000000004"

EVALUATOR = {
    "steps": {
        "hull": {
            "route_id": HULL_ROUTE_ID,
            "inputs": {"structure": "inputs.structure"},
        },
        "mae": {
            "route_id": MAE_ROUTE_ID,
            "inputs": {"structure": "hull.relaxed"},
            "after": ["stable"],
        },
    },
    "gates": {"stable": "hull.e_above_hull <= 0.075"},
    "categories": {"anisotropy": "min(mae.k1, 1)"},
    "score": "100 * anisotropy",
    "pass": {"min": 50},
}


def _run(**overrides) -> dict:
    return {
        "id": RUN_ID,
        "evaluator_id": EVALUATOR_ID,
        "quest_id": QUEST_ID,
        "item_id": ITEM_ID,
        "entry_id": None,
        "user_id": "00000000-0000-0000-0000-000000000003",
        "purpose": "test",
        "status": "running",
        "steps": {
            "hull": {
                "status": "success",
                "route_id": HULL_ROUTE_ID,
                "action_id": HULL_ACTION_ID,
            },
            "mae": {"status": "running", "route_id": MAE_ROUTE_ID},
        },
        "score": None,
        "passed": None,
        "gates": {"stable": True},
        "categories": None,
        "error": None,
        "created_at": "2026-11-18T00:00:00Z",
        "finished_at": None,
        **overrides,
    }


class TestQuestEvaluators(unittest.TestCase):
    def test_item_carries_its_evaluator(self) -> None:
        item = QuestItem(
            id=ITEM_ID,
            evaluator_id=EVALUATOR_ID,
            evaluator=EVALUATOR,
            eval_route_id=None,
        )
        self.assertEqual(str(item.evaluator_id), EVALUATOR_ID)
        self.assertEqual(item.evaluator["steps"]["mae"]["after"], ["stable"])
        self.assertIsNone(item.eval_route_id)

    def test_create_items_passes_evaluator_through(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": []})])

        Quests(ouro).create_items(
            QUEST_ID,
            [{"description": "Find a stable magnet", "evaluator": EVALUATOR}],
        )

        sent = ouro.client.requests[0]["json"]["items"][0]
        self.assertEqual(sent["evaluator"], EVALUATOR)

    def test_update_item_sends_evaluator(self) -> None:
        ouro = _FakeOuro(
            [_FakeResponse({"data": {"id": ITEM_ID, "evaluator": EVALUATOR}})]
        )

        item = Quests(ouro).update_item(QUEST_ID, ITEM_ID, evaluator=EVALUATOR)

        self.assertEqual(item.evaluator, EVALUATOR)
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": f"/quests/{QUEST_ID}/items/{ITEM_ID}",
                "json": {"item": {"evaluator": EVALUATOR}},
            },
        )

    def test_update_item_sends_null_to_remove_the_evaluator(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": {"id": ITEM_ID}})])

        Quests(ouro).update_item(QUEST_ID, ITEM_ID, evaluator=None)

        self.assertEqual(
            ouro.client.requests[0]["json"], {"item": {"evaluator": None}}
        )

    def test_update_item_leaves_the_evaluator_alone_by_default(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": {"id": ITEM_ID}})])

        Quests(ouro).update_item(QUEST_ID, ITEM_ID, notes="checked", status=None)

        self.assertEqual(
            ouro.client.requests[0]["json"], {"item": {"notes": "checked"}}
        )

    def test_invalid_evaluator_raises_bad_request_with_problems(self) -> None:
        problems = ['Step "mae" waits on "stable", which is not a gate']
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": None,
                        "error": {
                            "message": problems[0],
                            "status": 400,
                            "code": "invalid_evaluator",
                            "details": {"problems": problems},
                        },
                    },
                    status_code=400,
                )
            ]
        )

        with self.assertRaises(BadRequestError) as ctx:
            Quests(ouro).update_item(QUEST_ID, ITEM_ID, evaluator={"steps": {}})

        self.assertEqual(ctx.exception.code, "invalid_evaluator")
        self.assertIn("not a gate", str(ctx.exception))
        self.assertEqual(
            ctx.exception.body["error"]["details"]["problems"], problems
        )

    def test_evaluate_item_runs_a_trial_and_returns_the_run(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": _run()})])

        run = Quests(ouro).evaluate_item(
            QUEST_ID, ITEM_ID, {"structure": STRUCTURE_ID}
        )

        self.assertIsInstance(run, EvaluationRun)
        self.assertEqual(run.purpose, "test")
        self.assertIsNone(run.entry_id)
        self.assertEqual(run.status, "running")
        self.assertEqual(run.steps["hull"].status, "success")
        self.assertEqual(str(run.steps["hull"].action_id), HULL_ACTION_ID)
        self.assertIsNone(run.steps["mae"].action_id)
        self.assertEqual(run.gates, {"stable": True})
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": f"/quests/{QUEST_ID}/items/{ITEM_ID}/evaluate",
                "json": {"assets": {"structure": STRUCTURE_ID}},
            },
        )

    def test_get_evaluation_reads_one_run(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": _run(
                            status="completed",
                            score=72.5,
                            passed=True,
                            categories={"anisotropy": 0.725},
                            finished_at="2026-11-18T00:05:00Z",
                        )
                    }
                )
            ]
        )

        run = Quests(ouro).get_evaluation(QUEST_ID, RUN_ID)

        self.assertEqual(run.status, "completed")
        self.assertEqual(run.score, 72.5)
        self.assertTrue(run.passed)
        self.assertEqual(run.categories, {"anisotropy": 0.725})
        self.assertEqual(
            ouro.client.requests[0],
            {"path": f"/quests/{QUEST_ID}/evaluations/{RUN_ID}", "params": None},
        )

    def test_rescore_item_returns_started_runs(self) -> None:
        body = {
            "started": [{"entry_id": ENTRY_ID, "run_id": RUN_ID}],
            "skipped": [],
        }
        ouro = _FakeOuro([_FakeResponse({"data": body})])

        result = Quests(ouro).rescore_item(QUEST_ID, ITEM_ID)

        self.assertEqual(result, body)
        self.assertEqual(
            ouro.client.requests[0],
            {"path": f"/quests/{QUEST_ID}/items/{ITEM_ID}/rescore", "json": {}},
        )

    def test_evaluate_entry_returns_the_entry_with_its_run(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": ENTRY_ID,
                            "status": "submitted",
                            "eval_status": "running",
                            "eval_run_id": RUN_ID,
                            "eval_run": _run(purpose="entry", entry_id=ENTRY_ID),
                        }
                    }
                )
            ]
        )

        entry = Quests(ouro).evaluate_entry(QUEST_ID, ENTRY_ID)

        self.assertEqual(str(entry.eval_run_id), RUN_ID)
        self.assertIsInstance(entry.eval_run, EvaluationRun)
        self.assertEqual(
            str(entry.eval_run.steps["hull"].action_id), HULL_ACTION_ID
        )
        self.assertEqual(
            ouro.client.requests[0]["path"],
            f"/quests/{QUEST_ID}/entries/{ENTRY_ID}/evaluate",
        )


class TestQuestItems(unittest.TestCase):
    def test_create_items_unwraps_item_json_from_content(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": []})])
        item = {
            "description": Content(
                text=json.dumps(
                    {
                        "description": "Measure Curie temperature",
                        "expected_asset_type": "file",
                        "sort_order": 1,
                    }
                )
            )
        }

        Quests(ouro).create_items(
            "00000000-0000-0000-0000-000000000002",
            [item],
        )

        sent = ouro.client.requests[0]["json"]["items"][0]
        self.assertEqual(sent["description"], "Measure Curie temperature")
        self.assertEqual(sent["expected_asset_type"], "file")
        self.assertEqual(sent["sort_order"], 1)

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

    def test_quest_item_accepts_tiptap_content_description(self) -> None:
        content = {
            "text": "Link the dataset",
            "json": {
                "type": "doc",
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": "Link the dataset"}],
                    }
                ],
            },
        }
        item = QuestItem(
            id="00000000-0000-0000-0000-000000000001",
            quest_id="00000000-0000-0000-0000-000000000002",
            description=content,
            status="pending",
        )
        self.assertEqual(item.description.text, "Link the dataset")
        self.assertEqual(item.description.data["type"], "doc")

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
                                    "description": {
                                        "text": "Wait for reply",
                                        "json": {
                                            "type": "doc",
                                            "content": [
                                                {
                                                    "type": "paragraph",
                                                    "content": [
                                                        {
                                                            "type": "text",
                                                            "text": "Wait for reply",
                                                        }
                                                    ],
                                                }
                                            ],
                                        },
                                    },
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

        self.assertEqual(result[0].description.text, "Do the assigned work")
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
        self.assertEqual(entry.custom, "value")
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
        )

        self.assertEqual(page[0].eval_status, "passed")
        self.assertEqual(page[0].assets["file"]["asset_type"], "file")
        self.assertFalse(page.has_more)
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


class TestQuestLeaderboard(unittest.TestCase):
    def test_list_leaderboard_returns_ranked_rows(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": [
                            {
                                "placement": 1,
                                "entry_id": "00000000-0000-0000-0000-000000000001",
                                "score": 0.91,
                                "status": "accepted",
                                "eval_status": "passed",
                                "eval_run_id": RUN_ID,
                                "category_scores": {
                                    "accuracy": 0.95,
                                    "completeness": 0.8,
                                },
                            }
                        ],
                        "pagination": {"hasMore": False, "limit": 50},
                        "item": {
                            "id": "00000000-0000-0000-0000-000000000003",
                            "leaderboard_order": "desc",
                            "evaluator_id": EVALUATOR_ID,
                            "evaluator": EVALUATOR,
                        },
                    }
                )
            ]
        )

        page = Quests(ouro).list_leaderboard(
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
            limit=50,
            offset=0,
        )

        self.assertEqual(page[0].placement, 1)
        self.assertEqual(page[0].score, 0.91)
        self.assertEqual(
            page[0].category_scores,
            {"accuracy": 0.95, "completeness": 0.8},
        )
        self.assertEqual(str(page[0].eval_run_id), RUN_ID)
        self.assertEqual(page.item.leaderboard_order, "desc")
        self.assertEqual(page.item.evaluator["score"], "100 * anisotropy")
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": (
                    "/quests/00000000-0000-0000-0000-000000000002/items/"
                    "00000000-0000-0000-0000-000000000003/leaderboard"
                ),
                "params": {"limit": 50, "offset": 0},
            },
        )


if __name__ == "__main__":
    unittest.main()
