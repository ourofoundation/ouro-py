from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from ouro import cli
from ouro._exceptions import ExternalServiceError, InternalServerError
from ouro.models.action import Action
from ouro.resources.routes import Routes


ROUTE_ID = "00000000-0000-0000-0000-000000000010"
ACTION_ID = "00000000-0000-0000-0000-000000000001"


def _route_payload(**route) -> dict:
    return {
        "id": ROUTE_ID,
        "user_id": "00000000-0000-0000-0000-000000000011",
        "org_id": "00000000-0000-0000-0000-000000000012",
        "team_id": "00000000-0000-0000-0000-000000000013",
        "parent_id": "00000000-0000-0000-0000-000000000014",
        "asset_type": "route",
        "name": "Predict",
        "visibility": "public",
        "created_at": "2026-01-01T00:00:00+00:00",
        "last_updated": "2026-01-01T00:00:00+00:00",
        "route": {"method": "POST", "path": "/predict", **route},
    }


def _action(status: str, **overrides) -> dict:
    return {
        "id": ACTION_ID,
        "route_id": ROUTE_ID,
        "user_id": "00000000-0000-0000-0000-000000000003",
        "status": status,
        **overrides,
    }


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

    def post(self, path: str, json=None, timeout=None, headers=None):
        self.requests.append(
            {"path": path, "json": json, "timeout": timeout, "headers": headers}
        )
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


class TestRouteActions(unittest.TestCase):
    def test_list_actions_returns_action_models_with_pagination(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000011",
                            "org_id": "00000000-0000-0000-0000-000000000012",
                            "team_id": "00000000-0000-0000-0000-000000000013",
                            "parent_id": "00000000-0000-0000-0000-000000000014",
                            "asset_type": "route",
                            "name": "Predict",
                            "visibility": "public",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "last_updated": "2026-01-01T00:00:00+00:00",
                        }
                    }
                ),
                _FakeResponse(
                    {
                        "data": [
                            {
                                "id": "00000000-0000-0000-0000-000000000001",
                                "route_id": "00000000-0000-0000-0000-000000000002",
                                "user_id": "00000000-0000-0000-0000-000000000003",
                                "status": "success",
                            }
                        ],
                        "pagination": {"hasMore": False},
                    }
                ),
            ]
        )

        page = Routes(ouro).list_actions(
            "00000000-0000-0000-0000-000000000010",
            include_other_users=True,
            status=["queued", "in-progress"],
            limit=5,
        )

        self.assertEqual(page[0].status, "success")
        self.assertFalse(page.has_more)
        self.assertEqual(
            ouro.client.requests[1],
            {
                "path": (
                    "/services/00000000-0000-0000-0000-000000000014/"
                    "routes/00000000-0000-0000-0000-000000000010/actions"
                ),
                "params": {
                    "global": "true",
                    "status": "queued,in-progress",
                    "limit": 5,
                    "offset": 0,
                },
            },
        )

    def test_get_action_logs_sends_server_side_sort_order(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": [
                            {
                                "id": "00000000-0000-0000-0000-000000000002",
                                "action_id": "00000000-0000-0000-0000-000000000001",
                                "level": "info",
                                "message": "first",
                            },
                            {
                                "id": "00000000-0000-0000-0000-000000000003",
                                "action_id": "00000000-0000-0000-0000-000000000001",
                                "level": "info",
                                "message": "second",
                            },
                        ],
                        "pagination": {"hasMore": False},
                    }
                )
            ]
        )

        page = Routes(ouro).get_action_logs(
            "00000000-0000-0000-0000-000000000001",
            chronological=True,
        )

        self.assertEqual([log.message for log in page], ["first", "second"])
        self.assertEqual(
            ouro.client.requests[0]["path"],
            "/actions/00000000-0000-0000-0000-000000000001/logs",
        )
        self.assertEqual(ouro.client.requests[0]["params"]["sort_order"], "asc")

    def test_execute_sends_keyed_input_assets_and_parameters(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000011",
                            "org_id": "00000000-0000-0000-0000-000000000012",
                            "team_id": "00000000-0000-0000-0000-000000000013",
                            "parent_id": "00000000-0000-0000-0000-000000000014",
                            "asset_type": "route",
                            "name": "Predict",
                            "visibility": "public",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "last_updated": "2026-01-01T00:00:00+00:00",
                        }
                    }
                ),
                _FakeResponse(
                    {
                        "data": {"responseData": {"ok": True}},
                        "action": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "route_id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000003",
                            "status": "success",
                        },
                        "metadata": {},
                    }
                ),
            ]
        )

        action = Routes(ouro).execute(
            "00000000-0000-0000-0000-000000000010",
            body={"temperature": 300},
            params={"sample_id": "abc"},
            assets={"structure": "00000000-0000-0000-0000-000000000020"},
        )

        self.assertEqual(action.response, {"ok": True})
        self.assertEqual(
            ouro.client.requests[1]["json"]["config"]["input_assets"],
            {
                "structure": {
                    "assetId": "00000000-0000-0000-0000-000000000020"
                }
            },
        )
        self.assertEqual(
            ouro.client.requests[1]["json"]["config"]["parameters"],
            {"sample_id": "abc"},
        )

    def test_execute_warns_for_caller_side_input_asset_metadata(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000011",
                            "org_id": "00000000-0000-0000-0000-000000000012",
                            "team_id": "00000000-0000-0000-0000-000000000013",
                            "parent_id": "00000000-0000-0000-0000-000000000014",
                            "asset_type": "route",
                            "name": "Predict",
                            "visibility": "public",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "last_updated": "2026-01-01T00:00:00+00:00",
                        }
                    }
                ),
                _FakeResponse(
                    {
                        "data": {"responseData": {"ok": True}},
                        "action": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "route_id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000003",
                            "status": "success",
                        },
                        "metadata": {},
                    }
                ),
            ]
        )

        with self.assertWarnsRegex(
            DeprecationWarning,
            "Declare asset type on the route",
        ):
            Routes(ouro).execute(
                "00000000-0000-0000-0000-000000000010",
                input_assets={
                    "structure": {
                        "assetId": "00000000-0000-0000-0000-000000000020",
                        "assetType": "file",
                    }
                },
            )

        self.assertEqual(
            ouro.client.requests[1]["json"]["config"]["input_assets"],
            {
                "structure": {
                    "assetId": "00000000-0000-0000-0000-000000000020",
                    "assetType": "file",
                }
            },
        )

    def test_action_final_data_includes_keyed_output_assets(self) -> None:
        action = Action(
            id="00000000-0000-0000-0000-000000000001",
            route_id="00000000-0000-0000-0000-000000000010",
            user_id="00000000-0000-0000-0000-000000000003",
            status="success",
            response={"ok": True},
            output_assets=[
                {
                    "name": "report",
                    "asset": {
                        "id": "00000000-0000-0000-0000-000000000020",
                        "asset_type": "post",
                    },
                }
            ],
        )

        self.assertEqual(
            action.final_data,
            {
                "ok": True,
                "report": {
                    "id": "00000000-0000-0000-0000-000000000020",
                    "asset_type": "post",
                },
            },
        )

    def test_execute_preserves_failed_route_action_envelope(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000011",
                            "org_id": "00000000-0000-0000-0000-000000000012",
                            "team_id": "00000000-0000-0000-0000-000000000013",
                            "parent_id": "00000000-0000-0000-0000-000000000014",
                            "asset_type": "route",
                            "name": "Predict",
                            "visibility": "public",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "last_updated": "2026-01-01T00:00:00+00:00",
                        }
                    }
                ),
                _FakeResponse(
                    {
                        "data": None,
                        "action": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "route_id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000003",
                            "status": "error",
                            "response": {
                                "error": {
                                    "message": "Service unavailable",
                                    "status": 503,
                                }
                            },
                        },
                        "metadata": None,
                        "error": {
                            "message": "Service unavailable",
                            "status": 503,
                        },
                    },
                    status_code=503,
                ),
            ]
        )

        action = Routes(ouro).execute("00000000-0000-0000-0000-000000000010")

        self.assertEqual(action.status, "error")
        self.assertEqual(
            action.response,
            {"error": {"message": "Service unavailable", "status": 503}},
        )

    def test_execute_fills_minimal_failed_route_action_envelope(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000011",
                            "org_id": "00000000-0000-0000-0000-000000000012",
                            "team_id": "00000000-0000-0000-0000-000000000013",
                            "parent_id": "00000000-0000-0000-0000-000000000014",
                            "asset_type": "route",
                            "name": "Predict",
                            "visibility": "public",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "last_updated": "2026-01-01T00:00:00+00:00",
                        }
                    }
                ),
                _FakeResponse(
                    {
                        "data": None,
                        "action": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "status": "error",
                            "response": {
                                "error": {"message": "Not Found", "status": 404}
                            },
                        },
                        "metadata": None,
                        "error": {"message": "Not Found", "status": 404},
                    },
                    status_code=404,
                ),
            ]
        )

        action = Routes(ouro).execute("00000000-0000-0000-0000-000000000010")

        self.assertEqual(
            str(action.route_id), "00000000-0000-0000-0000-000000000010"
        )
        self.assertEqual(
            str(action.user_id), "00000000-0000-0000-0000-000000000003"
        )
        self.assertEqual(action.status, "error")

    def test_poll_action_raises_external_service_error(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "route_id": "00000000-0000-0000-0000-000000000010",
                            "user_id": "00000000-0000-0000-0000-000000000003",
                            "status": "error",
                            "response": {
                                "statusCode": 503,
                                "error": {
                                    "type": "external_service_error",
                                    "code": "external_service_error",
                                    "message": "Service unavailable",
                                    "status": 503,
                                    "retryable": True,
                                },
                            },
                        }
                    }
                )
            ]
        )

        with self.assertRaises(ExternalServiceError) as ctx:
            Routes(ouro).poll_action("00000000-0000-0000-0000-000000000001")

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertTrue(ctx.exception.retryable)

    def test_execute_async_route_takes_handle_first_then_polls(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse({"data": _route_payload(execution_mode="async")}),
                _FakeResponse(
                    {"data": None, "action": _action("in-progress"), "metadata": {}},
                    status_code=202,
                ),
                _FakeResponse({"data": _action("in-progress")}),
                _FakeResponse({"data": _action("success", response={"ok": True})}),
            ]
        )

        with patch("ouro.resources.routes.time.sleep") as sleep:
            action = Routes(ouro).execute(ROUTE_ID, poll_interval=5.0)

        self.assertEqual(action.status, "success")
        self.assertEqual(
            ouro.client.requests[1]["headers"], {"Prefer": "respond-async"}
        )
        # First check comes quickly rather than a full interval later
        self.assertEqual(sleep.call_args_list[0].args, (0.5,))

    def test_execute_sends_notify_level(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse({"data": _route_payload(execution_mode="async")}),
                _FakeResponse(
                    {"data": None, "action": _action("in-progress"), "metadata": {}},
                    status_code=202,
                ),
            ]
        )

        Routes(ouro).execute(ROUTE_ID, wait=False, notify="none")

        self.assertEqual(
            ouro.client.requests[1]["headers"],
            {"Prefer": "respond-async", "Ouro-Notify": "none"},
        )
        with self.assertRaises(ValueError):
            Routes(ouro).execute(ROUTE_ID, notify="quiet")

    def test_execute_sync_route_waits_inline(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse({"data": _route_payload(execution_mode="sync")}),
                _FakeResponse(
                    {
                        "data": {"responseData": {"ok": True}},
                        "action": _action("success"),
                        "metadata": {},
                    }
                ),
            ]
        )

        action = Routes(ouro).execute(ROUTE_ID)

        self.assertEqual(action.response, {"ok": True})
        self.assertEqual(ouro.client.requests[1]["headers"], {})

    def test_poll_timeout_carries_action_id(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": _action("in-progress")})])

        with self.assertRaises(TimeoutError) as ctx:
            Routes(ouro).poll_action(ACTION_ID, timeout=0)

        self.assertEqual(ctx.exception.action_id, ACTION_ID)

    def test_list_my_actions_filters_by_status(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": [_action("in-progress", source="mcp")],
                        "pagination": {"hasMore": True},
                    }
                )
            ]
        )

        page = Routes(ouro).list_my_actions(
            status=["queued", "in-progress"], limit=5
        )

        self.assertEqual(page[0].status, "in-progress")
        self.assertTrue(page.has_more)
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": "/actions",
                "params": {"status": "queued,in-progress", "limit": 5, "offset": 0},
            },
        )


class TestActionCli(unittest.TestCase):
    def _run(self, responses: list[_FakeResponse], argv: list[str]):
        ouro = _FakeOuro(responses)
        ouro.routes = Routes(ouro)
        out = io.StringIO()
        with patch("ouro.Ouro", return_value=ouro), redirect_stdout(out), patch(
            "ouro.resources.routes.time.sleep"
        ):
            code = cli.main(argv)
        return code, out.getvalue()

    def test_wait_exits_zero_on_success(self) -> None:
        code, out = self._run(
            [
                _FakeResponse({"data": _action("in-progress")}),
                _FakeResponse(
                    {
                        "data": _action(
                            "success",
                            route={"name": "Relax"},
                            output_assets=[
                                {
                                    "name": "report",
                                    "asset": {"id": "abc", "asset_type": "post"},
                                }
                            ],
                        )
                    }
                ),
            ],
            ["action", "wait", ACTION_ID],
        )

        self.assertEqual(code, 0)
        self.assertIn("success  Relax", out)
        self.assertIn("output: report  post  abc", out)

    def test_wait_exits_one_on_error(self) -> None:
        code, out = self._run(
            [
                _FakeResponse(
                    {
                        "data": _action(
                            "error", response={"error": {"message": "boom"}}
                        )
                    }
                )
            ],
            ["action", "wait", ACTION_ID, "--json"],
        )

        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["error"], "boom")

    def test_wait_timeout_exits_124(self) -> None:
        code, out = self._run(
            [
                _FakeResponse({"data": _action("in-progress")}),
                _FakeResponse({"data": _action("in-progress")}),
            ],
            ["action", "wait", ACTION_ID, "--timeout", "0"],
        )

        self.assertEqual(code, 124)
        self.assertIn("in-progress", out)


if __name__ == "__main__":
    unittest.main()
