from __future__ import annotations

import unittest
from types import SimpleNamespace
from uuid import UUID

import httpx
from ouro._exceptions import InternalServerError
from ouro.models.action import Action
from ouro.resources.assets import Assets


ASSET_ID = "00000000-0000-0000-0000-000000000099"
ACTION_ID = "00000000-0000-0000-0000-000000000001"
ROUTE_ID = "00000000-0000-0000-0000-000000000002"
USER_ID = "00000000-0000-0000-0000-000000000003"


def _action_payload(**overrides):
    base = {
        "id": ACTION_ID,
        "route_id": ROUTE_ID,
        "user_id": USER_ID,
        "status": "success",
    }
    base.update(overrides)
    return base


class _FakeResponse:
    def __init__(self, body: dict, *, status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300
        self.headers = {}
        self.request = httpx.Request("GET", "https://api.example.test")

    def json(self):
        return self._body


class _FakeClient:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self.requests: list[dict] = []

    def get(self, path: str, params=None):
        self.requests.append({"path": path, "params": params})
        return self._responses.pop(0)


class _FakeOuro:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.client = _FakeClient(responses)
        self.websocket = None
        self.user = SimpleNamespace(user_id=USER_ID)

    def _make_status_error(self, err_msg: str, *, body, response, status_override=None):
        return InternalServerError(err_msg, response=response, body=body)


class TestAssetActions(unittest.TestCase):
    def test_actions_role_both_auto_paginates(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": {
                            "created_by": _action_payload(id=ACTION_ID),
                            "as_input": [
                                _action_payload(
                                    id="00000000-0000-0000-0000-000000000004"
                                )
                            ],
                        },
                        "pagination": {
                            "offset": 0,
                            "limit": 200,
                            "hasMore": False,
                        },
                    }
                )
            ]
        )
        result = Assets(ouro).actions(ASSET_ID)

        self.assertIsInstance(result["created_by"], Action)
        self.assertEqual(result["created_by"].id, UUID(ACTION_ID))
        self.assertEqual(len(result["as_input"]), 1)
        self.assertEqual(
            result["as_input"][0].id,
            UUID("00000000-0000-0000-0000-000000000004"),
        )
        self.assertEqual(
            ouro.client.requests[0],
            {
                "path": f"/assets/{ASSET_ID}/actions",
                "params": {
                    "role": "both",
                    "include_response": "false",
                    "limit": 200,
                    "offset": 0,
                },
            },
        )

    def test_actions_role_input_single_page(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": [_action_payload()],
                        "pagination": {
                            "offset": 0,
                            "limit": 20,
                            "hasMore": False,
                        },
                    }
                )
            ]
        )
        result = Assets(ouro).actions(
            ASSET_ID, role="input", status="success", limit=20
        )

        self.assertIsNone(result["created_by"])
        self.assertEqual(len(result["as_input"]), 1)
        self.assertEqual(
            ouro.client.requests[0]["params"],
            {
                "role": "input",
                "status": "success",
                "include_response": "false",
                "limit": 20,
                "offset": 0,
            },
        )

    def test_actions_include_response(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse(
                    {
                        "data": [_action_payload(response={"e_above_hull": 0.1})],
                        "pagination": {"hasMore": False},
                    }
                )
            ]
        )
        result = Assets(ouro).actions(
            ASSET_ID, role="input", include_response=True, limit=50
        )
        self.assertEqual(len(result["as_input"]), 1)
        self.assertEqual(
            ouro.client.requests[0]["params"]["include_response"], "true"
        )

    def test_actions_role_output_wraps_singular(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": _action_payload()})])
        result = Assets(ouro).actions(ASSET_ID, role="output")

        self.assertIsInstance(result["created_by"], Action)
        self.assertEqual(result["as_input"], [])
        self.assertEqual(
            ouro.client.requests[0]["params"],
            {"role": "output", "include_response": "false"},
        )

    def test_actions_role_output_null(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": None})])
        result = Assets(ouro).actions(ASSET_ID, role="output")

        self.assertIsNone(result["created_by"])
        self.assertEqual(result["as_input"], [])

    def test_creation_actions_removed(self) -> None:
        self.assertFalse(hasattr(Assets, "creation_actions"))

    def test_actions_rejects_invalid_role(self) -> None:
        ouro = _FakeOuro([])
        with self.assertRaises(ValueError):
            Assets(ouro).actions(ASSET_ID, role="sideways")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
