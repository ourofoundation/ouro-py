from __future__ import annotations

from types import SimpleNamespace

import httpx

from ouro.resources.assets import Assets


class _FakeClient:
    def __init__(self) -> None:
        self.gets: list[tuple[str, dict]] = []

    def get(self, path: str, params=None):
        self.gets.append((path, params))
        return httpx.Response(
            200,
            json={"data": [], "pagination": {"hasMore": False}, "error": None},
            request=httpx.Request("GET", "https://api.example.test" + path),
        )


def test_connections_pass_type_and_direction() -> None:
    client = _FakeClient()
    assets = Assets(SimpleNamespace(client=client, websocket=None))

    assets.connections("a1", type=["reference", "link"], direction="target", limit=10)

    assert client.gets == [
        ("/assets/a1/connections", {"limit": 10, "type": "reference,link", "direction": "target"})
    ]
