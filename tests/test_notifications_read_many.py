from __future__ import annotations

from types import SimpleNamespace

import httpx

from ouro.resources.notifications import Notifications


class _FakeClient:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict]] = []

    def post(self, path: str, json=None):
        self.posts.append((path, json))
        return httpx.Response(
            200,
            json={"data": {"read": json["ids"][:1]}, "error": None},
            request=httpx.Request("POST", "https://api.example.test" + path),
        )


def test_read_many_posts_ids_once_and_returns_the_ones_marked() -> None:
    client = _FakeClient()
    notifications = Notifications(SimpleNamespace(client=client, websocket=None))

    read = notifications.read_many(["n1", "n2"])

    assert client.posts == [("/user/notifications/read", {"ids": ["n1", "n2"]})]
    assert read == ["n1"]
