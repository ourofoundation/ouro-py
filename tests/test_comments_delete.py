from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from ouro.resources.assets import Assets
from ouro.resources.comments import Comments


class TestCommentsDelete(unittest.TestCase):
    def test_delete_delegates_to_posts_delete(self) -> None:
        posts = MagicMock()
        posts.delete.return_value = {
            "id": "comment-1",
            "name": "",
            "asset_type": "comment",
            "deleted_children": [],
        }
        ouro = SimpleNamespace(
            posts=posts, client=MagicMock(), websocket=None
        )
        comments = Comments(ouro)

        result = comments.delete("comment-1", dry_run=True)

        posts.delete.assert_called_once_with(
            "comment-1", delete_children=False, dry_run=True
        )
        self.assertEqual(result["asset_type"], "comment")


class TestAssetsDeleteComment(unittest.TestCase):
    def test_delete_routes_comments(self) -> None:
        client = MagicMock()
        type_response = MagicMock()
        type_response.status_code = 200
        type_response.json.return_value = {
            "data": {"asset_type": "comment"},
            "error": None,
        }
        client.get.return_value = type_response

        comments = MagicMock()
        comments.delete.return_value = {
            "id": "comment-1",
            "name": "",
            "asset_type": "comment",
            "deleted_children": [],
        }
        ouro = SimpleNamespace(
            client=client, websocket=None, comments=comments
        )
        assets = Assets(ouro)

        result = assets.delete("comment-1")

        comments.delete.assert_called_once_with(
            "comment-1", delete_children=False, dry_run=False
        )
        self.assertEqual(result["id"], "comment-1")


class TestAssetsDeleteRoute(unittest.TestCase):
    def test_delete_routes_routes(self) -> None:
        client = MagicMock()
        type_response = MagicMock()
        type_response.status_code = 200
        type_response.json.return_value = {
            "data": {"asset_type": "route"},
            "error": None,
        }
        client.get.return_value = type_response

        routes = MagicMock()
        routes.delete.return_value = {
            "id": "route-1",
            "name": "predict",
            "asset_type": "route",
            "deleted_children": [],
        }
        ouro = SimpleNamespace(
            client=client, websocket=None, routes=routes
        )
        assets = Assets(ouro)

        result = assets.delete("route-1")

        routes.delete.assert_called_once_with(
            "route-1", delete_children=False, dry_run=False
        )
        self.assertEqual(result["id"], "route-1")


if __name__ == "__main__":
    unittest.main()
