from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from ouro.resources.assets import Assets
from ouro.resources.files import Files


class TestAssetsShare(unittest.TestCase):
    def test_share_puts_assets_share_endpoint(self) -> None:
        client = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"data": None, "error": None}
        client.put.return_value = response

        assets = Assets(SimpleNamespace(client=client, websocket=None))
        assets.share(
            "asset-1",
            "user-2",
            role="read",
        )

        client.put.assert_called_once_with(
            "/assets/asset-1/share",
            json={
                "permission": {
                    "user": {"user_id": "user-2"},
                    "role": "read",
                }
            },
        )


class TestFilesShareDelegates(unittest.TestCase):
    def test_files_share_delegates_to_assets_share(self) -> None:
        assets = MagicMock()
        ouro = SimpleNamespace(assets=assets, client=MagicMock(), websocket=None)
        files = Files(ouro)

        files.share("file-1", "user-2", role="admin")

        assets.share.assert_called_once_with("file-1", "user-2", role="admin")


if __name__ == "__main__":
    unittest.main()
