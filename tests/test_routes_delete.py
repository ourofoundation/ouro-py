from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from ouro.resources.routes import Routes


class TestRoutesDelete(unittest.TestCase):
    def test_delete_hits_routes_endpoint(self) -> None:
        client = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "data": {
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "predict",
                "asset_type": "route",
                "deleted_children": [],
            },
            "error": None,
        }
        client.delete.return_value = response

        routes = Routes(SimpleNamespace(client=client, websocket=None))
        result = routes.delete("00000000-0000-0000-0000-000000000001", dry_run=True)

        client.delete.assert_called_once_with(
            "/routes/00000000-0000-0000-0000-000000000001",
            params={"delete_children": "false", "dry_run": "true"},
        )
        self.assertEqual(result.asset_type, "route")


if __name__ == "__main__":
    unittest.main()
