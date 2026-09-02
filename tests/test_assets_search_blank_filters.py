from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock

from ouro.resources.assets import Assets


class TestAssetSearchBlankFilters(unittest.TestCase):
    def test_search_page_drops_blank_uuid_and_enum_filters(self) -> None:
        client = MagicMock()
        response = MagicMock()
        client.get.return_value = response

        assets = Assets(MagicMock(client=client))
        assets._handle_response = MagicMock(  # type: ignore[method-assign]
            return_value={"data": [], "pagination": None}
        )

        assets._search_page(
            "energy gate",
            limit=20,
            offset=0,
            with_pagination=True,
            kwargs={
                "asset_type": "service",
                "scope": "all",
                "org_id": "",
                "team_id": "   ",
                "user_id": "",
                "visibility": "",
                "sort": "relevant",
                "time_window": "",
            },
        )

        params = client.get.call_args.kwargs["params"]
        self.assertEqual(params["query"], "energy gate")
        self.assertEqual(params["scope"], "all")
        self.assertEqual(params["sort"], "relevant")
        self.assertNotIn("time_window", params)
        filters = json.loads(params["filters"])
        self.assertEqual(filters, {"asset_type": "service"})
        self.assertNotIn("org_id", filters)
        self.assertNotIn("team_id", filters)
        self.assertNotIn("user_id", filters)
        self.assertNotIn("visibility", filters)

    def test_search_page_drops_string_null_sentinels(self) -> None:
        client = MagicMock()
        response = MagicMock()
        client.get.return_value = response

        assets = Assets(MagicMock(client=client))
        assets._handle_response = MagicMock(  # type: ignore[method-assign]
            return_value={"data": [], "pagination": None}
        )

        assets._search_page(
            "energy gate",
            limit=20,
            offset=0,
            with_pagination=True,
            kwargs={
                "asset_type": "/null",
                "scope": "all",
                "org_id": "null",
                "team_id": "None",
                "user_id": "undefined",
                "visibility": " /null ",
                "sort": "relevant",
                "time_window": "/null",
            },
        )

        params = client.get.call_args.kwargs["params"]
        self.assertEqual(params["query"], "energy gate")
        self.assertEqual(params["scope"], "all")
        self.assertEqual(params["sort"], "relevant")
        self.assertNotIn("time_window", params)
        self.assertNotIn("filters", params)


if __name__ == "__main__":
    unittest.main()
