from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

from ouro.resources.files import (
    Files,
    _merge_file_metadata_filters,
    _normalize_extension,
)


def _file_hit(file_id: str, name: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "id": file_id,
        "user_id": "00000000-0000-0000-0000-000000000099",
        "org_id": "00000000-0000-0000-0000-000000000000",
        "team_id": "00000000-0000-0000-0000-000000000000",
        "name": name,
        "asset_type": "file",
        "visibility": "public",
        "created_at": now,
        "last_updated": now,
    }


class TestFileSearchHelpers(unittest.TestCase):
    def test_normalize_extension_strips_dot_and_lowercases(self) -> None:
        self.assertEqual(_normalize_extension(".CIF"), "cif")
        self.assertEqual(_normalize_extension([".cif", "XYZ"]), ["cif", "xyz"])
        self.assertIsNone(_normalize_extension(None))
        self.assertIsNone(_normalize_extension(""))
        self.assertIsNone(_normalize_extension([]))

    def test_merge_file_metadata_filters_prefers_first_class_kwargs(self) -> None:
        merged = _merge_file_metadata_filters(
            extension="cif",
            file_type="image",
            metadata_filters={"extension": "csv", "custom": 1},
        )
        self.assertEqual(
            merged,
            {"extension": "cif", "custom": 1, "file_type": "image"},
        )


class TestFilesSearch(unittest.TestCase):
    def _files(self, search_return) -> tuple[Files, MagicMock]:
        ouro = MagicMock()
        ouro.assets.search.return_value = search_return
        return Files(ouro), ouro.assets.search

    def test_search_scopes_to_files_and_passes_extension(self) -> None:
        files, search = self._files(
            [_file_hit("00000000-0000-0000-0000-000000000001", "MnBi.cif")]
        )

        results = files.search(extension=".CIF", scope="all", limit=50, offset=10)

        search.assert_called_once()
        kwargs = search.call_args.kwargs
        self.assertEqual(kwargs["query"], "")
        self.assertEqual(kwargs["asset_type"], "file")
        self.assertEqual(kwargs["scope"], "all")
        self.assertEqual(kwargs["limit"], 50)
        self.assertEqual(kwargs["offset"], 10)
        self.assertEqual(kwargs["metadata_filters"], {"extension": "cif"})
        self.assertFalse(kwargs["with_pagination"])
        self.assertNotIn("org_id", kwargs)
        self.assertNotIn("team_id", kwargs)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, UUID("00000000-0000-0000-0000-000000000001"))
        self.assertIs(results[0]._ouro, files.ouro)

    def test_search_with_pagination_wraps_file_objects(self) -> None:
        files, search = self._files(
            {
                "data": [_file_hit("00000000-0000-0000-0000-000000000002", "FeCo.cif")],
                "pagination": {"offset": 0, "limit": 100, "hasMore": True},
            }
        )

        page = files.search(
            extension=["cif", "xyz"],
            team_id="team-1",
            with_pagination=True,
        )

        kwargs = search.call_args.kwargs
        self.assertEqual(kwargs["metadata_filters"], {"extension": ["cif", "xyz"]})
        self.assertEqual(kwargs["team_id"], "team-1")
        self.assertTrue(kwargs["with_pagination"])
        self.assertNotIn("org_id", kwargs)
        self.assertEqual(len(page["data"]), 1)
        self.assertEqual(page["data"][0].name, "FeCo.cif")
        self.assertEqual(page["pagination"]["hasMore"], True)

    def test_search_ignores_caller_asset_type_override(self) -> None:
        files, search = self._files([])
        files.search(extension="cif", asset_type="post")
        self.assertEqual(search.call_args.kwargs["asset_type"], "file")

    def test_list_delegates_to_search(self) -> None:
        files, search = self._files([])
        files.list(extension="cif", query="magnet", scope="all")
        kwargs = search.call_args.kwargs
        self.assertEqual(kwargs["query"], "magnet")
        self.assertEqual(kwargs["asset_type"], "file")
        self.assertEqual(kwargs["metadata_filters"], {"extension": "cif"})
        self.assertEqual(kwargs["scope"], "all")
        self.assertFalse(kwargs["with_pagination"])


class TestSearchAutoPagination(unittest.TestCase):
    """assets.search transparently paginates for limit=None or limit > 200."""

    def _files_with_pages(self, pages: list) -> tuple[Files, MagicMock]:
        """Real Assets.search pagination logic; mocked page fetches."""
        from ouro.resources.assets import Assets

        ouro = MagicMock()
        assets = Assets(ouro)
        assets._search_page = MagicMock(side_effect=pages)
        ouro.assets = assets
        return Files(ouro), assets._search_page

    def test_limit_none_paginates_until_has_more_false(self) -> None:
        pages = [
            {
                "data": [
                    _file_hit("00000000-0000-0000-0000-000000000001", "a.cif"),
                    _file_hit("00000000-0000-0000-0000-000000000002", "b.cif"),
                ],
                "pagination": {"offset": 0, "limit": 200, "hasMore": True},
            },
            {
                "data": [
                    _file_hit("00000000-0000-0000-0000-000000000003", "c.cif"),
                ],
                "pagination": {"offset": 2, "limit": 200, "hasMore": False},
            },
        ]
        files, page_fetch = self._files_with_pages(pages)

        results = files.search(extension="cif", scope="all", limit=None)

        self.assertEqual([f.name for f in results], ["a.cif", "b.cif", "c.cif"])
        self.assertEqual(page_fetch.call_count, 2)
        (q1, limit1, offset1, wp1, kw1), _ = page_fetch.call_args_list[0]
        (_, limit2, offset2, _, _), _ = page_fetch.call_args_list[1]
        self.assertEqual((limit1, offset1), (200, 0))
        self.assertEqual((limit2, offset2), (200, 2))
        self.assertTrue(wp1)
        self.assertEqual(kw1["asset_type"], "file")
        self.assertEqual(kw1["metadata_filters"], {"extension": "cif"})

    def test_large_limit_paginates_and_truncates(self) -> None:
        pages = [
            {
                "data": [
                    _file_hit(f"00000000-0000-0000-0000-0000000000{i:02d}", f"{i}.cif")
                    for i in range(1, 4)
                ],
                "pagination": {"offset": 0, "limit": 200, "hasMore": True},
            },
            {
                "data": [
                    _file_hit("00000000-0000-0000-0000-000000000010", "10.cif"),
                    _file_hit("00000000-0000-0000-0000-000000000011", "11.cif"),
                ],
                "pagination": {"offset": 3, "limit": 200, "hasMore": False},
            },
        ]
        files, page_fetch = self._files_with_pages(pages)

        results = files.search(extension="cif", limit=204)

        self.assertEqual(page_fetch.call_count, 2)
        (_, limit1, offset1, _, _), _ = page_fetch.call_args_list[0]
        (_, limit2, offset2, _, _), _ = page_fetch.call_args_list[1]
        self.assertEqual((limit1, offset1), (200, 0))
        # remaining = 204 - 3 collected = 201, capped at 200
        self.assertEqual((limit2, offset2), (200, 3))
        self.assertEqual(len(results), 5)

    def test_small_limit_uses_single_page(self) -> None:
        files, page_fetch = self._files_with_pages(
            [[_file_hit("00000000-0000-0000-0000-000000000001", "a.cif")]]
        )
        results = files.search(extension="cif", limit=50)
        self.assertEqual(page_fetch.call_count, 1)
        (_, limit, offset, with_pagination, _), _ = page_fetch.call_args
        self.assertEqual((limit, offset, with_pagination), (50, 0, False))
        self.assertEqual(len(results), 1)

    def test_limit_none_stops_on_empty_page(self) -> None:
        pages = [
            {"data": [], "pagination": {"offset": 0, "limit": 200, "hasMore": True}},
        ]
        files, page_fetch = self._files_with_pages(pages)
        self.assertEqual(files.search(extension="cif", limit=None), [])
        self.assertEqual(page_fetch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
