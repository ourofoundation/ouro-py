from __future__ import annotations

import unittest

from ouro.resources.assets import Assets

from tests.test_asset_actions import ASSET_ID, ROUTE_ID, _FakeOuro, _FakeResponse

NAME = "mmoderwell/check-whether-a-crystal-structure-is-already-known"


def _matches(*rows: tuple[str, str]) -> _FakeResponse:
    return _FakeResponse({"data": [{"id": id, "asset_type": t} for id, t in rows]})


class TestAssetResolveId(unittest.TestCase):
    def test_uuid_needs_no_request(self) -> None:
        ouro = _FakeOuro([])
        self.assertEqual(Assets(ouro).resolve_id(ASSET_ID), ASSET_ID)
        self.assertEqual(ouro.client.requests, [])

    def test_entity_name_looks_up_any_type(self) -> None:
        ouro = _FakeOuro([_matches((ROUTE_ID, "route"))])
        self.assertEqual(Assets(ouro).resolve_id(NAME), ROUTE_ID)
        self.assertEqual(
            ouro.client.requests,
            [{"path": f"/assets/by-name/{NAME}", "params": None}],
        )

    def test_typed_path_and_url_narrow_the_type(self) -> None:
        for ref in (
            f"routes/{NAME}",
            f"/routes/{NAME}",
            f"https://ouro.foundation/routes/{NAME}?tab=runs",
            f"@{NAME}",
        ):
            ouro = _FakeOuro([_matches((ROUTE_ID, "route"))])
            self.assertEqual(Assets(ouro).resolve_id(ref), ROUTE_ID)
            request = ouro.client.requests[0]
            self.assertEqual(request["path"], f"/assets/by-name/{NAME}")
            expected = None if ref.startswith("@") else {"asset_type": "route"}
            self.assertEqual(request["params"], expected)

    def test_asset_type_argument_narrows_the_type(self) -> None:
        ouro = _FakeOuro([_matches((ROUTE_ID, "route"))])
        Assets(ouro).resolve_id(NAME, "route")
        self.assertEqual(ouro.client.requests[0]["params"], {"asset_type": "route"})

    def test_conflicting_types_fail(self) -> None:
        with self.assertRaisesRegex(ValueError, "names a dataset, not a route"):
            Assets(_FakeOuro([])).resolve_id(f"datasets/{NAME}", "route")

    def test_ambiguous_name_lists_candidates(self) -> None:
        ouro = _FakeOuro([_matches((ASSET_ID, "dataset"), (ROUTE_ID, "file"))])
        with self.assertRaises(ValueError) as caught:
            Assets(ouro).resolve_id("hermes/sine-wave-dataset")
        message = str(caught.exception)
        self.assertIn(f"dataset {ASSET_ID}", message)
        self.assertIn(f"file {ROUTE_ID}", message)
        self.assertIn('"datasets/hermes/sine-wave-dataset"', message)

    def test_malformed_ref_fails(self) -> None:
        for ref in ("just-a-slug", "a/b/c/d", "a/", "routes//a"):
            with self.assertRaisesRegex(ValueError, "not an asset id or name"):
                Assets(_FakeOuro([])).resolve_id(ref)


if __name__ == "__main__":
    unittest.main()
