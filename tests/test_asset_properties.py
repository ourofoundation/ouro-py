from __future__ import annotations

import unittest

from ouro.models import AssetProperty
from ouro.resources.assets import Assets

from tests.test_asset_actions import ASSET_ID, ROUTE_ID, _FakeOuro, _FakeResponse

VALUE_ID = "00000000-0000-0000-0000-000000000010"
ACTION_ID = "00000000-0000-0000-0000-000000000011"


def _property(**overrides):
    base = {
        "key": "curie_temperature",
        "label": "Curie temperature",
        "value_type": "number",
        "unit": "K",
        "asset_types": ["file"],
        "values": [
            {
                "id": VALUE_ID,
                "value": 512.5,
                "uncertainty": None,
                "source": "route",
                "method": "Monte Carlo",
                "status": "active",
                "created_at": "2026-10-18T16:18:22+00:00",
                "action_id": ACTION_ID,
                "route": {"id": ROUTE_ID, "name": "Curie", "slug": "/routes/a/curie"},
            }
        ],
    }
    base.update(overrides)
    return base


class TestAssetProperties(unittest.TestCase):
    def test_properties_parses_values(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": [_property()]})])
        properties = Assets(ouro).properties(ASSET_ID)

        self.assertEqual(ouro.client.requests[0]["path"], f"/assets/{ASSET_ID}/properties")
        self.assertIsNone(ouro.client.requests[0]["params"])
        self.assertEqual(len(properties), 1)
        prop = properties[0]
        self.assertIsInstance(prop, AssetProperty)
        self.assertEqual(prop.value, 512.5)
        self.assertEqual(prop.values[0].route.name, "Curie")
        self.assertEqual(str(prop.values[0].action_id), ACTION_ID)

    def test_history_flag(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": []})])
        self.assertEqual(Assets(ouro).properties(ASSET_ID, history=True), [])
        self.assertEqual(ouro.client.requests[0]["params"], {"history": "true"})

    def test_value_is_none_without_values(self) -> None:
        self.assertIsNone(AssetProperty.model_validate(_property(values=[])).value)


if __name__ == "__main__":
    unittest.main()
