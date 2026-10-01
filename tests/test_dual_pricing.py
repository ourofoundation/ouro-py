from __future__ import annotations

import unittest

from ouro.models.asset import Asset
from ouro.resources.routes import Routes

from test_route_actions import _FakeOuro, _FakeResponse

ROUTE_ID = "00000000-0000-0000-0000-000000000010"

_ROUTE = {
    "id": ROUTE_ID,
    "user_id": "00000000-0000-0000-0000-000000000011",
    "org_id": "00000000-0000-0000-0000-000000000012",
    "team_id": "00000000-0000-0000-0000-000000000013",
    "parent_id": "00000000-0000-0000-0000-000000000014",
    "asset_type": "route",
    "name": "Predict",
    "visibility": "monetized",
    "monetization": "pay-per-use",
    "price_currency": "usd",
    "unit_cost": 0.25,
    "unit_cost_usd": 0.25,
    "unit_cost_sats": 250,
    "created_at": "2026-01-01T00:00:00+00:00",
    "last_updated": "2026-01-01T00:00:00+00:00",
}

_RUN = {
    "data": {"responseData": {"ok": True}},
    "action": {
        "id": "00000000-0000-0000-0000-000000000001",
        "route_id": ROUTE_ID,
        "user_id": "00000000-0000-0000-0000-000000000003",
        "status": "success",
    },
    "metadata": {},
}


class TestDualPricing(unittest.TestCase):
    def test_asset_exposes_a_price_per_currency(self) -> None:
        route = Asset(**_ROUTE)
        self.assertEqual(route.unit_cost_usd, 0.25)
        self.assertEqual(route.unit_cost_sats, 250)
        self.assertIsNone(route.price_sats)

    def test_execute_sends_the_currency_to_pay_in(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": _ROUTE}), _FakeResponse(_RUN)])

        Routes(ouro).execute(ROUTE_ID, body={}, currency="btc")

        sent = ouro.client.requests[1]["json"]
        self.assertEqual(sent["currency"], "btc")
        self.assertNotIn("currency", sent["config"])

    def test_execute_leaves_the_currency_to_the_route_by_default(self) -> None:
        ouro = _FakeOuro([_FakeResponse({"data": _ROUTE}), _FakeResponse(_RUN)])

        Routes(ouro).execute(ROUTE_ID, body={})

        self.assertNotIn("currency", ouro.client.requests[1]["json"])

    def test_cost_can_be_quoted_in_either_currency(self) -> None:
        ouro = _FakeOuro(
            [
                _FakeResponse({"data": _ROUTE}),
                _FakeResponse(
                    {"data": {"cost": {"total_cost": 500, "currency": "btc"}}}
                ),
            ]
        )

        cost = Routes(ouro).cost(ROUTE_ID, "asset-1", currency="btc")

        self.assertEqual(
            ouro.client.requests[1]["params"],
            {"input": "asset-1", "currency": "btc"},
        )
        self.assertEqual(cost.currency, "btc")
        self.assertEqual(cost.total_cost, 500)


if __name__ == "__main__":
    unittest.main()
