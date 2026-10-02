"""Unit tests for pinning a client to an organization."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from ouro._exceptions import OuroError
from ouro.client import Ouro

ORG = "01a0fcf5-0765-7463-b0e2-ffe2d0fa994d"
OTHER_ORG = "01a0fcf5-0765-7463-b0e2-ffe2d0fa0000"
TEAM = "01a0fcf5-0765-7463-b0e2-ffe2d0fa1111"
DEFAULT_TEAM = "01a0fcf5-0765-7463-b0e2-ffe2d0fa2222"


def _response(data: object, ok: bool = True) -> MagicMock:
    response = MagicMock(is_success=ok)
    response.json.return_value = {"data": data}
    return response


class OrganizationPinTests(unittest.TestCase):
    def _construct(self, env: dict | None = None, **kwargs) -> Ouro:
        """Build an Ouro instance without hitting the network."""
        with (
            patch.dict(os.environ, env or {}, clear=False),
            patch.object(Ouro, "exchange_api_key"),
            patch.object(Ouro, "_bootstrap_authenticated_client"),
            patch("ouro.client.OuroWebSocket", return_value=MagicMock()),
            patch("ouro.client.httpx.Client") as mock_httpx,
        ):
            mock_httpx.return_value = MagicMock(headers={})
            ouro = Ouro(api_key="test-key", **kwargs)
        ouro.client = MagicMock()
        return ouro

    def test_unpinned_client_leaves_assets_alone(self) -> None:
        ouro = self._construct(organization="")
        asset = {"name": "x"}
        self.assertEqual(ouro.posts._scope_create(asset), {"name": "x"})
        ouro.posts._scope_update({"org_id": OTHER_ORG})
        ouro.client.get.assert_not_called()

    def test_pinned_create_fills_org_and_team(self) -> None:
        ouro = self._construct(organization=ORG, team=TEAM)
        asset = ouro._scope_create({"name": "x"})
        self.assertEqual(asset["org_id"], ORG)
        self.assertEqual(asset["team_id"], TEAM)
        ouro.client.get.assert_not_called()

    def test_default_team_is_looked_up_once(self) -> None:
        ouro = self._construct(organization=ORG)
        ouro.client.get.return_value = _response({"default_team": {"id": DEFAULT_TEAM}})
        self.assertEqual(ouro._scope_create({})["team_id"], DEFAULT_TEAM)
        self.assertEqual(ouro._scope_create({})["team_id"], DEFAULT_TEAM)
        ouro.client.get.assert_called_once_with(f"/organizations/{ORG}")

    def test_explicit_team_is_kept(self) -> None:
        ouro = self._construct(organization=ORG)
        asset = ouro._scope_create({"team_id": TEAM})
        self.assertEqual(asset["team_id"], TEAM)
        ouro.client.get.assert_not_called()

    def test_other_org_is_refused_on_create_and_update(self) -> None:
        ouro = self._construct(organization=ORG, team=TEAM)
        with self.assertRaises(OuroError):
            ouro._scope_create({"org_id": OTHER_ORG})
        with self.assertRaises(OuroError):
            ouro.posts._scope_update({"org_id": OTHER_ORG})
        ouro.posts._scope_update({"org_id": ORG.upper()})
        ouro.posts._scope_update({"name": "renamed"})

    def test_env_pins_and_empty_string_opts_out(self) -> None:
        env = {"OURO_ORG_ID": ORG, "OURO_TEAM_ID": TEAM}
        pinned = self._construct(env=env)
        self.assertEqual(pinned.organization, ORG)
        self.assertEqual(pinned.team, TEAM)
        unpinned = self._construct(env=env, organization="")
        self.assertIsNone(unpinned.organization)
        self.assertIsNone(unpinned.team)

    def test_org_name_is_resolved(self) -> None:
        ouro = self._construct(organization="")
        ouro.client.get.return_value = _response({"id": ORG, "name": "acme"})
        ouro.use_organization("acme")
        self.assertEqual(ouro.organization, ORG)
        ouro.client.get.assert_called_once_with("/organizations/by-name/acme")

    def test_unknown_org_name_raises(self) -> None:
        ouro = self._construct(organization="")
        ouro.client.get.return_value = _response(None, ok=False)
        with self.assertRaises(OuroError):
            ouro.use_organization("nope")

    def test_routes_keep_service_team(self) -> None:
        ouro = self._construct(organization=ORG)
        route = ouro._scope_create({"org_id": ORG}, default_team=False)
        self.assertNotIn("team_id", route)
        ouro.client.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
