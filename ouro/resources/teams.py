from __future__ import annotations

import logging
from typing import List, Optional

from ouro._resource import SyncAPIResource, _strip_none

log: logging.Logger = logging.getLogger(__name__)

__all__ = ["Teams"]


class Teams(SyncAPIResource):
    def create(
        self,
        name: str,
        org_id: str,
        description: Optional[dict] = None,
        visibility: Optional[str] = None,
        default_role: Optional[str] = None,
        actor_type_policy: Optional[str] = None,
        source_policy: Optional[str] = None,
        **kwargs,
    ) -> dict:
        """Create a team in an organization."""
        team = _strip_none({
            "name": name,
            "org_id": org_id,
            "description": description,
            "visibility": visibility,
            "default_role": default_role,
            "actor_type_policy": actor_type_policy,
            "source_policy": source_policy,
            **kwargs,
        })
        request = self.client.post("/teams/create", json={"team": team})
        return self._handle_response(request) or {}

    def list(
        self,
        org_id: Optional[str] = None,
        joined: Optional[bool] = None,
        public_only: Optional[bool] = None,
    ) -> List[dict]:
        """List teams with optional filters.

        Args:
            org_id: Filter by organization ID.
            joined: If True, only return teams the user has joined.
            public_only: If True, only return public teams.
        """
        params = {}
        if org_id is not None:
            params["org_id"] = org_id
        if joined is not None:
            params["joined"] = str(joined).lower()
        if public_only is not None:
            params["public_only"] = str(public_only).lower()

        request = self.client.get("/teams", params=params)
        return self._handle_response(request) or []

    def retrieve(self, id: str) -> dict:
        """Retrieve a team by ID, including members and metrics."""
        request = self.client.get(f"/teams/{id}")
        return self._handle_response(request) or {}

    def join(self, id: str) -> dict:
        """Join a team."""
        request = self.client.post(f"/teams/{id}/join", json={})
        return self._handle_response(request) or {}

    def leave(self, id: str) -> dict:
        """Leave a team. Uses the authenticated user's ID."""
        user_id = self.ouro.user.id
        request = self.client.delete(f"/teams/{id}/members/{user_id}")
        return self._handle_response(request) or {}

    def activity(
        self,
        id: str,
        offset: int = 0,
        limit: int = 20,
        asset_type: Optional[str] = None,
    ) -> dict:
        """Get a team's activity feed.

        Args:
            id: Team ID.
            offset: Zero-based pagination offset.
            limit: Number of items per page.
            asset_type: Filter by asset type (e.g. "post", "dataset", "file").
        """
        params = {"offset": offset, "limit": limit}
        if asset_type is not None:
            params["assetType"] = asset_type

        request = self.client.get(f"/teams/{id}/activity", params=params)
        return self._handle_response(request, raw=True)
