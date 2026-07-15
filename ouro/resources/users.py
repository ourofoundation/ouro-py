from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ouro._resource import SyncAPIResource

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Users"]


class Users(SyncAPIResource):
    def me(self) -> Optional[Dict[str, Any]]:
        """Return the authenticated user's profile (username, bio, etc.)."""
        request = self.client.get("/user/profile")
        return self._handle_response(request)

    def get(self, name_or_id: str) -> Optional[Dict[str, Any]]:
        """Look up a user profile by username or user_id."""
        request = self.client.get(f"/users/{name_or_id}")
        return self._handle_response(request)

    def search(
        self,
        query: str,
        **kwargs: Any,
    ) -> List[dict]:
        """Search for users."""
        request = self.client.get(
            "/users/search",
            params={"query": query, **kwargs},
        )
        return self._handle_response(request) or []

    def impact(
        self,
        name_or_id: str,
        *,
        since: Optional[str] = None,
        limit: Optional[int] = None,
        asset_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Engagement / outcome impact for a user's assets.

        Includes external-vs-self comments/reactions and bot-filtered quality
        views. Pass ``asset_ids`` to scope the rollup to specific assets.
        """
        params: Dict[str, Any] = {}
        if since:
            params["since"] = since
        if limit is not None:
            params["limit"] = limit
        if asset_ids:
            params["asset_ids"] = ",".join(str(a) for a in asset_ids if a)
        request = self.client.get(f"/users/{name_or_id}/impact", params=params)
        return self._handle_response(request) or {}
