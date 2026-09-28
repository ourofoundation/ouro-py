from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ouro._resource import SyncAPIResource
from ouro.models import PlanInfo, User, UserImpact

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Users"]


class Users(SyncAPIResource):
    def me(self) -> User:
        """Return the authenticated user's profile (username, bio, etc.)."""
        request = self.client.get("/user/profile")
        return self._parse(User, self._handle_response(request))

    def plan(self) -> PlanInfo:
        """Return the authenticated user's plan, its limits, and current usage."""
        request = self.client.get("/user/plan-info")
        return self._parse(PlanInfo, self._handle_response(request))

    def get(self, name_or_id: str) -> User:
        """Look up a user profile by username or user_id."""
        request = self.client.get(f"/users/{name_or_id}")
        return self._parse(User, self._handle_response(request))

    def search(
        self,
        query: str,
        **kwargs: Any,
    ) -> List[User]:
        """Search for users."""
        request = self.client.get(
            "/users/search",
            params={"query": query, **kwargs},
        )
        return self._parse_list(User, self._handle_response(request))

    def impact(
        self,
        name_or_id: str,
        *,
        since: Optional[str] = None,
        limit: Optional[int] = None,
        asset_ids: Optional[List[str]] = None,
    ) -> UserImpact:
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
        return self._parse(UserImpact, self._handle_response(request))
