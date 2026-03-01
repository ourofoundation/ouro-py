from __future__ import annotations

import logging
from typing import List, Optional, Union

from ouro._resource import SyncAPIResource


log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Notifications"]


class Notifications(SyncAPIResource):
    def list(
        self,
        offset: int = 0,
        limit: int = 20,
        org_id: Optional[str] = None,
        unread_only: bool = False,
        with_pagination: bool = False,
    ) -> Union[List[dict], dict]:
        """Fetch paginated notifications for the authenticated user."""
        params = {
            "offset": offset,
            "limit": limit,
        }
        if org_id is not None:
            params["org_id"] = org_id
        if unread_only:
            params["unread_only"] = "true"

        request = self.client.get("/user/notifications", params=params)
        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            if not isinstance(result, dict):
                return {"data": [], "pagination": None}
            result["data"] = result.get("data") or []
            return result
        return self._handle_response(request) or []

    def unreads(self, org_id: Optional[str] = None) -> int:
        """Get the count of unread notifications."""
        params = {}
        if org_id is not None:
            params["org_id"] = org_id

        request = self.client.get("/user/notifications/unreads", params=params)
        return self._handle_response(request) or 0

    def read(self, id: str) -> dict:
        """Mark a single notification as read and return it."""
        request = self.client.get(f"/user/notifications/{id}")
        return self._handle_response(request) or {}
