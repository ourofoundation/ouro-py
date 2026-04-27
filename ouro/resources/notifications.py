from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from ouro._resource import SyncAPIResource
from ouro.models import Notification


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
    ) -> Union[List[Notification], Dict[str, Any]]:
        """Fetch paginated notifications for the authenticated user.

        Returns a list of :class:`Notification` by default. When
        ``with_pagination=True``, returns ``{"data": [Notification, ...],
        "pagination": ...}`` so callers can implement their own paging.
        """
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
            items = result.get("data") or []
            result["data"] = [Notification.model_validate(n) for n in items]
            return result
        data = self._handle_response(request) or []
        return [Notification.model_validate(n) for n in data]

    def unreads(self, org_id: Optional[str] = None) -> int:
        """Get the count of unread notifications."""
        params = {}
        if org_id is not None:
            params["org_id"] = org_id

        request = self.client.get("/user/notifications/unreads", params=params)
        return self._handle_response(request) or 0

    def read(self, id: str) -> Notification:
        """Mark a single notification as read and return it."""
        request = self.client.get(f"/user/notifications/{id}")
        data = self._handle_response(request) or {}
        return Notification.model_validate(data)
