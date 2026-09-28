from __future__ import annotations

import logging
from typing import Optional

from ouro._resource import SyncAPIResource
from ouro.models import Notification, Page


log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Notifications"]


class Notifications(SyncAPIResource):
    def list(
        self,
        offset: int = 0,
        limit: int = 20,
        org_id: Optional[str] = None,
        unread_only: bool = False,
        category: Optional[str] = None,
    ) -> Page[Notification]:
        """Fetch a page of notifications for the authenticated user.

        ``category`` accepts a single category or a comma-separated list of
        categories (``mentions``, ``comments``, ``references``, ``shares``,
        ``money``, ``actions``).
        """
        params = {
            "offset": offset,
            "limit": limit,
        }
        if org_id is not None:
            params["org_id"] = org_id
        if unread_only:
            params["unread_only"] = "true"
        if category is not None:
            params["category"] = category

        request = self.client.get("/user/notifications", params=params)
        return self._page(Page[Notification], self._handle_response(request, raw=True))

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
        return self._parse(Notification, self._handle_response(request))
