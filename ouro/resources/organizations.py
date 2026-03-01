from __future__ import annotations

import logging
from typing import List

from ouro._resource import SyncAPIResource

log: logging.Logger = logging.getLogger(__name__)

__all__ = ["Organizations"]


class Organizations(SyncAPIResource):
    def list(self) -> List[dict]:
        """List organizations the authenticated user belongs to."""
        request = self.client.get("/organizations/user")
        return self._handle_response(request) or []

    def list_discoverable(self) -> List[dict]:
        """List discoverable organizations (open or request-to-join policy)."""
        request = self.client.get("/organizations/discoverable")
        return self._handle_response(request) or []

    def retrieve(self, id: str) -> dict:
        """Retrieve an organization by ID."""
        request = self.client.get(f"/organizations/{id}")
        return self._handle_response(request) or {}

    def get_context(self) -> dict:
        """Get the current user's active organization context."""
        request = self.client.get("/organizations/context")
        return self._handle_response(request) or {}
