from __future__ import annotations

import logging
from typing import List

from ouro._resource import SyncAPIResource
from ouro.models import Organization

log: logging.Logger = logging.getLogger(__name__)

__all__ = ["Organizations"]


class Organizations(SyncAPIResource):
    def list(self) -> List[Organization]:
        """List organizations the authenticated user belongs to."""
        request = self.client.get("/organizations/user")
        return self._parse_list(Organization, self._handle_response(request))

    def list_discoverable(self) -> List[Organization]:
        """List discoverable organizations (open or request-to-join policy)."""
        request = self.client.get("/organizations/discoverable")
        return self._parse_list(Organization, self._handle_response(request))

    def retrieve(self, id: str) -> Organization:
        """Retrieve an organization by ID."""
        request = self.client.get(f"/organizations/{id}")
        return self._parse(Organization, self._handle_response(request))

    def get_context(self) -> Organization:
        """Get the current user's active organization context."""
        request = self.client.get("/organizations/context")
        return self._parse(Organization, self._handle_response(request))
