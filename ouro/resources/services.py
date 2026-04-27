from __future__ import annotations

import logging
from typing import Dict, List

from ouro._resource import SyncAPIResource
from ouro.models import Route, Service
from ouro.resources.routes import Routes

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Services"]


class Services(SyncAPIResource):
    @property
    def routes(self) -> Routes:
        """Deprecated alias for ``ouro.routes``.

        Kept for backwards compatibility. Prefer ``ouro.routes`` directly — both
        point to the same underlying ``Routes`` instance now.
        """
        return self.ouro.routes

    def retrieve(self, id: str) -> Service:
        """Retrieve a Service by its ID."""
        request = self.client.get(f"/services/{id}")
        return Service(**self._handle_response(request), _ouro=self.ouro)

    def list(self) -> List[Service]:
        """List all services in the current context."""
        request = self.client.get("/services")
        return [Service(**s, _ouro=self.ouro) for s in self._handle_response(request)]

    def read_spec(self, id: str) -> Dict:
        """Get the OpenAPI specification for a service."""
        request = self.client.get(f"/services/{id}/spec")
        return self._handle_response(request)

    def read_routes(self, id: str) -> List[Route]:
        """Get all routes for a service."""
        request = self.client.get(f"/services/{id}/routes")
        return [Route(**r, _ouro=self.ouro) for r in self._handle_response(request)]
