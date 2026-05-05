from typing import TYPE_CHECKING, Dict, List, Optional, Union

from pydantic import BaseModel

from ouro.utils import is_valid_uuid

from .asset import Asset
from .route import Route, RouteData, RouteMetrics

if TYPE_CHECKING:
    from ouro import Ouro
    from ouro.models.action import Action


class ServiceMetadata(BaseModel):
    base_url: str
    spec_path: str
    authentication: str


class Service(Asset):
    metadata: Optional[ServiceMetadata] = None
    _ouro: Optional["Ouro"] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ouro = kwargs.get("_ouro")

    def _require_client(self) -> "Ouro":
        if not self._ouro:
            raise RuntimeError("Service object not connected to Ouro client")
        return self._ouro

    def read_spec(self) -> Dict:
        """Get the OpenAPI specification for this service."""
        ouro = self._require_client()
        return ouro.services.read_spec(str(self.id))

    def read_routes(self) -> List[Route]:
        """Get all routes for this service."""
        ouro = self._require_client()
        return ouro.services.read_routes(str(self.id))

    def _route_target(self, route_name_or_id: str) -> str:
        if is_valid_uuid(route_name_or_id) or "/" in route_name_or_id:
            return route_name_or_id
        return f"{self.id}/{route_name_or_id}"

    def execute_route(self, route_name_or_id: str, **kwargs) -> "Action":
        """Execute a specific route of this service and return the full Action."""
        ouro = self._require_client()
        return ouro.routes.execute(self._route_target(route_name_or_id), **kwargs)

    def use_route(self, route_name_or_id: str, **kwargs) -> Union[Dict, "Action"]:
        """Deprecated compatibility wrapper for :meth:`execute_route`.

        ``route_name_or_id`` may be:
          - a bare route slug (e.g. ``"predict"``), which is resolved relative
            to this service's entity name;
          - a fully-qualified ``"entity_name/route_name"``;
          - or a route UUID.

        The latter two are passed through unchanged so we don't accidentally
        build a 3-segment identifier like ``"{service_id}/entity/route"``.
        """
        wait = kwargs.get("wait", True)
        kwargs.setdefault("raise_on_error", wait)
        action = self.execute_route(route_name_or_id, **kwargs)
        return action if not wait else action.final_data
