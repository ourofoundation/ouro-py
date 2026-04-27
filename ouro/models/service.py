from typing import TYPE_CHECKING, Dict, List, Optional

from pydantic import BaseModel

from ouro.utils import is_valid_uuid

from .asset import Asset
from .route import Route

if TYPE_CHECKING:
    from ouro import Ouro


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

    def use_route(self, route_name_or_id: str, **kwargs) -> Dict:
        """Use/execute a specific route of this service.

        ``route_name_or_id`` may be:
          - a bare route slug (e.g. ``"predict"``), which is resolved relative
            to this service's entity name;
          - a fully-qualified ``"entity_name/route_name"``;
          - or a route UUID.

        The latter two are passed through unchanged so we don't accidentally
        build a 3-segment identifier like ``"{service_id}/entity/route"``.
        """
        ouro = self._require_client()
        if is_valid_uuid(route_name_or_id) or "/" in route_name_or_id:
            target = route_name_or_id
        else:
            target = f"{self.id}/{route_name_or_id}"
        return ouro.services.routes.use(target, **kwargs)
