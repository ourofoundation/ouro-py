from typing import Any, Dict, List, Optional
from uuid import UUID

from ouro.utils import is_valid_uuid

from ._base import OuroModel
from .action import Action
from .asset import Asset
from .route import Route


class ServiceMetadata(OuroModel):
    base_url: str
    authentication: str
    version: Optional[str] = None
    spec_path: Optional[str] = None
    spec_url: Optional[str] = None
    auth_url: Optional[str] = None


class ServiceAuthentication(OuroModel):
    """The service owner's stored authentication secret."""

    id: UUID
    secret_id: UUID
    method: str
    # False when the new secret matched the stored one.
    rotated: bool


class Service(Asset):
    metadata: Optional[ServiceMetadata] = None

    def read_spec(self) -> Dict[str, Any]:
        """Get the OpenAPI specification for this service."""
        return self._require_client().services.read_spec(str(self.id))

    def read_routes(self) -> List[Route]:
        """Get all routes for this service."""
        return self._require_client().services.read_routes(str(self.id))

    def execute_route(self, route_name_or_id: str, **kwargs) -> Action:
        """Execute one of this service's routes and return the full Action.

        ``route_name_or_id`` may be a bare route slug (e.g. ``"predict"``),
        which is resolved relative to this service, a fully-qualified
        ``"entity_name/route_name"``, or a route UUID.
        """
        target = (
            route_name_or_id
            if is_valid_uuid(route_name_or_id) or "/" in route_name_or_id
            else f"{self.id}/{route_name_or_id}"
        )
        return self._require_client().routes.execute(target, **kwargs)
