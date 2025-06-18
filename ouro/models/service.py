from typing import TYPE_CHECKING, Dict, List, Optional

from pydantic import BaseModel

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

    def get_spec(self) -> Dict:
        """
        Get the OpenAPI specification for this service
        """
        if not self._ouro:
            raise RuntimeError("Service object not connected to Ouro client")
        return self._ouro.services.get_spec(str(self.id))

    def get_routes(self) -> List[Route]:
        """
        Get all routes for this service
        """
        if not self._ouro:
            raise RuntimeError("Service object not connected to Ouro client")
        return self._ouro.services.get_routes(str(self.id))

    def use_route(self, route_name_or_id: str, **kwargs) -> Dict:
        """
        Use/execute a specific route of this service
        """
        if not self._ouro:
            raise RuntimeError("Service object not connected to Ouro client")
        return self._ouro.services.routes.use(f"{self.id}/{route_name_or_id}", **kwargs)
