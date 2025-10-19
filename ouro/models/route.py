from typing import TYPE_CHECKING, Dict, List, Optional

from pydantic import BaseModel

from .asset import Asset

if TYPE_CHECKING:
    from ouro import Ouro


class RouteData(BaseModel):
    description: Optional[str] = None
    path: str
    method: str
    parameters: Optional[List[Dict]] = None
    request_body: Optional[Dict] = {}
    responses: Optional[Dict] = None
    security: Optional[str] = None
    input_type: Optional[str] = None
    input_filter: Optional[str] = None
    input_file_extension: Optional[str] = None
    output_type: Optional[str] = None
    output_file_extension: Optional[str] = None
    rate_limit: Optional[int] = None


class Route(Asset):
    route: RouteData
    _ouro: Optional["Ouro"] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ouro = kwargs.get("_ouro")

    def read_stats(self) -> Dict:
        """
        Get stats for a route
        """
        if not self._ouro:
            raise RuntimeError("Route object not connected to Ouro client")
        request = self._ouro.client.get(f"/routes/{self.id}/stats")
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]

    def read_actions(self) -> List[Dict]:
        """
        Get actions for a route
        """
        if not self._ouro:
            raise RuntimeError("Route object not connected to Ouro client")
        request = self._ouro.client.get(
            f"/services/{self.parent_id}/routes/{self.id}/actions"
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]

    def read_analytics(self) -> Dict:
        """
        Get analytics for a route
        """
        if not self._ouro:
            raise RuntimeError("Route object not connected to Ouro client")
        request = self._ouro.client.get(
            f"/services/{self.parent_id}/routes/{self.id}/analytics"
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]

    def read_cost(self, asset_id) -> Dict:
        """
        Calculate the cost for a route
        """
        if not self._ouro:
            raise RuntimeError("Route object not connected to Ouro client")
        request = self._ouro.client.get(
            f"/services/{self.parent_id}/routes/{self.id}/cost?input={asset_id}"
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]

    def use(self, **kwargs) -> Dict:
        """
        Use/execute this route
        """
        if not self._ouro:
            raise RuntimeError("Route object not connected to Ouro client")
        return self._ouro.routes.use(str(self.id), **kwargs)
