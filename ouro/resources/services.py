import logging
from typing import Dict, List

from ouro._resource import SyncAPIResource
from ouro.models import Route, Service
from ouro.resources.routes import Routes

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Services"]


class Services(SyncAPIResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.routes = Routes(*args, **kwargs)

    def retrieve(self, id: str) -> Service:
        """
        Retrieve a Service by its ID
        """
        request = self.client.get(
            f"/services/{id}",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Service(**response["data"], _ouro=self.ouro)

    def list(self) -> List[Service]:
        """
        List all services in the current context
        """
        request = self.client.get("/services")
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return [Service(**service, _ouro=self.ouro) for service in response["data"]]

    def read_spec(self, id: str) -> Dict:
        """
        Get the OpenAPI specification for a service
        """
        request = self.client.get(f"/services/{id}/spec")
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]

    def read_routes(self, id: str) -> List[Route]:
        """
        Get all routes for a service
        """
        request = self.client.get(f"/services/{id}/routes")
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return [Route(**route, _ouro=self.ouro) for route in response["data"]]
