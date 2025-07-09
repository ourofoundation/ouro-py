import logging
from typing import Any, Dict, Optional

from ouro._constants import DEFAULT_TIMEOUT
from ouro._resource import SyncAPIResource
from ouro.models import Route
from ouro.utils import is_valid_uuid

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Routes"]


class Routes(SyncAPIResource):
    def _resolve_name_to_id(self, name_or_id: str, asset_type: str) -> str:
        """
        Resolve a name to an ID using the backend endpoint
        """
        if is_valid_uuid(name_or_id):
            return name_or_id
        else:
            entity_name, name = name_or_id.split("/", 1)
            request = self.client.post(
                "/elements/common/name-to-id",
                json={
                    "name": name,
                    "assetType": asset_type,
                    "entityName": entity_name,
                },
            )
            request.raise_for_status()
            response = request.json()
            if response["error"]:
                raise Exception(response["error"])
            return response["data"]["id"]

    def retrieve(self, name_or_id: str) -> Route:
        """
        Retrieve a Route by its ID
        """
        route_id = self._resolve_name_to_id(name_or_id, "route")
        request = self.client.get(
            f"/routes/{route_id}",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Route(**response["data"], _ouro=self.ouro)

    def update(self, id: str, **kwargs) -> Route:
        """
        Update a route
        """

        route = self.retrieve(id)
        service_id = route.parent_id
        request = self.client.put(
            f"/services/{service_id}/routes/{route.id}",
            json=kwargs,
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Route(**response["data"], _ouro=self.ouro)

    def create(self, service_id: str, **kwargs) -> Route:
        """
        Create a new route for a service
        """
        request = self.client.post(
            f"/services/{service_id}/routes/create",
            json=kwargs,
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Route(**response["data"], _ouro=self.ouro)

    def use(
        self,
        name_or_id: str,
        body: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> Dict:
        """
        Use/execute a specific route by its name or ID.
        The route name should be in the format "entity_name/route_name".

        Args:
            name_or_id: The name or ID of the route in the format "entity_name/route_name"
            data: The data to send to the route
            **kwargs: Additional keyword arguments to send to the route
        """
        # Get the route ID
        route_id = self._resolve_name_to_id(name_or_id, "route")
        route = self.retrieve(route_id)

        payload = {"config": {"body": body, **kwargs}, "async": False}
        request_timeout = timeout or DEFAULT_TIMEOUT
        request = self.client.post(
            f"/services/{route.parent_id}/routes/{route_id}/use",
            json=payload,
            timeout=request_timeout,
        )
        # request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        # Since we are using the sync version, we will have access to the response data
        return response["data"]["responseData"]
