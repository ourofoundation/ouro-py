from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pydantic import BaseModel

from .asset import Asset

if TYPE_CHECKING:
    from ouro import Ouro
    from ouro.models.action import Action


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

    def _require_client(self) -> "Ouro":
        if not self._ouro:
            raise RuntimeError("Route object not connected to Ouro client")
        return self._ouro

    def _api_get(self, path: str) -> Any:
        """Make a GET request through the centralized response handler."""
        ouro = self._require_client()
        from ouro._resource import SyncAPIResource

        resource = SyncAPIResource(ouro)
        return resource._handle_response(ouro.client.get(path))

    def read_stats(self) -> Dict:
        """Get stats for a route."""
        return self._api_get(f"/routes/{self.id}/stats")

    def read_actions(self) -> List[Dict]:
        """Get actions for a route."""
        return self._api_get(
            f"/services/{self.parent_id}/routes/{self.id}/actions"
        )

    def read_analytics(self) -> Dict:
        """Get analytics for a route."""
        return self._api_get(
            f"/services/{self.parent_id}/routes/{self.id}/analytics"
        )

    def read_cost(self, asset_id: str) -> Dict:
        """Calculate the cost for a route."""
        return self._api_get(
            f"/services/{self.parent_id}/routes/{self.id}/cost?input={asset_id}"
        )

    def use(
        self,
        *,
        wait: bool = True,
        poll_interval: float = 1.0,
        poll_timeout: Optional[float] = 600.0,
        **kwargs,
    ) -> Union[Dict, "Action"]:
        """Use/execute this route.

        For routes that return 202 (async processing), this method will automatically
        poll for updates until the action completes, unless wait=False.

        Args:
            wait: If True (default), wait for async routes to complete.
            poll_interval: Seconds between status checks when waiting (default: 1.0).
            poll_timeout: Maximum seconds to wait for completion (default: 600).
            **kwargs: Additional arguments (body, query, params, output, timeout).
        """
        ouro = self._require_client()
        return ouro.routes.use(
            str(self.id),
            wait=wait,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
            **kwargs,
        )
