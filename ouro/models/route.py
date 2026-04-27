from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pydantic import BaseModel

from .asset import Asset

if TYPE_CHECKING:
    from ouro import Ouro
    from ouro.models.action import Action

# Kept in sync with ``ouro.resources.routes.DEFAULT_POLL_INTERVAL`` /
# ``DEFAULT_POLL_TIMEOUT``. Imported lazily in ``use`` to avoid a circular
# import between ``ouro.models`` and ``ouro.resources``.


class RouteData(BaseModel):
    description: Optional[str] = None
    path: str
    method: str
    parameters: Optional[List[Dict]] = None
    request_body: Optional[Dict] = {}
    responses: Optional[Dict] = None
    security: Optional[str] = None
    input_assets: Optional[Dict[str, Dict[str, Any]]] = None
    input_type: Optional[str] = None
    input_filter: Optional[str] = None
    input_file_extension: Optional[str] = None
    input_file_extensions: Optional[List[str]] = None
    output_type: Optional[str] = None
    output_file_extension: Optional[str] = None
    rate_limit: Optional[int] = None


class Route(Asset):
    route: Optional[RouteData] = None
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

    def read_actions(self) -> List["Action"]:
        """Get actions for a route."""
        ouro = self._require_client()
        return ouro.routes.list_actions(str(self.id))

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
        poll_interval: Optional[float] = None,
        poll_timeout: Optional[float] = None,
        **kwargs,
    ) -> Union[Dict, "Action"]:
        """Use/execute this route.

        For routes that return 202 (async processing), this method will automatically
        poll for updates until the action completes, unless wait=False.

        Args:
            wait: If True (default), wait for async routes to complete.
            poll_interval: Seconds between status checks when waiting. Defaults
                to :data:`ouro.resources.routes.DEFAULT_POLL_INTERVAL` (10s).
            poll_timeout: Maximum seconds to wait for completion. Defaults to
                :data:`ouro.resources.routes.DEFAULT_POLL_TIMEOUT` (600s).
            **kwargs: Additional arguments (body, query, params, output, timeout).
        """
        from ouro.resources.routes import (
            DEFAULT_POLL_INTERVAL,
            DEFAULT_POLL_TIMEOUT,
        )

        ouro = self._require_client()
        return ouro.routes.use(
            str(self.id),
            wait=wait,
            poll_interval=DEFAULT_POLL_INTERVAL if poll_interval is None else poll_interval,
            poll_timeout=DEFAULT_POLL_TIMEOUT if poll_timeout is None else poll_timeout,
            **kwargs,
        )
