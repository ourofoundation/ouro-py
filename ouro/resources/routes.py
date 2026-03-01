from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Union

from ouro._constants import DEFAULT_TIMEOUT
from ouro._resource import SyncAPIResource
from ouro.models import Action, Route
from ouro.utils import is_valid_uuid

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Routes"]

DEFAULT_POLL_INTERVAL = 10.0  # seconds
DEFAULT_POLL_TIMEOUT = 600.0  # 10 minutes


class Routes(SyncAPIResource):
    def _resolve_name_to_id(self, name_or_id: str, asset_type: str) -> str:
        """Resolve a name to an ID using the backend endpoint."""
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
            return self._handle_response(request)["id"]

    def retrieve(self, name_or_id: str) -> Route:
        """Retrieve a Route by its name or ID."""
        route_id = self._resolve_name_to_id(name_or_id, "route")
        request = self.client.get(f"/routes/{route_id}")
        return Route(**self._handle_response(request), _ouro=self.ouro)

    def update(self, id: str, **kwargs) -> Route:
        """Update a route."""
        route = self.retrieve(id)
        service_id = route.parent_id
        request = self.client.put(
            f"/services/{service_id}/routes/{route.id}",
            json=kwargs,
        )
        return Route(**self._handle_response(request), _ouro=self.ouro)

    def create(self, service_id: str, **kwargs) -> Route:
        """Create a new route for a service."""
        request = self.client.post(
            f"/services/{service_id}/routes/create",
            json=kwargs,
        )
        return Route(**self._handle_response(request), _ouro=self.ouro)

    def retrieve_action(self, action_id: str) -> Action:
        """Retrieve an action by its ID to check its status and response."""
        request = self.client.get(f"/actions/{action_id}")
        return Action(**self._handle_response(request), _ouro=self.ouro)

    def poll_action(
        self,
        action_id: str,
        *,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
        raise_on_error: bool = True,
    ) -> Action:
        """
        Poll an action until it completes (status is 'success' or 'error').

        Args:
            action_id: The ID of the action to poll
            poll_interval: Seconds between status checks (default: 10.0)
            timeout: Maximum seconds to wait (default: 600). None = wait forever.
            raise_on_error: If True, raise an exception when action status is 'error'
        """
        start_time = time.time()

        while True:
            action = self.retrieve_action(action_id)

            if action.is_complete:
                if raise_on_error and action.is_error:
                    error_msg = action.response if action.response else "Action failed"
                    raise Exception(f"Action failed: {error_msg}")
                return action

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(
                        f"Action {action_id} did not complete within {timeout} seconds. "
                        f"Current status: {action.status}"
                    )

            log.debug(
                f"Action {action_id} status: {action.status}, "
                f"waiting {poll_interval}s before next check..."
            )
            time.sleep(poll_interval)

    def use(
        self,
        name_or_id: str,
        body: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        output: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
        wait: bool = True,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
        **kwargs,
    ) -> Union[Dict, Action]:
        """
        Use/execute a specific route by its name or ID.
        The route name should be in the format "entity_name/route_name".

        For routes that return 202 (async processing), this method will automatically
        poll for updates until the action completes, unless wait=False.

        Args:
            name_or_id: Route name ("entity_name/route_name") or UUID
            body: Request body data
            query: Query parameters
            params: URL parameters
            output: Output configuration
            timeout: HTTP request timeout in seconds
            wait: If True (default), wait for async routes to complete
            poll_interval: Seconds between status checks when waiting (default: 10.0)
            poll_timeout: Maximum seconds to wait for completion (default: 600)
            **kwargs: Additional keyword arguments to send to the route
        """
        route_id = self._resolve_name_to_id(name_or_id, "route")
        route = self.retrieve(route_id)

        payload = {
            "config": {
                "body": body,
                "query": query,
                "params": params,
                "output": output,
                **kwargs,
            },
            "async": False,
        }
        request_timeout = timeout or DEFAULT_TIMEOUT
        http_response = self.client.post(
            f"/services/{route.parent_id}/routes/{route_id}/use",
            json=payload,
            timeout=request_timeout,
        )

        # Use raw=True because we need status_code and metadata beyond just "data"
        response = self._handle_response(http_response, raw=True)

        metadata = response.get("metadata") or {}
        is_async = http_response.status_code == 202 or metadata.get(
            "requiresPolling", False
        )
        action_data = response.get("action")

        if is_async and action_data:
            action = Action(**action_data, _ouro=self.ouro)
            log.info(
                f"Route returned 202 Accepted. Action ID: {action.id}, "
                f"status: {action.status}"
            )

            if wait:
                completed_action = self.poll_action(
                    str(action.id),
                    poll_interval=poll_interval,
                    timeout=poll_timeout,
                )
                return completed_action.response
            else:
                return action

        data = response.get("data")
        if isinstance(data, dict):
            response_data = data.get("responseData")
            if response_data is not None:
                return response_data
            return data
        return data
