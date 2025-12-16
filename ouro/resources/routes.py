import logging
import time
from typing import Any, Dict, Optional, Union

from ouro._constants import DEFAULT_TIMEOUT
from ouro._resource import SyncAPIResource
from ouro.models import Action, Route
from ouro.utils import is_valid_uuid

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Routes"]

# Default polling settings
DEFAULT_POLL_INTERVAL = 10.0  # seconds
DEFAULT_POLL_TIMEOUT = 600.0  # 10 minutes


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

    def retrieve_action(self, action_id: str) -> Action:
        """
        Retrieve an action by its ID to check its status and response.

        Args:
            action_id: The ID of the action to retrieve

        Returns:
            Action object with current status and response data
        """
        request = self.client.get(f"/actions/{action_id}")
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Action(**response["data"], _ouro=self.ouro)

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
            poll_interval: Seconds between status checks (default: 1.0)
            timeout: Maximum seconds to wait (default: 600). None = wait forever.
            raise_on_error: If True, raise an exception when action status is 'error'

        Returns:
            The completed Action

        Raises:
            TimeoutError: If timeout is reached before completion
            Exception: If raise_on_error=True and the action completed with an error
        """
        start_time = time.time()

        while True:
            action = self.retrieve_action(action_id)

            if action.is_complete:
                if raise_on_error and action.is_error:
                    error_msg = action.response if action.response else "Action failed"
                    raise Exception(f"Action failed: {error_msg}")
                return action

            # Check timeout
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
            name_or_id: The name or ID of the route in the format "entity_name/route_name"
            body: Request body data
            query: Query parameters
            params: URL parameters
            output: Output configuration
            timeout: HTTP request timeout in seconds
            wait: If True (default), wait for async routes to complete. If False,
                  return the Action immediately for manual polling.
            poll_interval: Seconds between status checks when waiting (default: 1.0)
            poll_timeout: Maximum seconds to wait for completion (default: 600).
                         Set to None to wait forever.
            **kwargs: Additional keyword arguments to send to the route

        Returns:
            If the route returns immediately: Dict with response data
            If the route is async and wait=True: Dict with response data
            If the route is async and wait=False: Action object for manual polling
        """
        # Get the route ID
        route_id = self._resolve_name_to_id(name_or_id, "route")
        route = self.retrieve(route_id)

        payload = {
            # Route config
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

        response = http_response.json()
        if response.get("error"):
            raise Exception(response["error"])

        # Check if this is an async response (202 Accepted)
        # The backend returns 202 status code and includes the action in the response
        is_async = http_response.status_code == 202 or response.get("metadata", {}).get(
            "requiresPolling", False
        )
        action_data = response.get("action")

        if is_async and action_data:
            # This is an async route - create an Action object
            action = Action(**action_data, _ouro=self.ouro)
            log.info(
                f"Route returned 202 Accepted. Action ID: {action.id}, "
                f"status: {action.status}"
            )

            if wait:
                # Poll until completion and return the response data
                completed_action = self.poll_action(
                    str(action.id),
                    poll_interval=poll_interval,
                    timeout=poll_timeout,
                )
                return completed_action.response
            else:
                # Return the action for manual polling
                return action

        # Sync response - return the response data directly
        data = response.get("data", {})
        return data.get("responseData", data)
