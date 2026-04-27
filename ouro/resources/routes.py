from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Union

from ouro._constants import DEFAULT_TIMEOUT
from ouro._exceptions import APIStatusError, ExternalServiceError, RouteExecutionError
from ouro._resource import SyncAPIResource, _strip_none
from ouro.models import Action, ActionLog, Route
from ouro.utils import is_valid_uuid

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Routes"]

DEFAULT_POLL_INTERVAL = 10.0  # seconds
DEFAULT_POLL_TIMEOUT = 600.0  # 10 minutes


def _normalize_input_assets(
    input_assets: Optional[Dict[str, Any]] = None,
    assets: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Normalize ergonomic keyed asset inputs into the API config shape."""
    raw = input_assets if input_assets is not None else assets
    if raw is None:
        return None
    normalized: Dict[str, Any] = {}
    for name, value in raw.items():
        if isinstance(value, str):
            normalized[name] = {"assetId": value}
        elif isinstance(value, dict):
            asset_id = value.get("assetId") or value.get("asset_id") or value.get("id")
            normalized[name] = {"assetId": asset_id, **value} if asset_id else value
        else:
            raise ValueError(
                "input_assets values must be asset IDs or dictionaries "
                f"(got {type(value).__name__} for {name!r})."
            )
    return normalized


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _route_failure_info(response: Any) -> Dict[str, Any]:
    """Extract normalized failure metadata from an action response payload."""
    envelope = response if isinstance(response, dict) else {}
    error = envelope.get("error") if isinstance(envelope.get("error"), dict) else envelope
    status_code = (
        _coerce_int(envelope.get("statusCode"))
        or _coerce_int(error.get("statusCode"))
        or _coerce_int(error.get("status"))
        or _coerce_int(error.get("upstreamStatus"))
    )
    code = error.get("code")
    error_type = error.get("type")
    message = (
        error.get("message")
        or error.get("detail")
        or envelope.get("message")
        or "Action failed"
    )
    service_url = error.get("serviceUrl")
    retryable = error.get("retryable")
    if retryable is None and status_code is not None:
        retryable = status_code in {408, 429, 500, 502, 503, 504}

    is_external = (
        error_type == "external_service_error"
        or service_url is not None
        or (isinstance(code, str) and code.startswith("external_service"))
    )
    return {
        "message": str(message),
        "status_code": status_code,
        "code": code,
        "type": error_type,
        "service_url": service_url,
        "retryable": retryable,
        "is_external": is_external,
    }


class Routes(SyncAPIResource):
    def list(
        self,
        query: str = "",
        limit: int = 20,
        offset: int = 0,
        scope: Optional[str] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        sort: Optional[str] = None,
        time_window: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Route]:
        """List routes, optionally filtered by search query and scope.

        Results include base asset fields but not full route definitions.
        Use ``retrieve()`` for the complete route with path, method, and parameters.

        Args:
            sort: "relevant" | "recent" | "popular" | "updated"
            time_window: For sort="popular": "day" | "week" | "month" | "all".
                         Default: "month".
        """
        results = self.ouro.assets.search(
            query=query,
            asset_type="route",
            limit=limit,
            offset=offset,
            scope=scope,
            org_id=org_id,
            team_id=team_id,
            sort=sort,
            time_window=time_window,
            **kwargs,
        )
        return [Route(**item, _ouro=self.ouro) for item in results]

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

    def list_actions(
        self,
        route_id: str,
        *,
        include_other_users: bool = False,
        exclude_self: bool = False,
        limit: int = 20,
        offset: int = 0,
        with_pagination: bool = False,
    ) -> Union[List[Action], Dict[str, Any]]:
        """List executions/actions for a route.

        By default, the backend returns only actions owned by the authenticated
        user. Set ``include_other_users=True`` to include visible actions from
        other users as well.
        """
        route = self.retrieve(route_id)
        if not route.parent_id:
            raise ValueError("Route has no parent service; cannot list actions.")

        params = {
            "global": "true" if include_other_users else "false",
            "exclude_self": "true" if exclude_self else None,
            "limit": limit,
            "offset": offset,
        }
        request = self.client.get(
            f"/services/{route.parent_id}/routes/{route.id}/actions",
            params=_strip_none(params),
        )

        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            if not isinstance(result, dict):
                return {"data": [], "pagination": None}
            items = result.get("data") or []
            result["data"] = [Action(**item, _ouro=self.ouro) for item in items]
            return result

        data = self._handle_response(request) or []
        return [Action(**item, _ouro=self.ouro) for item in data]

    def get_action_logs(
        self,
        action_id: str,
        *,
        level: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        sort_order: str = "desc",
        chronological: Optional[bool] = None,
        with_pagination: bool = False,
    ) -> Union[List[ActionLog], Dict[str, Any]]:
        """Read logs for a route action."""
        if chronological is not None:
            sort_order = "asc" if chronological else "desc"
        if sort_order not in {"asc", "desc"}:
            raise ValueError("sort_order must be 'asc' or 'desc'")

        params = {
            "level": level,
            "limit": limit,
            "offset": offset,
            "sort_order": sort_order,
        }
        request = self.client.get(
            f"/actions/{action_id}/logs",
            params=_strip_none(params),
        )

        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            if not isinstance(result, dict):
                return {"data": [], "pagination": None}
            items = result.get("data") or []
            result["data"] = [ActionLog.model_validate(item) for item in items]
            return result

        data = self._handle_response(request) or []
        return [ActionLog.model_validate(item) for item in data]

    def poll_action(
        self,
        action_id: str,
        *,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
        raise_on_error: bool = True,
    ) -> Action:
        """
        Poll an action until it completes (status is 'success', 'error', or 'timed-out').

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
                    failure = _route_failure_info(action.response)
                    error_cls = (
                        ExternalServiceError
                        if failure["is_external"]
                        else RouteExecutionError
                    )
                    kwargs = {
                        "action_id": str(action.id),
                        "status": action.status,
                        "response": action.response,
                        "retryable": failure["retryable"],
                    }
                    if error_cls is ExternalServiceError:
                        kwargs.update(
                            {
                                "status_code": failure["status_code"],
                                "service_url": failure["service_url"],
                                "code": failure["code"],
                            }
                        )
                    raise error_cls(
                        f"Action failed: {failure['message']}",
                        **kwargs,
                    )
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

    def execute(
        self,
        name_or_id: str,
        body: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        output: Optional[Dict[str, Any]] = None,
        input_assets: Optional[Dict[str, Any]] = None,
        assets: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
        **kwargs,
    ) -> Action:
        """
        Execute a route and return the full :class:`Action`.

        Unlike :meth:`use`, this always returns an :class:`Action` with metadata
        (``id``, ``status``, ``response``, ``output_asset``, timestamps) so callers
        can reference the action afterwards — e.g. to poll, log, or embed a route
        preview pinned to this action.

        Handles both sync 200 and async 202 responses transparently: for 202 it
        polls until the action reaches a terminal state (``success`` / ``error``
        / ``timed-out``). For a dict return shaped like :meth:`use`, read
        ``action.final_data``.

        Args:
            name_or_id: Route name ("entity_name/route_name") or UUID
            body: Request body data
            query: Query parameters
            params: URL parameters
            output: Output configuration
            timeout: HTTP request timeout in seconds for the initial call
            poll_interval: Seconds between status checks while waiting (default: 10.0)
            poll_timeout: Maximum seconds to wait for async completion (default: 600)
            **kwargs: Additional keyword arguments to send to the route

        Raises:
            TimeoutError: If the action doesn't reach a terminal state within
                ``poll_timeout``. The action keeps running server-side; call
                :meth:`retrieve_action` or :meth:`poll_action` later with the
                id from the raised exception's ``action_id`` attribute.
        """
        route_id = self._resolve_name_to_id(name_or_id, "route")
        route = self.retrieve(route_id)
        normalized_input_assets = _normalize_input_assets(input_assets, assets)

        payload = {
            "config": {
                "body": body,
                "query": query,
                "parameters": params,
                "params": params,
                "output": output,
                "input_assets": normalized_input_assets,
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
        try:
            envelope = self._handle_response(http_response, raw=True)
        except APIStatusError as exc:
            body = getattr(exc, "body", None)
            if isinstance(body, dict) and isinstance(body.get("action"), dict):
                envelope = body
            else:
                raise
        envelope = envelope if isinstance(envelope, dict) else {}

        metadata = envelope.get("metadata") or {}
        action_data = envelope.get("action") or {}
        is_async = http_response.status_code == 202 or metadata.get(
            "requiresPolling", False
        )

        if is_async and action_data:
            action = Action(**action_data, _ouro=self.ouro)
            log.info(
                f"Route returned 202 Accepted. Action ID: {action.id}, "
                f"status: {action.status}"
            )
            try:
                return self.poll_action(
                    str(action.id),
                    poll_interval=poll_interval,
                    timeout=poll_timeout,
                    raise_on_error=False,
                )
            except TimeoutError as exc:
                # Attach the action id so callers can resume polling later.
                setattr(exc, "action_id", str(action.id))
                raise

        # Sync 200 path — synthesize an Action from the envelope. The backend
        # always returns `action` (see backend/src/controllers/elements/routes.ts)
        # and may report a side-effect asset via `metadata.sideEffect`.
        if action_data:
            data = envelope.get("data")
            response_payload: Any
            if isinstance(data, dict) and "responseData" in data:
                response_payload = data.get("responseData")
            else:
                response_payload = data

            action_kwargs: Dict[str, Any] = dict(action_data)
            action_kwargs.setdefault("route_id", route_id)
            if not action_kwargs.get("user_id"):
                current_user = getattr(self.ouro, "user", None)
                user_id = (
                    getattr(current_user, "user_id", None)
                    or getattr(current_user, "id", None)
                )
                if user_id:
                    action_kwargs["user_id"] = user_id
            action_kwargs.setdefault("response", response_payload)

            side_effect = metadata.get("sideEffect") or {}
            if side_effect and not action_kwargs.get("output_asset"):
                obj_type = side_effect.get("object")
                if obj_type and isinstance(side_effect.get(obj_type), dict):
                    action_kwargs["output_asset"] = side_effect[obj_type]

            return Action(**action_kwargs, _ouro=self.ouro)

        # Last-resort fallback: backend didn't return action metadata at all.
        # Surface an Action-shaped failure rather than a raw dict so callers can
        # rely on the return type.
        raise RuntimeError(
            f"Route {route.name} returned no action metadata; "
            f"response: {envelope.get('data')}"
        )

    def use(
        self,
        name_or_id: str,
        body: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        output: Optional[Dict[str, Any]] = None,
        input_assets: Optional[Dict[str, Any]] = None,
        assets: Optional[Dict[str, Any]] = None,
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

        For programmatic access to the full action (id, status, output asset),
        prefer :meth:`execute` — it always returns an :class:`Action`. Use
        ``action.final_data`` if you need the same dict shape this method
        returns.

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
        normalized_input_assets = _normalize_input_assets(input_assets, assets)

        payload = {
            "config": {
                "body": body,
                "query": query,
                "parameters": params,
                "params": params,
                "output": output,
                "input_assets": normalized_input_assets,
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
                response_data = completed_action.response
                if completed_action.output_asset:
                    if not isinstance(response_data, dict):
                        response_data = {"_raw": response_data} if response_data is not None else {}
                    asset_type = completed_action.output_asset.get("asset_type")
                    if asset_type:
                        response_data[asset_type] = completed_action.output_asset
                return response_data
            else:
                return action

        data = response.get("data")
        if isinstance(data, dict):
            response_data = data.get("responseData")
            if response_data is not None:
                return response_data
            return data
        return data
