from __future__ import annotations

import logging
import time
from datetime import datetime
import warnings
from typing import Any, Dict, List, Optional, Union

from ouro._constants import (
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_TIMEOUT,
    DEFAULT_TIMEOUT,
)
from ouro._exceptions import APIStatusError, ExternalServiceError, RouteExecutionError
from ouro._resource import (
    SyncAPIResource,
    _coerce_description,
    _ensure_attribution,
    _optional_attribution,
    _strip_none,
)
from ouro.models import Action, ActionLog, DeleteResult, Page, Route, RouteCost, RouteStats
from ouro.utils import is_valid_uuid

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Routes"]

# First wait between status checks; doubles up to ``poll_interval`` so quick
# actions return quickly without polling slow ones any harder.
POLL_RAMP_START = 0.5  # seconds
# In-app notification levels a caller may ask for when executing a route.
NOTIFY_LEVELS = ("all", "failure", "none")
_COMPAT_INPUT_ASSET_METADATA_KEYS = {
    "assetType",
    "asset_type",
}


def _normalize_input_assets(
    input_assets: Optional[Dict[str, Any]] = None,
    assets: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Normalize keyed asset IDs into the API config shape.

    Prefer ``input_assets={"name": asset_id}``. Object values are kept for
    compatibility with older or under-declared routes.
    """
    raw = input_assets if input_assets is not None else assets
    if raw is None:
        return None
    normalized: Dict[str, Any] = {}
    for name, value in raw.items():
        if isinstance(value, str):
            normalized[name] = {"assetId": value}
        elif isinstance(value, dict):
            asset_id = value.get("assetId") or value.get("asset_id") or value.get("id")
            metadata_keys = _COMPAT_INPUT_ASSET_METADATA_KEYS.intersection(value)
            if metadata_keys:
                warnings.warn(
                    "Passing input_assets object metadata "
                    f"({', '.join(sorted(metadata_keys))}) is deprecated. "
                    "Declare asset type on the route instead, "
                    "and pass bare asset IDs from callers.",
                    DeprecationWarning,
                    stacklevel=2,
                )
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


def _adaptive_poll_params(
    route: Optional[Route],
    poll_interval: Optional[float],
    poll_timeout: Optional[float],
) -> tuple[float, Optional[float]]:
    """Resolve poll_interval / poll_timeout, preferring caller-supplied values
    and falling back to per-route latency metrics from ``asset_metrics`` so
    fast routes poll fast and slow routes poll slow.

    Falls back to ``DEFAULT_POLL_INTERVAL`` / ``DEFAULT_POLL_TIMEOUT`` when
    no metrics are available yet.
    """
    avg_completion_ms = None
    p95_completion_ms = None
    metrics = getattr(route, "metrics", None) if route is not None else None
    if metrics is not None:
        avg_completion_ms = getattr(metrics, "avg_completion_ms", None)
        p95_completion_ms = getattr(metrics, "p95_completion_ms", None)

    if poll_interval is None:
        if avg_completion_ms:
            # Aim for ~5 polls across the average duration, clamped to
            # something humans-can-stand: at least 1s, at most 30s.
            poll_interval = max(1.0, min(30.0, (avg_completion_ms / 1000.0) / 5.0))
        else:
            poll_interval = DEFAULT_POLL_INTERVAL

    if poll_timeout is None:
        if p95_completion_ms:
            # 2x p95 covers the long tail without waiting forever.
            poll_timeout = max(60.0, (p95_completion_ms / 1000.0) * 2.0)
        else:
            poll_timeout = DEFAULT_POLL_TIMEOUT

    return poll_interval, poll_timeout


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
    retryable = error.get("retryable")
    if retryable is None and status_code is not None:
        retryable = status_code in {408, 429, 500, 502, 503, 504}

    is_external = error_type == "external_service_error" or (
        isinstance(code, str) and code.startswith("external_service")
    )
    return {
        "message": str(message),
        "status_code": status_code,
        "code": code,
        "type": error_type,
        "retryable": retryable,
        "is_external": is_external,
    }


def _raise_action_failure(action: Action) -> None:
    failure = _route_failure_info(action.response)
    error_cls = (
        ExternalServiceError if failure["is_external"] else RouteExecutionError
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
                "code": failure["code"],
            }
        )
    raise error_cls(
        f"Action failed: {failure['message']}",
        **kwargs,
    )


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
    ) -> Page[Route]:
        """List routes, optionally filtered by search query and scope.

        Results include base asset fields but not full route definitions.
        Use ``retrieve()`` for the complete route with path, method, and parameters.

        Args:
            sort: "relevant" | "recent" | "popular" | "updated"
            time_window: For sort="popular": "day" | "week" | "month" | "all".
                         Default: "month".
        """
        return self.ouro.assets._search(
            Route,
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
        return self._parse(Route, self._handle_response(request))

    def create(
        self,
        service_id: str,
        method: str,
        path: str,
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
        request_body: Optional[Dict[str, Any]] = None,
        input_assets: Optional[Dict[str, Any]] = None,
        output_assets: Optional[Dict[str, Any]] = None,
        execution_mode: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        price_currency: Optional[str] = None,
        cost_accounting: Optional[str] = None,
        cost_unit: Optional[str] = None,
        unit_cost: Optional[float] = None,
        unit_cost_usd: Optional[float] = None,
        unit_cost_sats: Optional[float] = None,
        max_billable_seconds: Optional[int] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> Route:
        """Create a new route on a service.

        ``method`` + ``path`` must be unique within the service. ``org_id``
        and ``team_id`` default to the parent service's values when omitted.
        ``visibility`` defaults to ``"inherit"`` so the route tracks the
        service. ``input_assets`` / ``output_assets`` are keyed asset
        declarations, e.g. ``{"structure": {"asset_type": "file"}}``.
        ``execution_mode`` is ``"sync"`` (default) or ``"async"``.

        To charge per call, pass ``visibility="monetized"``,
        ``monetization="pay-per-use"``, ``cost_accounting="fixed"``, and a
        price: ``unit_cost_usd`` (dollars), ``unit_cost_sats`` (sats), or
        both to let callers pick which currency to pay in. With both,
        ``price_currency`` is the one charged when a caller doesn't pick.
        ``unit_cost`` with ``price_currency`` still sets a single price.
        To charge per second of runtime use ``cost_accounting="runtime"``
        with ``max_billable_seconds``.
        """
        if visibility is None:
            visibility = "inherit"
        if org_id is None or team_id is None:
            service = self.ouro.services.retrieve(service_id)
            if org_id is None and service.org_id:
                org_id = str(service.org_id)
            if team_id is None and service.team_id:
                team_id = str(service.team_id)

        route = _strip_none(
            {
                "method": method,
                "path": path,
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "parameters": parameters,
                "request_body": request_body,
                "input_assets": input_assets,
                "output_assets": output_assets,
                "execution_mode": execution_mode,
                "monetization": monetization,
                "price": price,
                "price_currency": price_currency,
                "cost_accounting": cost_accounting,
                "cost_unit": cost_unit,
                "unit_cost": unit_cost,
                "unit_cost_usd": unit_cost_usd,
                "unit_cost_sats": unit_cost_sats,
                "max_billable_seconds": max_billable_seconds,
                "org_id": org_id,
                "team_id": team_id,
                "license_id": license_id,
                **kwargs,
                "source": "api",
                "asset_type": "route",
            }
        )
        route["attribution"] = _ensure_attribution(attribution)
        # Org and team come from the parent service; only check the pin.
        route = self._scope_create(route, default_team=False)

        request = self.client.post(
            f"/services/{service_id}/routes/create",
            json={"route": route},
        )
        return self._parse(Route, self._handle_response(request))

    def update(
        self,
        id: str,
        method: Optional[str] = None,
        path: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
        request_body: Optional[Dict[str, Any]] = None,
        input_assets: Optional[Dict[str, Any]] = None,
        output_assets: Optional[Dict[str, Any]] = None,
        execution_mode: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        price_currency: Optional[str] = None,
        cost_accounting: Optional[str] = None,
        cost_unit: Optional[str] = None,
        unit_cost: Optional[float] = None,
        unit_cost_usd: Optional[float] = None,
        unit_cost_sats: Optional[float] = None,
        max_billable_seconds: Optional[int] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> Route:
        """Update a route by its ID or ``"entity_name/route_name"`` identifier.

        Only the fields you pass are changed. The backend re-derives the
        route's display name from ``name`` (or ``method``/``path``), so the
        current name is preserved when ``name`` is omitted.

        ``unit_cost_usd`` / ``unit_cost_sats`` price the route per currency;
        pass ``0`` to stop selling in one.
        """
        existing = self.retrieve(id)
        service_id = existing.parent_id

        route = _strip_none(
            {
                "id": str(existing.id),
                "method": method,
                "path": path,
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "parameters": parameters,
                "request_body": request_body,
                "input_assets": input_assets,
                "output_assets": output_assets,
                "execution_mode": execution_mode,
                "monetization": monetization,
                "price": price,
                "price_currency": price_currency,
                "cost_accounting": cost_accounting,
                "cost_unit": cost_unit,
                "unit_cost": unit_cost,
                "unit_cost_usd": unit_cost_usd,
                "unit_cost_sats": unit_cost_sats,
                "max_billable_seconds": max_billable_seconds,
                "license_id": license_id,
                "attribution": _optional_attribution(attribution),
                **kwargs,
            }
        )
        # The backend recomputes the display name (and URL slug) on every
        # update, defaulting to "{method} {path}" when name is absent; carry the
        # existing name forward so metadata-only updates don't rename the route.
        if "name" not in route:
            route["name"] = existing.name

        self._scope_update(route)
        request = self.client.put(
            f"/services/{service_id}/routes/{existing.id}",
            json={"route": route},
        )
        return self._parse(Route, self._handle_response(request))

    def delete(
        self, id: str, *, delete_children: bool = False, dry_run: bool = False
    ) -> DeleteResult:
        """Delete a Route by its id.

        Args:
            id: Route UUID.
            delete_children: When True, also delete child assets linked via
                ``parent_id``.
            dry_run: When True, return the delete summary without deleting.

        Returns:
            What was deleted, or would be when ``dry_run`` is true.
        """
        return self._delete(
            f"/routes/{id}", delete_children=delete_children, dry_run=dry_run
        )

    def retrieve_action(self, action_id: str) -> Action:
        """Retrieve an action by its ID to check its status and response."""
        request = self.client.get(f"/actions/{action_id}")
        return self._parse(Action, self._handle_response(request))

    def list_actions(
        self,
        route_id: str,
        *,
        include_other_users: bool = False,
        exclude_self: bool = False,
        status: Optional[Union[str, List[str]]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Page[Action]:
        """List a page of executions/actions for a route.

        By default, the backend returns only actions owned by the authenticated
        user. Set ``include_other_users=True`` to include visible actions from
        other users as well. ``status`` keeps one status or a list of them
        ("queued" | "in-progress" | "success" | "error" | "timed-out").
        """
        if isinstance(status, (list, tuple)):
            status = ",".join(status)
        route = self.retrieve(route_id)
        if not route.parent_id:
            raise ValueError("Route has no parent service; cannot list actions.")

        params = {
            "global": "true" if include_other_users else "false",
            "exclude_self": "true" if exclude_self else None,
            "status": status or None,
            "limit": limit,
            "offset": offset,
        }
        request = self.client.get(
            f"/services/{route.parent_id}/routes/{route.id}/actions",
            params=_strip_none(params),
        )
        return self._page(Page[Action], self._handle_response(request, raw=True))

    def list_my_actions(
        self,
        *,
        status: Optional[Union[str, List[str]]] = None,
        since: Optional[Union[str, datetime]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Page[Action]:
        """List your own actions across every route, newest first.

        Use this to find runs you started earlier without knowing their route,
        e.g. ``status=["queued", "in-progress"]`` for everything still running.

        Args:
            status: One status or a list of them: "queued" | "in-progress" |
                "success" | "error" | "timed-out". All statuses by default.
            since: Only actions created at or after this time (datetime or
                ISO 8601 string).
        """
        if isinstance(status, (list, tuple)):
            status = ",".join(status)
        if isinstance(since, datetime):
            since = since.isoformat()
        params = {
            "status": status or None,
            "since": since,
            "limit": limit,
            "offset": offset,
        }
        request = self.client.get("/actions", params=_strip_none(params))
        return self._page(Page[Action], self._handle_response(request, raw=True))

    def get_action_logs(
        self,
        action_id: str,
        *,
        level: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        sort_order: str = "desc",
        chronological: Optional[bool] = None,
    ) -> Page[ActionLog]:
        """Read a page of logs for a route action."""
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
        return self._page(Page[ActionLog], self._handle_response(request, raw=True))

    def stats(self, id: str) -> RouteStats:
        """Usage counts plus today's in-progress and recent actions for a route."""
        request = self.client.get(f"/routes/{id}/stats")
        return self._parse(RouteStats, self._handle_response(request))

    def cost(
        self, id: str, asset_id: str, currency: Optional[str] = None
    ) -> RouteCost:
        """Price of running a variable-cost route on the input asset ``asset_id``.

        ``currency`` (``"usd"`` or ``"btc"``) prices the run in that currency
        for routes sold in both; the route's primary currency by default.
        """
        route = self.retrieve(id)
        params = {"input": asset_id}
        if currency:
            params["currency"] = currency
        request = self.client.get(
            f"/services/{route.parent_id}/routes/{route.id}/cost",
            params=params,
        )
        return self._parse(RouteCost, (self._handle_response(request) or {}).get("cost"))

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

        Running out of ``timeout`` here is not an outcome: the action is still
        running and can be polled again. That is unrelated to the final
        ``timed-out`` status, where Ouro itself gave up on a silent run.

        Args:
            action_id: The ID of the action to poll
            poll_interval: Seconds between status checks (default: 10.0). The
                first checks come sooner, backing off to this interval.
            timeout: Maximum seconds to wait (default: 600). None = wait forever.
            raise_on_error: If True, raise an exception when action status is 'error'

        Raises:
            TimeoutError: If ``timeout`` passes first. The action keeps
                running; its id is on the exception's ``action_id``.
        """
        start_time = time.time()
        delay = min(poll_interval, POLL_RAMP_START)

        while True:
            action = self.retrieve_action(action_id)

            if action.is_complete:
                if raise_on_error and action.is_error:
                    _raise_action_failure(action)
                return action

            sleep_for = delay
            if timeout is not None:
                remaining = timeout - (time.time() - start_time)
                if remaining <= 0:
                    exc = TimeoutError(
                        f"Stopped waiting for action {action_id} after {timeout} "
                        f"seconds. It is still running (status: {action.status}); "
                        "poll it again for the result."
                    )
                    setattr(exc, "action_id", str(action_id))
                    raise exc
                sleep_for = min(delay, remaining)

            log.debug(
                f"Action {action_id} status: {action.status}, "
                f"waiting {sleep_for}s before next check..."
            )
            time.sleep(sleep_for)
            delay = min(poll_interval, delay * 2)

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
        wait: bool = True,
        timeout: Optional[float] = None,
        poll_interval: Optional[float] = None,
        poll_timeout: Optional[float] = None,
        raise_on_error: bool = False,
        currency: Optional[str] = None,
        notify: Optional[str] = None,
        **kwargs,
    ) -> Action:
        """
        Execute a route and return the full :class:`Action`.

        The :class:`Action` carries ``id``, ``status``, ``response``,
        ``output_asset``, and timestamps so callers can reference it afterwards — e.g. to poll, log, or embed a route
        preview pinned to this action.

        Handles both sync and async routes transparently. Routes declared
        ``async`` always get the action handle back first (``Prefer:
        respond-async``) and, when ``wait=True``, are polled from here until
        they reach a terminal state, so a long run never depends on one HTTP
        request staying open and its id is never lost. Sync routes answer
        inline. When ``wait=False`` the handle comes back immediately for
        either kind — useful for long-running routes where you want to do
        something else and check back later via :meth:`retrieve_action` /
        :meth:`poll_action`.

        Polling cadence is adapted from the route's observed latency
        (``avg_completion_ms`` / ``p95_completion_ms`` from ``asset_metrics``)
        when ``poll_interval`` / ``poll_timeout`` are not explicitly set.

        Args:
            name_or_id: Route name ("entity_name/route_name") or UUID
            body: Request body data
            query: Query parameters
            params: URL parameters
            output: Output configuration
            input_assets: Mapping of route input names to Ouro asset IDs. Route
                authors should declare asset type and body path metadata on the
                route; caller-side object metadata is compatibility-only.
            wait: If True (default), block until the action reaches a terminal
                state. If False, send ``Prefer: respond-async`` and return
                immediately with the in-progress action handle.
            timeout: HTTP request timeout in seconds for the initial call
            poll_interval: Seconds between status checks while waiting; if
                None (default), derived from route's avg_completion_ms.
            poll_timeout: Maximum seconds to wait for completion; if None
                (default), derived from route's p95_completion_ms.
            raise_on_error: If True, raise route execution exceptions for
                terminal error actions instead of returning the errored Action.
            currency: ``"usd"`` or ``"btc"``: what to pay in on a paid route
                sold in both. Left out, the route's primary currency
                (``price_currency``) is charged. A currency the route isn't
                sold in is refused, never swapped for the other.
            notify: Which in-app notifications this action sends you when it
                finishes: ``"all"`` (the default), ``"failure"`` (only if it
                errors or times out), or ``"none"``. Use ``"none"`` when a
                program runs many actions as one workflow and reports on them
                itself. Webhook endpoints still receive the action event.
            **kwargs: Additional keyword arguments to send to the route

        Raises:
            TimeoutError: If the action doesn't reach a terminal state within
                ``poll_timeout``. The action keeps running server-side; call
                :meth:`retrieve_action` or :meth:`poll_action` later with the
                id from the raised exception's ``action_id`` attribute.
        """
        if notify is not None and notify not in NOTIFY_LEVELS:
            raise ValueError(
                f"notify must be one of {', '.join(NOTIFY_LEVELS)}; got {notify!r}"
            )
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
        }
        if currency:
            payload["currency"] = currency
        request_timeout = timeout or DEFAULT_TIMEOUT
        # RFC 7240: signal "I don't want to block on this" so the backend
        # returns the action handle the moment work is committed. Async routes
        # always take this path: holding the request open until they finish
        # would outlive the HTTP timeout and drop the action id with it.
        execution_mode = getattr(route.route, "execution_mode", None)
        respond_async = not wait or execution_mode == "async"
        request_headers = {"Prefer": "respond-async"} if respond_async else {}
        if notify:
            request_headers["Ouro-Notify"] = notify
        http_response = self.client.post(
            f"/services/{route.parent_id}/routes/{route_id}/use",
            json=payload,
            timeout=request_timeout,
            headers=request_headers,
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
            action = self._parse(Action, action_data)
            log.info(
                f"Route returned 202 Accepted. Action ID: {action.id}, "
                f"status: {action.status}"
            )
            if not wait:
                return action
            effective_interval, effective_timeout = _adaptive_poll_params(
                route, poll_interval, poll_timeout
            )
            # A TimeoutError from here carries ``action_id`` for resuming later.
            return self.poll_action(
                str(action.id),
                poll_interval=effective_interval,
                timeout=effective_timeout,
                raise_on_error=raise_on_error,
            )

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

            output_assets = metadata.get("outputAssets")
            if output_assets and not action_kwargs.get("output_assets"):
                action_kwargs["output_assets"] = output_assets

            action = self._parse(Action, action_kwargs)
            if raise_on_error and action.is_error:
                _raise_action_failure(action)
            return action

        # Last-resort fallback: backend didn't return action metadata at all.
        # Surface an Action-shaped failure rather than a raw dict so callers can
        # rely on the return type.
        raise RuntimeError(
            f"Route {route.name} returned no action metadata; "
            f"response: {envelope.get('data')}"
        )
