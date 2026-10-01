def ouro_field(key, value):
    """
    Decorator to add custom fields to the OpenAPI schema of your FastAPI app.
    """

    def decorator(func):
        if not hasattr(func, "ouro_fields"):
            func.ouro_fields = {}
        func.ouro_fields[key] = value
        return func

    return decorator


def ouro_execution_mode(mode: str):
    """
    Convenience decorator to declare a route's execution model so Ouro (and
    AI agents discovering the route) know whether to wait inline for a
    response or expect to poll/await a webhook completion via action_id.

    Modes:
      - "sync":  upstream returns the result inline (HTTP 200). Caller may
                 block on the response. This is the default if not declared.
      - "async": upstream returns 202 quickly and posts completion to
                 /actions/{action_id}/response. Caller should typically
                 retrieve results via the action handle.

    Equivalent to ``ouro_field("x-ouro-execution-mode", mode)``.
    """
    if mode not in ("sync", "async"):
        raise ValueError(
            f"ouro_execution_mode: mode must be 'sync' or 'async', got {mode!r}"
        )
    return ouro_field("x-ouro-execution-mode", mode)


def ouro_capabilities(capabilities):
    """Declare validated semantic capabilities on an OpenAPI operation."""
    from ouro.models.route import RouteCapabilities

    normalized = RouteCapabilities.model_validate(capabilities).model_dump(
        by_alias=True,
        exclude_none=True,
    )
    return ouro_field("x-ouro-capabilities", normalized)


def ouro_pricing(
    *,
    per_second=None,
    per_call=None,
    free=False,
    max_seconds=None,
    currency="usd",
):
    """
    Declare a route's price in code. Ouro applies it whenever the service's
    OpenAPI spec is synced, overriding a price set in the UI; routes without
    the decorator keep whatever pricing they already have.

    Give exactly one of:
      - ``per_second``: price per second the route runs, billed up to
        ``max_seconds`` (required, 1-86400). Each call's budget arrives in the
        ``ouro-max-billable-seconds`` header; failed runs are free.
      - ``per_call``: price per successful call.
      - ``free=True``: make a paid route free again.

    Prices are in ``currency``: dollars for "usd", sats for "btc".

        @app.post("/simulate")
        @ouro_pricing(per_second=0.0003, max_seconds=3600)
        def simulate(...): ...

    Emits ``x-ouro-pricing`` on the operation.
    """
    chosen = [
        name
        for name, value in (
            ("per_second", per_second),
            ("per_call", per_call),
            ("free", free or None),
        )
        if value is not None
    ]
    if len(chosen) != 1:
        raise ValueError(
            "ouro_pricing: give exactly one of per_second, per_call or free=True"
        )
    model = chosen[0]
    if model != "per_second" and max_seconds is not None:
        raise ValueError("ouro_pricing: max_seconds only applies to per_second")
    if model == "free":
        return ouro_field("x-ouro-pricing", {"model": "free"})

    if currency not in ("usd", "btc"):
        raise ValueError(
            f"ouro_pricing: currency must be 'usd' or 'btc', got {currency!r}"
        )
    unit_cost = per_second if model == "per_second" else per_call
    if isinstance(unit_cost, bool) or not isinstance(unit_cost, (int, float)):
        raise ValueError(f"ouro_pricing: {model} must be a number")
    if not unit_cost > 0:
        raise ValueError(f"ouro_pricing: {model} must be above 0")
    pricing = {"model": model, "unit_cost": unit_cost, "currency": currency}
    if model == "per_second":
        if (
            isinstance(max_seconds, bool)
            or not isinstance(max_seconds, int)
            or not 1 <= max_seconds <= 86400
        ):
            raise ValueError(
                "ouro_pricing: per_second needs max_seconds, a whole number "
                "of seconds from 1 to 86400"
            )
        pricing["max_billable_seconds"] = max_seconds
    return ouro_field("x-ouro-pricing", pricing)


def get_custom_openapi(app, get_openapi):
    """
    Function to generate a custom OpenAPI schema for your FastAPI app.
    """

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            summary=app.summary,
            description=app.description,
            routes=app.routes,
        )

        from fastapi import routing

        # FastAPI >= 0.141 keeps included routers nested in app.routes;
        # iter_route_contexts flattens them with their full, prefixed paths.
        iter_routes = getattr(routing, "iter_route_contexts", iter)
        route_map = {
            route.path: route.endpoint
            for route in iter_routes(app.routes)
            if getattr(route, "endpoint", None) is not None
        }

        for path, path_item in openapi_schema["paths"].items():
            for method, operation in path_item.items():
                # Find the matching endpoint from our route map
                endpoint = route_map.get(path)

                # Only update operation if we found a matching endpoint with ouro_fields
                if endpoint and hasattr(endpoint, "ouro_fields"):
                    operation.update(endpoint.ouro_fields)

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    return custom_openapi
