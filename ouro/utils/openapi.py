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


_PRICING_CURRENCIES = ("usd", "btc")


def _is_price(value):
    return not isinstance(value, bool) and isinstance(value, (int, float))


def ouro_pricing(
    *,
    per_second=None,
    per_call=None,
    free=False,
    max_seconds=None,
    currency=None,
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

    A number is a price in ``currency``: dollars for "usd" (the default), sats
    for "btc".

        @app.post("/simulate")
        @ouro_pricing(per_second=0.0003, max_seconds=3600)
        def simulate(...): ...

    To sell in both currencies, give a price for each. Callers pick which to
    pay in; one who doesn't pays in the first currency listed (or
    ``currency``, when given).

        @ouro_pricing(per_call={"usd": 0.05, "btc": 50})

    The declaration is the route's whole price: a currency it leaves out is
    one the route is no longer sold in.

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

    if currency is not None and currency not in _PRICING_CURRENCIES:
        raise ValueError(
            f"ouro_pricing: currency must be 'usd' or 'btc', got {currency!r}"
        )
    price = per_second if model == "per_second" else per_call
    if isinstance(price, dict):
        # A price per currency; the first listed is the primary
        if not price:
            raise ValueError(f"ouro_pricing: {model} needs at least one price")
        unknown = [key for key in price if key not in _PRICING_CURRENCIES]
        if unknown:
            raise ValueError(
                f"ouro_pricing: {model} prices are keyed by 'usd' or 'btc', "
                f"got {unknown[0]!r}"
            )
        if currency is not None and currency not in price:
            raise ValueError(
                f"ouro_pricing: currency {currency!r} has no price in {model}"
            )
        prices = dict(price)
        primary = currency or next(iter(prices))
    else:
        primary = currency or "usd"
        prices = {primary: price}
    for unit_cost in prices.values():
        if not _is_price(unit_cost):
            raise ValueError(f"ouro_pricing: {model} must be a number")
        if not unit_cost > 0:
            raise ValueError(f"ouro_pricing: {model} must be above 0")

    # unit_cost + currency is the primary price (and all a single-currency
    # route declares)
    pricing = {"model": model, "unit_cost": prices[primary], "currency": primary}
    if len(prices) > 1:
        pricing["unit_cost_usd"] = prices["usd"]
        pricing["unit_cost_sats"] = prices["btc"]
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
