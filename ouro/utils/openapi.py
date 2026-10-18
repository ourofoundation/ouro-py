OUTPUT_ASSETS_EXTENSION = "x-ouro-output-assets"
PROPERTIES_EXTENSION = "x-ouro-properties"
RESPONSE_CHARTS_EXTENSION = "x-ouro-response-charts"


def ouro_field(key, value):
    """
    Decorator to add custom fields to the OpenAPI schema of your FastAPI app.
    """

    def decorator(func):
        if not hasattr(func, "ouro_fields"):
            func.ouro_fields = {}
        func.ouro_fields[key] = value
        _check_references(func.ouro_fields)
        return func

    return decorator


def _validated(model, value, label):
    """``value`` checked against ``model`` (unknown keys included), returned
    as written minus None fields so the spec carries what the author meant."""
    from pydantic import ValidationError

    if not isinstance(value, dict):
        raise ValueError(f"{label}: must be a dict, got {type(value).__name__}")
    unknown = sorted(set(value) - set(model.model_fields))
    if unknown:
        raise ValueError(
            f"{label}: unknown key{'s' if len(unknown) > 1 else ''} "
            f"{', '.join(map(repr, unknown))} "
            f"(takes {', '.join(model.model_fields)})"
        )
    try:
        model.model_validate(value)
    except ValidationError as error:
        issue = error.errors()[0]
        where = ".".join(str(part) for part in issue["loc"])
        raise ValueError(
            f"{label}: {where + ': ' if where else ''}{issue['msg']}"
        ) from None
    return {key: item for key, item in value.items() if item is not None}


def _check_references(fields):
    """Names one declaration uses for another on the same operation must
    exist: an output's ``embedded_in``, a chart's ``dataset`` and a
    property's output ``subject``. Run as each decorator lands, so the order
    they're stacked in doesn't matter. Ouro checks the rest on import."""
    outputs = fields.get(OUTPUT_ASSETS_EXTENSION)
    if not isinstance(outputs, dict):
        return
    for name, declaration in outputs.items():
        parent = declaration.get("embedded_in") if isinstance(declaration, dict) else None
        if parent is not None and (parent == name or parent not in outputs):
            raise ValueError(
                f"ouro_outputs: {name!r} embedded_in {parent!r} is not another output"
            )
    datasets = [
        name
        for name, declaration in outputs.items()
        if isinstance(declaration, dict) and declaration.get("asset_type") == "dataset"
    ]
    for index, chart in enumerate(fields.get(RESPONSE_CHARTS_EXTENSION) or []):
        dataset = chart.get("dataset") if isinstance(chart, dict) else None
        if dataset is not None and dataset not in datasets:
            raise ValueError(
                f"ouro_response_charts[{index}]: dataset {dataset!r} is not a "
                f"dataset output ({', '.join(datasets) or 'none declared'})"
            )
    for index, declaration in enumerate(fields.get(PROPERTIES_EXTENSION) or []):
        subject = declaration.get("subject") if isinstance(declaration, dict) else None
        output = subject.get("output") if isinstance(subject, dict) else None
        if output is not None and output not in outputs:
            raise ValueError(
                f"ouro_properties[{index}]: subject output {output!r} is not an output"
            )


def ouro_outputs(outputs):
    """
    Declare the assets a route's response creates (``x-ouro-output-assets``),
    keyed by response field:

        @app.post("/relax")
        @ouro_outputs({
            "structure": {"asset_type": "file", "file_extensions": ["cif"], "primary": True},
            "trajectory": {"asset_type": "file", "when": {"save_trajectory": True}},
            "curve_dataset": include_dataset_output(),
        })
        def relax(...): ...

    Each declaration takes ``asset_type``, ``primary``, ``file_extensions``,
    ``contains_file_extensions``, ``when`` (request fields that switch it on),
    ``optional`` (may be absent depending on results) and ``embedded_in``
    (the post output it goes into). Unknown keys are rejected here; Ouro
    checks ``when`` against the request body when the spec is synced.
    """
    from ouro.models.route import RouteOutputAssetDeclaration

    if not isinstance(outputs, dict) or not outputs:
        raise ValueError("ouro_outputs: give a dict of response field -> declaration")
    declared = {
        name: _validated(RouteOutputAssetDeclaration, declaration, f"ouro_outputs: {name!r}")
        for name, declaration in outputs.items()
    }
    _check_references({OUTPUT_ASSETS_EXTENSION: declared})
    return ouro_field(OUTPUT_ASSETS_EXTENSION, declared)


def include_dataset_output(**fields):
    """
    An opt-in dataset output: created only when the caller sends
    ``include_dataset: true``. The route's request model needs that field
    (default False), and the response puts a ``dataset_payload(...)`` under a
    key of its own, never the inline list's, since a dataset output key may
    only hold a dataset. Pair it with a response chart naming the key as its
    ``dataset`` to give the dataset a view.
    """
    return {"asset_type": "dataset", "when": {"include_dataset": True}, **fields}


def ouro_properties(properties):
    """
    Declare the asset properties a route fills from its JSON response
    (``x-ouro-properties``). Ouro records each value against the run's
    primary input, or the input or output ``subject`` names:

        @ouro_properties([
            {"property": "curie_temperature", "value": "Tc_K", "unit": "K",
             "method": "Monte Carlo",
             "requires": {"ordering_overlap": {"min": 0.5}}},
        ])

    ``when`` takes request fields as on outputs; ``requires`` takes response
    dot paths that must equal a value, one of a list, or lie in a
    ``{"min", "max"}`` range. A property declared twice needs a different
    ``method`` each time. Ouro checks property keys and units on sync.
    """
    from ouro.models.route import RoutePropertyDeclaration

    if not isinstance(properties, list) or not properties:
        raise ValueError("ouro_properties: give a list of declarations")
    declared = [
        _validated(RoutePropertyDeclaration, declaration, f"ouro_properties[{index}]")
        for index, declaration in enumerate(properties)
    ]
    seen = set()
    for index, declaration in enumerate(declared):
        key = (declaration["property"], declaration.get("method"))
        if key in seen:
            raise ValueError(
                f"ouro_properties[{index}]: {key[0]} is declared twice; "
                "give each a different method"
            )
        seen.add(key)
    return ouro_field(PROPERTIES_EXTENSION, declared)


def ouro_response_charts(charts):
    """
    Declare charts Ouro draws on every run from lists in the JSON response
    (``x-ouro-response-charts``):

        @ouro_response_charts([{
            "data": "magnetization_curve",
            "title": "Magnetization curve",
            "description": "Tc = {Tc_K:0} K",
            "sort": "temperature_K",
            "dataset": "magnetization_curve_dataset",
            "config": {"type": "line",
                       "category": {"dataKey": "temperature_K", "type": "number"},
                       "series": [{"dataKey": "order_parameter"}]},
        }])

    ``config`` is a dataset view's chart config keyed by the records' own
    keys; Ouro checks it on sync. ``dataset`` names a dataset output, which
    then opens on this chart.
    """
    from ouro.models.route import RouteResponseChart

    if not isinstance(charts, list) or not charts:
        raise ValueError("ouro_response_charts: give a list of charts")
    declared = [
        _validated(RouteResponseChart, chart, f"ouro_response_charts[{index}]")
        for index, chart in enumerate(charts)
    ]
    return ouro_field(RESPONSE_CHARTS_EXTENSION, declared)


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
        # Keyed by path and method: a GET and a POST on one path are
        # different operations with their own fields.
        route_map = {
            (route.path, method.lower()): route.endpoint
            for route in iter_routes(app.routes)
            if getattr(route, "endpoint", None) is not None
            for method in getattr(route, "methods", None) or ()
        }

        for path, path_item in openapi_schema["paths"].items():
            for method, operation in path_item.items():
                # Find the matching endpoint from our route map
                endpoint = route_map.get((path, method))

                # Only update operation if we found a matching endpoint with ouro_fields
                if endpoint and hasattr(endpoint, "ouro_fields"):
                    operation.update(endpoint.ouro_fields)

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    return custom_openapi
