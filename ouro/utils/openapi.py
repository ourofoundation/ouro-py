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
