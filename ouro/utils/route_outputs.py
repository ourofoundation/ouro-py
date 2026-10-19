"""Which of a route's declared outputs a request will create.

An output declaration in ``routes.output_assets`` may carry
``when: {field: value | [values]}`` naming top-level JSON request-body fields.
Every field must match; a list matches any of its values. A field the caller
leaves out is read from the request schema's ``default``, and with no default
the output is off. ``optional`` only means "may be absent depending on the
run's results". ``embedded_in`` names the output this one goes into while that
output is on; otherwise it comes back on its own.

Python port of ouro-js ``utils/route-outputs.ts``; keep the rules in step.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

__all__ = [
    "describe_route_output_when",
    "request_body_json_schema",
    "resolve_route_outputs",
    "route_output_when_matches",
]


def request_body_json_schema(request_body: Any) -> Optional[Dict[str, Any]]:
    """The application/json schema of an OpenAPI ``requestBody``, preferring
    the object variant when the body is wrapped in anyOf/oneOf/allOf; None
    when there is nothing to inspect. Returns the schema object itself, not a
    copy, so a caller can edit a deep copy of the request body through it."""
    if not isinstance(request_body, Mapping):
        return None
    content = request_body.get("content")
    if not isinstance(content, Mapping):
        return None
    media = content.get("application/json")
    if not isinstance(media, Mapping):
        return None
    schema = media.get("schema")
    if not isinstance(schema, Mapping):
        return None
    composed = schema.get("anyOf") or schema.get("oneOf") or schema.get("allOf")
    if isinstance(composed, list) and not isinstance(schema.get("properties"), Mapping):
        schema = next(
            (
                item
                for item in composed
                if isinstance(item, Mapping) and isinstance(item.get("properties"), Mapping)
            ),
            composed[0] if composed else None,
        )
    return schema if isinstance(schema, Mapping) else None


def _schema_default(request_body: Any, field: str) -> Any:
    schema = request_body_json_schema(request_body) or {}
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return None
    prop = properties.get(field)
    return prop.get("default") if isinstance(prop, Mapping) else None


def _values(expected: Any) -> List[Any]:
    return list(expected) if isinstance(expected, (list, tuple)) else [expected]


def _same(actual: Any, expected: Any) -> bool:
    # bool is an int in Python; never let True match 1
    if isinstance(actual, bool) or isinstance(expected, bool):
        return isinstance(actual, bool) and isinstance(expected, bool) and actual is expected
    if actual == expected:
        return True
    if isinstance(expected, (int, float)) and isinstance(actual, str) and actual.strip():
        try:
            return float(actual) == float(expected)
        except ValueError:
            return False
    return False


def route_output_when_matches(
    when: Any, body: Optional[Mapping[str, Any]], request_body: Any = None
) -> bool:
    """Whether ``body`` switches on an output with this ``when``.

    A missing or empty ``when`` always matches.
    """
    if not isinstance(when, Mapping):
        return True
    values = body if isinstance(body, Mapping) else {}
    for field, expected in when.items():
        actual = values.get(field) if field in values else _schema_default(request_body, field)
        if actual is None:
            return False
        if not any(_same(actual, value) for value in _values(expected)):
            return False
    return True


def describe_route_output_when(when: Any) -> Optional[str]:
    """Short condition, e.g. ``include_dataset is true``."""
    if not isinstance(when, Mapping) or not when:
        return None

    def show(value: Any) -> str:
        return str(value).lower() if isinstance(value, bool) else str(value)

    parts = []
    for field, expected in when.items():
        values = [show(value) for value in _values(expected)]
        if len(values) == 1:
            parts.append(f"{field} is {values[0]}")
        else:
            parts.append(f"{field} is {', '.join(values[:-1])} or {values[-1]}")
    return " and ".join(parts)


def resolve_route_outputs(
    output_assets: Any,
    request_body: Any = None,
    body: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Every declared output with whether ``body`` switches it on.

    Each entry has ``name``, ``asset_type``, ``primary``, ``optional``,
    ``when``, ``embedded_in``, ``active`` and ``embedded`` (active and going
    into its active ``embedded_in`` parent). Pass ``body=None`` to describe a
    run that sends only the schema defaults.
    """
    if not isinstance(output_assets, Mapping):
        return []
    entries = []
    for name, declaration in output_assets.items():
        if hasattr(declaration, "model_dump"):
            declaration = declaration.model_dump(exclude_none=True)
        if isinstance(declaration, Mapping):
            entries.append((name, declaration))

    active = {
        name: route_output_when_matches(declaration.get("when"), body, request_body)
        for name, declaration in entries
    }

    resolved = []
    for name, declaration in entries:
        parent = declaration.get("embedded_in") or None
        is_active = active[name]
        when = declaration.get("when")
        resolved.append(
            {
                "name": name,
                "asset_type": declaration.get("asset_type"),
                "primary": declaration.get("primary") is True,
                "optional": declaration.get("optional") is True,
                "when": dict(when) if isinstance(when, Mapping) else None,
                "embedded_in": parent,
                "active": is_active,
                "embedded": bool(is_active and parent and active.get(parent)),
            }
        )
    return resolved
