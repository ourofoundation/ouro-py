from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

import httpx
from ouro.realtime.websocket import OuroWebSocket

if TYPE_CHECKING:
    from ouro.resources.content import Content


def _coerce_description(
    description: Optional[Union[str, "Content"]],
) -> Optional[Union[str, dict]]:
    """Convert a Content instance to its dict form; pass strings/None through."""
    from ouro.resources.content import Content

    if isinstance(description, Content):
        return description.to_dict()
    return description


def _strip_none(d: dict) -> dict:
    """Return a copy of *d* with all None-valued keys removed."""
    return {k: v for k, v in d.items() if v is not None}


class SyncAPIResource:
    client: httpx.Client
    websocket: OuroWebSocket

    def __init__(self, ouro) -> None:
        self.client = ouro.client
        self.websocket = ouro.websocket
        self.ouro = ouro

    def _handle_response(
        self,
        response: httpx.Response,
        *,
        raw: bool = False,
        with_pagination: bool = False,
    ) -> Any:
        """Parse JSON, check for errors, and return the data payload.

        Raises typed exceptions (NotFoundError, AuthenticationError, etc.)
        based on HTTP status codes instead of generic Exception.

        Args:
            response: The httpx response to process.
            raw: If True, return the full parsed body instead of just the
                 ``data`` field.  Useful for endpoints that return metadata
                 alongside data.
            with_pagination: If True, return ``{"data": ..., "pagination": ...}``
                 when the response follows the standard envelope.
        """
        try:
            body = response.json()
        except Exception:
            response.raise_for_status()
            return None

        if not response.is_success:
            error_msg = ""
            if isinstance(body, dict):
                error_msg = self._extract_error_message(body.get("error", body))
            raise self.ouro._make_status_error(
                error_msg or f"HTTP {response.status_code}",
                response=response,
                body=body,
                status_override=self._envelope_status(body),
            )

        if isinstance(body, dict) and body.get("error"):
            raise self.ouro._make_status_error(
                self._extract_error_message(body["error"]) or "Unknown error",
                response=response,
                body=body,
                status_override=self._envelope_status(body),
            )

        if raw:
            return body

        if with_pagination and isinstance(body, dict):
            return {
                "data": body.get("data"),
                "pagination": body.get("pagination"),
            }

        if isinstance(body, dict):
            return body.get("data")
        return body

    @staticmethod
    def _envelope_status(body: Any) -> Optional[int]:
        """Pull a semantic HTTP status from the response body envelope.

        Some Ouro endpoints historically return 200 OK with the real
        status carried inside ``body.error.status`` (or ``statusCode``).
        Prefer that over the HTTP status when present so typed exceptions
        — ``NotFoundError`` / ``PermissionDeniedError`` / etc. — still
        surface to the caller.
        """
        if not isinstance(body, dict):
            return None
        error = body.get("error")
        if not isinstance(error, dict):
            return None
        for key in ("status", "statusCode"):
            value = error.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        return None

    def _extract_error_message(self, error: Any) -> str:
        """Extract a readable message from string or structured error payloads."""
        if isinstance(error, str):
            return error

        if isinstance(error, dict):
            message = error.get("message")
            if message:
                return str(message)

            detail = error.get("detail")
            if isinstance(detail, str):
                return detail
            if isinstance(detail, list):
                parts = []
                for item in detail:
                    if not isinstance(item, dict):
                        continue
                    loc = item.get("loc")
                    loc_text = ".".join(map(str, loc)) if isinstance(loc, list) else ""
                    msg = item.get("msg") or "Invalid value"
                    parts.append(f"{loc_text}: {msg}" if loc_text else str(msg))
                if parts:
                    return "; ".join(parts)

            nested_error = error.get("error")
            if isinstance(nested_error, str):
                return nested_error

            return str(error)

        return str(error) if error else ""
