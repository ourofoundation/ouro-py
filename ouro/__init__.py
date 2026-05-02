from __future__ import annotations

from ouro.__version__ import __title__, __version__
from ouro._exceptions import (
    APIConnectionError,
    APIError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    ExternalServiceError,
    InternalServerError,
    NotFoundError,
    OuroError,
    PermissionDeniedError,
    RateLimitError,
    RouteExecutionError,
    UnprocessableEntityError,
)
from ouro.events import (
    WEBHOOK_EVENT_TYPES,
    WebhookActor,
    WebhookAssetRef,
    WebhookEvent,
    WebhookEventName,
    WebhookEventPayload,
    WebhookOrgRef,
    WebhookTeamRef,
    normalize_event_type,
    parse_webhook_event,
)

from .utils.plotly import build_plotly_asset_tags, inject_assets_into_html

__all__ = [
    "__version__",
    "__title__",
    "build_plotly_asset_tags",
    "inject_assets_into_html",
    "OuroError",
    "APIError",
    "APIStatusError",
    "APIConnectionError",
    "APITimeoutError",
    "BadRequestError",
    "AuthenticationError",
    "PermissionDeniedError",
    "NotFoundError",
    "ConflictError",
    "ExternalServiceError",
    "UnprocessableEntityError",
    "RateLimitError",
    "InternalServerError",
    "RouteExecutionError",
    "WEBHOOK_EVENT_TYPES",
    "WebhookActor",
    "WebhookAssetRef",
    "WebhookEvent",
    "WebhookEventName",
    "WebhookEventPayload",
    "WebhookOrgRef",
    "WebhookTeamRef",
    "normalize_event_type",
    "parse_webhook_event",
]


def __getattr__(name: str):
    if name == "Ouro":
        from .client import Ouro as _Ouro

        return _Ouro
    raise AttributeError(f"module 'ouro' has no attribute {name!r}")
