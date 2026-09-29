"""Webhook event parsing for the Ouro Python SDK.

This mirrors the canonical event registry in `@ourofoundation/ouro-js`
(`src/schema/events.ts`). Update both in lockstep when adding event types -
`tests/test_events.py` enforces parity.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Mapping, Optional, Tuple

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Event registry (mirror of ouro-js src/schema/events.ts)
# ---------------------------------------------------------------------------

WEBHOOK_EVENT_TYPES: Tuple[str, ...] = (
    "comment",
    "mention",
    "reference",
    "reaction",
    "share",
    "follow",
    "action",
    "asset.deleted",
    "file.uploaded",
    "new-conversation",
    "new-message",
    "interrupt",
)

WebhookEventName = Literal[
    "comment",
    "mention",
    "reference",
    "reaction",
    "share",
    "follow",
    "action",
    "asset.deleted",
    "file.uploaded",
    "new-conversation",
    "new-message",
    "interrupt",
]


# ---------------------------------------------------------------------------
# Payload shapes
# ---------------------------------------------------------------------------


class WebhookActor(BaseModel):
    """The user (or agent) that triggered the event."""

    id: str
    username: str
    is_agent: bool = False


class WebhookAssetRef(BaseModel):
    id: str
    type: str


class WebhookOrgRef(BaseModel):
    id: str
    name: str


class WebhookTeamRef(BaseModel):
    id: str
    name: str


class WebhookEventPayload(BaseModel):
    """Canonical envelope for every webhook delivery from the Ouro backend."""

    event: str
    data: Dict[str, Any]
    timestamp: Optional[str] = None
    user_id: Optional[str] = None
    delivery_id: Optional[str] = None
    notification_id: Optional[str] = None
    notification_ids: Optional[list[str]] = None


@dataclass(frozen=True)
class WebhookEvent:
    """Parsed, typed view of a webhook delivery.

    Field shape matches the canonical payload produced by the backend's
    `notify()`/`dispatchWebhookEvent` pipeline. Optional fields are populated
    only for the event types that include them in `data`.
    """

    event_type: str
    data: Dict[str, Any]
    timestamp: Optional[str]
    delivery_id: Optional[str]
    recipient_user_id: Optional[str]
    notification_ids: Tuple[str, ...]

    actor: Optional[WebhookActor]
    asset: Optional[WebhookAssetRef]
    parent_asset: Optional[WebhookAssetRef]
    root_asset: Optional[WebhookAssetRef]
    team: Optional[WebhookTeamRef]
    organization: Optional[WebhookOrgRef]

    conversation_id: Optional[str]
    source_id: Optional[str]
    source_asset_type: Optional[str]

    @property
    def actor_user_id(self) -> Optional[str]:
        """Convenience accessor for the acting user's id."""
        return self.actor.id if self.actor else None

    @property
    def sender_username(self) -> Optional[str]:
        """Convenience accessor for the actor's username."""
        return self.actor.username if self.actor else None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def normalize_event_type(event_name: str) -> str:
    """Lowercase and dash-normalize an event name (`new_message` -> `new-message`)."""
    return event_name.strip().lower().replace("_", "-")


def _coerce_model(model_cls, value: Any):
    if value is None:
        return None
    if isinstance(value, model_cls):
        return value
    if isinstance(value, Mapping):
        try:
            return model_cls.model_validate(dict(value))
        except Exception:
            return None
    return None


def _legacy_actor(value: Any) -> Optional[Dict[str, Any]]:
    """Coerce legacy `sender`/`user` dicts into the new actor shape.

    Used during the rollout of the canonical payload (`data.actor`); can be
    removed once all producers are on the new shape.
    """
    if not isinstance(value, Mapping):
        return None
    if "id" not in value or "username" not in value:
        return None
    return {
        "id": value["id"],
        "username": value["username"],
        "is_agent": bool(value.get("is_agent", False)),
    }


def _legacy_actor_from_top_level(data: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """Reconstruct an actor from top-level `user_id` / `sender_username`."""
    user_id = data.get("user_id")
    if not isinstance(user_id, str):
        return None
    username = (
        data.get("sender_username")
        if isinstance(data.get("sender_username"), str)
        else "unknown"
    )
    return {"id": user_id, "username": username, "is_agent": False}


def _legacy_asset_ref(
    data: Mapping[str, Any], id_key: str, type_key: str
) -> Optional[Dict[str, Any]]:
    asset_id = data.get(id_key)
    asset_type = data.get(type_key)
    if not isinstance(asset_id, str):
        return None
    return {"id": asset_id, "type": asset_type if isinstance(asset_type, str) else "unknown"}


def _normalize_notification_ids(*values: Any) -> Tuple[str, ...]:
    notification_ids: list[str] = []
    for value in values:
        if isinstance(value, str):
            notification_ids.append(value)
        elif isinstance(value, list):
            notification_ids.extend(item for item in value if isinstance(item, str))

    return tuple(dict.fromkeys(notification_ids))


def parse_webhook_event(
    body: Mapping[str, Any] | WebhookEventPayload,
) -> WebhookEvent:
    """Validate a raw webhook body and return a typed `WebhookEvent`."""
    payload = (
        body
        if isinstance(body, WebhookEventPayload)
        else WebhookEventPayload.model_validate(body)
    )
    data = payload.data or {}
    event_type = normalize_event_type(payload.event)

    actor = _coerce_model(
        WebhookActor,
        data.get("actor")
        or _legacy_actor(data.get("sender"))
        or _legacy_actor(data.get("user"))
        or _legacy_actor_from_top_level(data),
    )
    asset = _coerce_model(WebhookAssetRef, data.get("asset"))
    parent_asset = _coerce_model(
        WebhookAssetRef,
        data.get("parent_asset") or _legacy_asset_ref(data, "parent_asset_id", "parent_asset_type"),
    )
    root_asset = _coerce_model(
        WebhookAssetRef,
        data.get("root_asset") or _legacy_asset_ref(data, "root_asset_id", "root_asset_type"),
    )
    team = _coerce_model(WebhookTeamRef, data.get("team"))
    organization = _coerce_model(WebhookOrgRef, data.get("organization"))

    # Conversation events embed the conversation id as the asset id for
    # `new-conversation`, and as `conversation_id` for `new-message`.
    conversation_id = data.get("conversation_id")
    if not conversation_id and event_type == "new-conversation":
        conversation_id = data.get("id") or (asset.id if asset else None)

    notification_ids = _normalize_notification_ids(
        payload.notification_id,
        payload.notification_ids,
        data.get("notification_id"),
        data.get("notification_ids"),
    )

    return WebhookEvent(
        event_type=event_type,
        data=data,
        timestamp=payload.timestamp,
        delivery_id=payload.delivery_id,
        recipient_user_id=payload.user_id,
        notification_ids=notification_ids,
        actor=actor,
        asset=asset,
        parent_asset=parent_asset,
        root_asset=root_asset,
        team=team,
        organization=organization,
        conversation_id=conversation_id,
        source_id=data.get("source_id"),
        source_asset_type=data.get("source_asset_type"),
    )


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------

WEBHOOK_SIGNATURE_HEADER = "X-Ouro-Signature"
WEBHOOK_DELIVERY_HEADER = "X-Ouro-Delivery"
WEBHOOK_ATTEMPT_HEADER = "X-Ouro-Attempt"
DEFAULT_SIGNATURE_TOLERANCE_SECONDS = 300


def verify_webhook_signature(
    body: bytes | str,
    header: Optional[str],
    secret: str,
    tolerance: int = DEFAULT_SIGNATURE_TOLERANCE_SECONDS,
    *,
    now: Optional[float] = None,
) -> bool:
    """Check an `X-Ouro-Signature` header against the raw request body.

    The backend sends `t=<unix seconds>,v1=<hex HMAC-SHA256>` where the HMAC
    is computed over `f"{t}.{body}"` keyed by the endpoint's `whsec_` secret.
    `body` must be the exact bytes received (not re-serialized JSON). Multiple
    `v1=` entries are accepted so secrets can be rotated. Returns False when the
    header is missing/malformed, no signature matches, or `t` is more than
    `tolerance` seconds away from `now` (pass `tolerance <= 0` to skip the
    timestamp check).
    """
    if not header or not secret:
        return False

    timestamp: Optional[int] = None
    signatures: list[str] = []
    for part in header.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError:
                return False
        elif key == "v1":
            signatures.append(value.strip().lower())
    if timestamp is None or not signatures:
        return False

    if tolerance > 0:
        current = time.time() if now is None else now
        if abs(current - timestamp) > tolerance:
            return False

    raw = body.encode("utf-8") if isinstance(body, str) else body
    expected = hmac.new(
        secret.encode("utf-8"), f"{timestamp}.".encode("ascii") + raw, hashlib.sha256
    ).hexdigest()
    return any(hmac.compare_digest(expected, sig) for sig in signatures)
