from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from pydantic import BaseModel


class WebhookEventPayload(BaseModel):
    event: str
    data: Dict[str, Any]
    timestamp: Optional[str] = None
    user_id: Optional[str] = None


@dataclass(frozen=True)
class WebhookEvent:
    event_type: str
    data: Dict[str, Any]
    timestamp: Optional[str]
    recipient_user_id: Optional[str]
    conversation_id: Optional[str]
    actor_user_id: Optional[str]
    sender_username: Optional[str]
    source_id: Optional[str]
    source_asset_type: Optional[str]


def normalize_event_type(event_name: str) -> str:
    return event_name.strip().lower().replace("_", "-")


def parse_webhook_event(
    body: Mapping[str, Any] | WebhookEventPayload,
) -> WebhookEvent:
    payload = (
        body if isinstance(body, WebhookEventPayload) else WebhookEventPayload.model_validate(body)
    )
    data = payload.data or {}
    event_type = normalize_event_type(payload.event)

    conversation_id = data.get("conversation_id")
    if not conversation_id and event_type == "new-conversation":
        conversation_id = data.get("id")

    sender_obj = data.get("sender") if isinstance(data.get("sender"), Mapping) else {}
    user_obj = data.get("user") if isinstance(data.get("user"), Mapping) else {}
    actor_user_id = data.get("user_id") or sender_obj.get("id") or user_obj.get("id")
    sender_username = (
        data.get("sender_username")
        or sender_obj.get("username")
        or (data.get("sender") if isinstance(data.get("sender"), str) else None)
        or user_obj.get("username")
    )

    return WebhookEvent(
        event_type=event_type,
        data=data,
        timestamp=payload.timestamp,
        recipient_user_id=payload.user_id,
        conversation_id=conversation_id,
        actor_user_id=actor_user_id,
        sender_username=sender_username,
        source_id=data.get("source_id"),
        source_asset_type=data.get("source_asset_type"),
    )
