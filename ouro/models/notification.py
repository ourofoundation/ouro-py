from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from ._base import OuroModel
from .asset import AssetRef, UserProfile

__all__ = ["Notification"]


class Notification(OuroModel):
    """A single user notification."""

    id: UUID
    destination_user_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    parent_asset_id: Optional[UUID] = None
    root_asset_id: Optional[UUID] = None
    action_id: Optional[UUID] = None
    source_user_id: Optional[UUID] = None
    org_id: Optional[UUID] = None
    type: Optional[str] = None
    viewed: Optional[bool] = None
    content: Optional[Dict[str, Any]] = None
    source_user: Optional[UserProfile] = None
    asset: Optional[AssetRef] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
