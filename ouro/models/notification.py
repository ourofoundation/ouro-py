from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from ._base import DictCompatModel

__all__ = ["Notification"]


class Notification(DictCompatModel):
    """A single user notification.

    The backend response includes nested ``source_user`` and ``asset`` objects
    with a variable shape; they're kept as ``dict`` here to avoid over-fitting
    to a snapshot of the schema. Use ``.get("field")`` or attribute access for
    the common fields below.
    """

    id: UUID
    user_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    source_user_id: Optional[UUID] = None
    org_id: Optional[UUID] = None
    type: Optional[str] = None
    read: Optional[bool] = None
    read_at: Optional[datetime] = None
    content: Optional[dict] = None
    source_user: Optional[dict] = None
    asset: Optional[dict] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
