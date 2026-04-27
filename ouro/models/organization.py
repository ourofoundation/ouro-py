from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from ._base import DictCompatModel

__all__ = ["Organization", "OrganizationMembership"]


class OrganizationMembership(DictCompatModel):
    """User's membership info within an organization."""

    role: Optional[str] = None
    membership_type: Optional[str] = None


class Organization(DictCompatModel):
    """An Ouro organization (workspace).

    Fields are permissive (``extra="allow"``) because the backend returns a
    richer shape than what's declared here. Use attribute access for the
    fields below; fall back to ``.get("field")`` for anything else.
    """

    id: UUID
    name: str
    display_name: Optional[str] = None
    mission: Optional[str] = None
    avatar_path: Optional[str] = None
    join_policy: Optional[str] = None
    visibility: Optional[str] = None
    source_policy: Optional[str] = None
    actor_type_policy: Optional[str] = None
    membership: Optional[OrganizationMembership] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
