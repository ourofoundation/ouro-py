from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from ._base import DictCompatModel
from .organization import Organization

__all__ = ["Team", "TeamMember", "TeamMembership"]


class TeamMember(DictCompatModel):
    user_id: Optional[UUID] = None
    role: Optional[str] = None
    user: Optional[dict] = None


class TeamMembership(DictCompatModel):
    """Current user's membership info within a team."""

    role: Optional[str] = None
    membership_type: Optional[str] = None


class Team(DictCompatModel):
    """An Ouro team (channel within an organization).

    Gating policies (``source_policy``, ``actor_type_policy``) are always
    resolved server-side. A ``None`` value here means the backend didn't
    return the field; fall back to the owning organization's policy.
    """

    id: UUID
    name: Optional[str] = None
    slug: Optional[str] = None
    org_id: Optional[UUID] = None
    organization_id: Optional[UUID] = None
    organization: Optional[Organization] = None
    visibility: Optional[str] = None
    default_role: Optional[str] = None
    source_policy: Optional[str] = None
    actor_type_policy: Optional[str] = None
    description: Optional[dict] = None
    members: Optional[List[TeamMember]] = None
    memberCount: Optional[int] = None
    userMembership: Optional[TeamMembership] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
