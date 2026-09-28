from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, model_validator

from ._base import OuroModel, Page
from .asset import AssetRef, RichText, UserProfile
from .organization import Organization

__all__ = [
    "JoinRequest",
    "Team",
    "TeamBan",
    "TeamMember",
    "TeamMembership",
    "TeamUnreads",
]


class TeamMembership(OuroModel):
    """A user's membership in a team."""

    user_id: Optional[UUID] = None
    user: Optional[UserProfile] = None
    role: Optional[str] = None
    membership_type: Optional[str] = None
    added_at: Optional[datetime] = None

    @model_validator(mode="before")
    @classmethod
    def _carry_user_id(cls, value: Any) -> Any:
        # The API nests the profile without its id; the membership row has it.
        if isinstance(value, dict) and isinstance(value.get("user"), dict):
            value = {**value, "user": {"user_id": value.get("user_id"), **value["user"]}}
        return value


class TeamMember(TeamMembership):
    user_id: UUID


class JoinRequest(OuroModel):
    """A request to join a team with ``join_policy="request"``."""

    id: UUID
    team_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    user: Optional[UserProfile] = None
    status: str
    message: Optional[str] = None
    created_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = None


class TeamBan(OuroModel):
    id: UUID
    team_id: UUID
    user_id: UUID
    user: Optional[UserProfile] = None
    reason: Optional[str] = None
    created_at: Optional[datetime] = None


class Team(OuroModel):
    """An Ouro team (channel within an organization).

    Gating policies (``source_policy``, ``actor_type_policy``) are resolved
    server-side. ``None`` means the team inherits its organization's policy.
    """

    id: UUID
    name: Optional[str] = None
    slug: Optional[str] = None
    org_id: Optional[UUID] = None
    organization: Optional[Organization] = None
    visibility: Optional[str] = None
    default_role: Optional[str] = None
    source_policy: Optional[str] = None
    actor_type_policy: Optional[str] = None
    join_policy: Optional[str] = None
    description: Optional[RichText] = None
    members: Optional[List[TeamMember]] = None
    member_count: Optional[int] = Field(default=None, alias="memberCount")
    user_membership: Optional[TeamMembership] = Field(
        default=None, alias="userMembership"
    )
    user_join_request: Optional[JoinRequest] = Field(
        default=None, alias="userJoinRequest"
    )
    join_status: Optional[str] = Field(default=None, alias="joinStatus")
    join_request_pending: Optional[bool] = None
    can_moderate: Optional[bool] = Field(default=None, alias="canModerate")
    metrics: Optional[Dict[str, int]] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class TeamUnreads(Page[AssetRef]):
    """A page of unread assets in one team."""

    team_id: UUID
    unread_count: int = 0
