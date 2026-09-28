from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field

from ._base import OuroModel
from .asset import TeamProfile

__all__ = ["Organization", "OrganizationMembership"]


class OrganizationMembership(OuroModel):
    """User's membership info within an organization."""

    role: Optional[str] = None
    membership_type: Optional[str] = None


class Organization(OuroModel):
    """An Ouro organization (workspace)."""

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
    membership_type: Optional[str] = Field(default=None, alias="membershipType")
    user_role: Optional[str] = Field(default=None, alias="userRole")
    default_team: Optional[TeamProfile] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
