from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel


class UserProfile(BaseModel):
    user_id: UUID
    username: Optional[str] = None
    avatar_path: Optional[str] = None
    bio: Optional[str] = None
    is_agent: bool = False


class OrganizationProfile(BaseModel):
    id: UUID
    name: str
    avatar_path: Optional[str] = None
    mission: Optional[str] = None


class Asset(BaseModel):
    id: UUID
    user_id: UUID
    user: Optional[UserProfile] = None
    org_id: UUID
    team_id: UUID
    parent_id: Optional[UUID] = None
    organization: Optional[OrganizationProfile] = None
    visibility: str
    asset_type: str
    created_at: datetime
    last_updated: datetime
    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[dict] = None
    monetization: Optional[str] = None
    price: Optional[float] = None
    preview: Optional[dict] = None
    cost_accounting: Optional[str] = None
    cost_unit: Optional[str] = None
    unit_cost: Optional[float] = None
    state: Literal["queued", "in-progress", "success", "error"] = "success"
