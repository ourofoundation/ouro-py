from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "Asset",
    "PostContent",
    "Post",
]


class Asset(BaseModel):
    id: UUID
    user_id: UUID
    org_id: UUID | None
    name: str
    visibility: str
    asset_type: str
    created_at: datetime
    last_updated: datetime
    description: Optional[str]
    metadata: Optional[dict]
    monetization: Optional[str]
    price: Optional[float]
    product_id: Optional[str]
    price_id: Optional[str]
    preview: Optional[dict]
    cost_accounting: Optional[str]
    cost_unit: Optional[str]
    unit_cost: Optional[float]


class PostContent(BaseModel):
    text: str
    data: dict = Field(
        alias="json",
    )


class Post(Asset):
    content: Optional[PostContent] = None
    # preview: Optional[PostContent]
    comments: Optional[int] = Field(default=0)
    views: Optional[int] = Field(default=0)
