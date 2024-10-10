from datetime import datetime
from typing import TYPE_CHECKING, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from ouro.resources.conversations import ConversationMessages

    from ouro import Ouro

__all__ = [
    "Asset",
    "PostContent",
    "Post",
    "Conversation",
    "File",
    "FileData",
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


class ConversationMetadata(BaseModel):
    members: List[UUID]
    summary: Optional[str] = None


class Conversation(Asset):
    id: UUID
    name: str
    description: Optional[str] = None
    summary: Optional[str] = None
    metadata: ConversationMetadata
    _messages: Optional["ConversationMessages"] = None
    _ouro: Optional["Ouro"] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ouro = kwargs.get("_ouro")

    @property
    def messages(self):
        if self._messages is None:
            from ouro.resources.conversations import ConversationMessages

            self._messages = ConversationMessages(self)
        return self._messages


class FileData(BaseModel):
    signedUrl: Optional[str] = None
    publicUrl: Optional[str] = None


class FileMetadata(BaseModel):
    id: UUID
    name: str
    path: str
    size: int
    type: str
    bucket: Literal["public-files", "files"]
    fullPath: str


class File(Asset):
    metadata: FileMetadata
    data: Optional[FileData] = None
