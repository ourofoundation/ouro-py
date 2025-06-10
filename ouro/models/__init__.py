from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, List, Literal, Optional, Union
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
    "Dataset",
    "Comment",
]


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
    name: str
    path: str
    size: int
    type: str
    bucket: Literal["public-files", "files"]
    id: Optional[UUID] = None
    fullPath: Optional[str] = None


class InProgressFileMetadata(BaseModel):
    type: str


class File(Asset):
    metadata: Union[FileMetadata, InProgressFileMetadata] = Field(
        union_mode="left_to_right",
        # discriminator="state",
    )
    data: Optional[FileData] = None
    _ouro: Optional["Ouro"] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ouro = kwargs.get("_ouro")

    def share(
        self, user_id: UUID | str, role: Literal["read", "write", "admin"] = "read"
    ) -> None:
        """Share this file with another user. You must be an admin of the file to share.

        Args:
            user_id: The UUID of the user to share with
            role: The role to grant the user (read, write, admin)
        """
        if not self._ouro:
            raise RuntimeError("File object not connected to Ouro client")
        self._ouro.files.share(self.id, user_id, role)


class DatasetMetadata(BaseModel):
    table_name: str
    columns: List[str]


class Dataset(Asset):
    # metadata: Union[DatasetMetadata, Optional[FileMetadata]]
    preview: Optional[List[dict]] = None


class Comment(Asset):
    content: Optional[PostContent] = None
