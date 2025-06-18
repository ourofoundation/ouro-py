from typing import TYPE_CHECKING, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from .asset import Asset

if TYPE_CHECKING:
    from ouro.resources.conversations import ConversationMessages

    from ouro import Ouro

from .file import File, FileData
from .service import Route, Service

__all__ = [
    "Asset",
    "PostContent",
    "Post",
    "Conversation",
    "File",
    "FileData",
    "Dataset",
    "Comment",
    "Service",
    "Route",
]


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


class DatasetMetadata(BaseModel):
    table_name: str
    columns: List[str]


class Dataset(Asset):
    # metadata: Union[DatasetMetadata, Optional[FileMetadata]]
    preview: Optional[List[dict]] = None


class Comment(Asset):
    content: Optional[PostContent] = None
