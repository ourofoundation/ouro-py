from typing import TYPE_CHECKING, List, Optional, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from .asset import Asset, DescriptionDict, TeamProfile

if TYPE_CHECKING:
    from ouro.resources.conversations import ConversationMessages

    from ouro import Ouro

from .action import Action, ActionLog
from .file import File, FileData
from .notification import Notification
from .organization import Organization, OrganizationMembership
from .service import Route, RouteData, RouteMetrics, Service
from .team import Team, TeamMember, TeamMembership

__all__ = [
    "Action",
    "ActionLog",
    "Asset",
    "DescriptionDict",
    "TeamProfile",
    "PostContent",
    "Post",
    "Quest",
    "QuestDetails",
    "QuestItem",
    "QuestProgress",
    "Conversation",
    "File",
    "FileData",
    "Dataset",
    "Comment",
    "Notification",
    "Organization",
    "OrganizationMembership",
    "Service",
    "Route",
    "RouteData",
    "RouteMetrics",
    "Team",
    "TeamMember",
    "TeamMembership",
]


class PostContent(BaseModel):
    text: str
    data: dict = Field(
        alias="json",
    )


class Post(Asset):
    content: Optional[PostContent] = None
    comments: Optional[int] = Field(default=0)
    views: Optional[int] = Field(default=0)


class QuestDetails(BaseModel):
    id: Optional[UUID] = None
    type: Optional[Literal["closable", "continuous"]] = None
    status: Optional[Literal["draft", "open", "closed", "cancelled"]] = None
    reward_xp: Optional[int] = 0
    reward_currency: Optional[str] = "btc"
    reward_sats: Optional[int] = 0
    reward_usd_cents: Optional[int] = 0
    max_xp_per_contributor: Optional[int] = None
    allowed_submission_type: Optional[str] = None
    constraints: Optional[dict] = None


class QuestItem(BaseModel):
    id: Optional[UUID] = None
    quest_id: Optional[UUID] = None
    description: str = ""
    status: Literal["pending", "in_progress", "done", "skipped"] = "pending"
    auto_skipped: bool = False
    status_before_auto_skip: Optional[
        Literal["pending", "in_progress", "done", "skipped"]
    ] = None
    sort_order: int = 0
    type: str = "task"
    created_by: Optional[UUID] = None
    assignee_id: Optional[UUID] = None
    expected_asset_type: Optional[str] = None
    reward_xp: int = 0
    reward_currency: str = "btc"
    reward_sats: int = 0
    reward_usd_cents: int = 0
    child_quest_id: Optional[UUID] = None
    completed_entry_id: Optional[UUID] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class QuestProgress(BaseModel):
    total: int = 0
    done: int = 0
    remaining: int = 0


class Quest(Asset):
    quest: Optional[QuestDetails] = None
    items: Optional[List[QuestItem]] = None
    progress: Optional[QuestProgress] = None
    comments: Optional[int] = Field(default=0)


class ConversationMetadata(BaseModel):
    members: List[UUID]
    summary: Optional[str] = None


class Conversation(Asset):
    asset_type: Literal["conversation"] = "conversation"
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
    preview: Optional[List[dict]] = None


class Comment(Asset):
    content: Optional[PostContent] = None
