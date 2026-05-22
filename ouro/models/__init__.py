from typing import TYPE_CHECKING, List, Optional, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from ._base import DictCompatModel
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
    "Entry",
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
    max_xp_per_contributor: Optional[int] = None


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
    reward_amount: int = 0
    child_quest_id: Optional[UUID] = None
    completed_entry_id: Optional[UUID] = None
    eval_route_id: Optional[UUID] = None
    eval_score_path: Optional[str] = None
    eval_pass_min: Optional[float] = None
    eval_pass_max: Optional[float] = None
    eval_input_key: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class Entry(DictCompatModel):
    id: Optional[UUID] = None
    quest_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    item_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    asset_type: Optional[str] = None
    description: Optional[dict] = None
    review: Optional[dict] = None
    status: Literal["submitted", "accepted", "rejected"] = "submitted"
    reviewer_id: Optional[UUID] = None
    reviewed_at: Optional[str] = None
    eval_action_id: Optional[UUID] = None
    eval_score: Optional[float] = None
    eval_status: Optional[str] = None
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
