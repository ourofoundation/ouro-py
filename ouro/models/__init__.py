from typing import TYPE_CHECKING, Any, Dict, List, Optional, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

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
    submission_assets: Optional[dict] = None
    eval_static_inputs: Optional[dict] = None
    notes: Optional[str] = None
    waiting_on: Optional[str] = None
    waiting_until: Optional[str] = None
    waiting_check_every: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class Entry(DictCompatModel):
    id: Optional[UUID] = None
    quest_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    item_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    asset_type: Optional[str] = None
    assets: Optional[dict] = None
    embedded_assets: Optional[List[Any]] = None
    users: Optional[List[Any]] = None
    description: Optional[dict] = None
    review: Optional[dict] = None
    status: Literal["submitted", "accepted", "rejected"] = "submitted"
    reviewer_id: Optional[UUID] = None
    reviewed_at: Optional[str] = None
    eval_action_id: Optional[UUID] = None
    eval_score: Optional[float] = None
    eval_status: Optional[str] = None
    judge_signals: Optional[dict] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @field_validator("assets", mode="before")
    @classmethod
    def _coerce_submission_assets(cls, value: Any) -> Any:
        # Read responses only: `assets` is keyed submission refs (dict).
        # Ignore mistaken [] from older list endpoints; embeds use embedded_assets.
        if value is None or value == {} or isinstance(value, list):
            return None
        return value


class QuestProgress(BaseModel):
    total: int = 0
    resolved: int = 0
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


class DatasetRef(BaseModel):
    # Per-column declaration for dataset reference columns. The backend adds the
    # FK to the table named by `kind` (public.assets or public.actions);
    # asset_type refines display/validation for the "asset" kind only.
    kind: Literal["asset", "action"] = "asset"
    asset_type: Optional[str] = None


class DatasetMetadata(BaseModel):
    table_name: str
    columns: List[str]
    refs: Optional[Dict[str, DatasetRef]] = None


class Dataset(Asset):
    preview: Optional[List[dict]] = None


class Comment(Asset):
    content: Optional[PostContent] = None
