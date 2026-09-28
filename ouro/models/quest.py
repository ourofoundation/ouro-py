from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ._base import OuroModel, Page
from .asset import Asset, AssetRef, RichText, UserProfile

__all__ = [
    "Entry",
    "ItemCompletion",
    "LeaderboardPage",
    "Quest",
    "QuestDetails",
    "QuestItem",
    "QuestLeaderboardRow",
    "QuestProgress",
]

ItemStatus = Literal["pending", "in_progress", "done", "skipped"]


class QuestDetails(OuroModel):
    id: Optional[UUID] = None
    type: Optional[Literal["closable", "continuous"]] = None
    status: Optional[Literal["draft", "open", "closed", "cancelled"]] = None
    max_xp_per_contributor: Optional[int] = None


class QuestItem(OuroModel):
    id: Optional[UUID] = None
    quest_id: Optional[UUID] = None
    description: Optional[RichText] = None
    status: ItemStatus = "pending"
    auto_skipped: bool = False
    status_before_auto_skip: Optional[ItemStatus] = None
    sort_order: int = 0
    type: str = "task"
    created_by: Optional[UUID] = None
    assignee_id: Optional[UUID] = None
    assignee: Optional[UserProfile] = None
    expected_asset_type: Optional[str] = None
    reward_xp: int = 0
    reward_currency: str = "btc"
    reward_amount: int = 0
    child_quest_id: Optional[UUID] = None
    completed_entry_id: Optional[UUID] = None
    eval_route_id: Optional[UUID] = None
    eval_score_path: Optional[str] = None
    eval_categories_path: Optional[str] = None
    eval_pass_min: Optional[float] = None
    eval_pass_max: Optional[float] = None
    leaderboard_enabled: bool = False
    leaderboard_order: Optional[Literal["desc", "asc"]] = "desc"
    submission_assets: Optional[Dict[str, Any]] = None
    eval_static_inputs: Optional[Dict[str, Any]] = None
    notes: Optional[str] = None
    waiting_on: Optional[str] = None
    waiting_until: Optional[str] = None
    waiting_check_every: Optional[str] = None
    embedded_assets: Optional[List[Any]] = None
    users: Optional[List[Any]] = None
    # Present on assigned-item listings, which span many quests.
    quest: Optional[QuestDetails] = None
    quest_asset: Optional[Asset] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class Entry(OuroModel):
    id: Optional[UUID] = None
    quest_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    item_id: Optional[UUID] = None
    item: Optional[QuestItem] = None
    asset_id: Optional[UUID] = None
    asset_type: Optional[str] = None
    asset: Optional[Asset] = None
    # Submission inputs keyed by the item's contributor keys.
    assets: Optional[Dict[str, Any]] = None
    embedded_assets: Optional[List[Any]] = None
    users: Optional[List[Any]] = None
    description: Optional[RichText] = None
    review: Optional[RichText] = None
    status: Literal["submitted", "accepted", "rejected"] = "submitted"
    reviewer_id: Optional[UUID] = None
    reviewed_at: Optional[str] = None
    eval_action_id: Optional[UUID] = None
    eval_score: Optional[float] = None
    eval_category_scores: Optional[Dict[str, Any]] = None
    eval_status: Optional[str] = None
    judge_signals: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @field_validator("assets", mode="before")
    @classmethod
    def _empty_assets_as_none(cls, value: Any) -> Any:
        # Older list endpoints send [] here; embeds live on embedded_assets.
        return None if value == {} or isinstance(value, list) else value


class ItemCompletion(OuroModel):
    """The auto-accepted entry and the item it marked done."""

    entry: Entry
    item: QuestItem


class QuestProgress(OuroModel):
    total: int = 0
    resolved: int = 0
    remaining: int = 0


class QuestLeaderboardRow(OuroModel):
    placement: int
    entry_id: Optional[UUID] = None
    score: Optional[float] = None
    status: Optional[str] = None
    eval_status: Optional[str] = None
    eval_action_id: Optional[UUID] = None
    created_at: Optional[str] = None
    category_scores: Optional[Dict[str, Any]] = None
    user: Optional[UserProfile] = None
    asset: Optional[AssetRef] = None


class LeaderboardPage(Page[QuestLeaderboardRow]):
    """Ranked rows for one quest item, plus the item being ranked."""

    item: Optional[QuestItem] = None


class Quest(Asset):
    quest: Optional[QuestDetails] = None
    items: Optional[List[QuestItem]] = None
    progress: Optional[QuestProgress] = None
    comments: Optional[int] = Field(default=0)
