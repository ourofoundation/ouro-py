from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field

from ._base import OuroModel
from .asset import Asset, UserProfile

if TYPE_CHECKING:
    from ouro.resources.conversations import ConversationMessages

__all__ = ["Conversation", "ConversationMetadata", "Message"]


class ConversationMetadata(OuroModel):
    members: List[UUID]
    summary: Optional[str] = None


class Conversation(Asset):
    summary: Optional[str] = None
    metadata: ConversationMetadata
    unreads: Optional[int] = None

    @property
    def messages(self) -> "ConversationMessages":
        from ouro.resources.conversations import ConversationMessages

        return ConversationMessages(self._require_client(), str(self.id))


class Message(OuroModel):
    id: UUID
    conversation_id: UUID
    user_id: UUID
    user: Optional[UserProfile] = None
    org_id: Optional[UUID] = None
    type: Optional[str] = None
    text: str = ""
    # The TipTap document behind ``text``.
    data: Optional[Dict[str, Any]] = Field(default=None, alias="json")
    turn_id: Optional[UUID] = None
    seq: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
