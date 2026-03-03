from __future__ import annotations

import logging
from typing import List, Optional

from ouro._resource import SyncAPIResource, _strip_none
from ouro.models import Conversation

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Conversations", "Messages"]


class Messages(SyncAPIResource):
    def create(self, conversation_id: str, **kwargs) -> dict:
        json = kwargs.get("json")
        text = kwargs.get("text")
        user_id = kwargs.get("user_id")
        message = _strip_none({
            "json": json,
            "text": text,
            "user_id": user_id,
            **kwargs,
        })

        request = self.client.post(
            f"/conversations/{conversation_id}/messages/create",
            json={"message": message},
        )
        return self._handle_response(request)

    def list(self, conversation_id: str, **kwargs) -> List[dict]:
        request = self.client.get(
            f"/conversations/{conversation_id}/messages", params=kwargs
        )
        return self._handle_response(request)


class ConversationMessages:
    def __init__(self, conversation: "Conversation"):
        self.conversation = conversation
        self._ouro = conversation._ouro

    def create(self, **kwargs) -> dict:
        return Messages(self._ouro).create(self.conversation.id, **kwargs)

    def list(self, **kwargs) -> List[dict]:
        return Messages(self._ouro).list(self.conversation.id, **kwargs)


class Conversations(SyncAPIResource):
    def create(
        self,
        member_user_ids: List[str],
        name: Optional[str] = None,
        summary: Optional[str] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        **kwargs,
    ) -> Conversation:
        """Create a conversation with the specified member user IDs."""
        conversation = _strip_none({
            "name": name,
            "summary": summary,
            "org_id": org_id,
            "team_id": team_id,
            "metadata": {"members": member_user_ids},
            **kwargs,
        })

        request = self.client.post(
            "/conversations/create",
            json={"conversation": conversation},
        )
        return Conversation(**self._handle_response(request), _ouro=self.ouro)

    def retrieve(self, conversation_id: str) -> Conversation:
        """Retrieve a conversation by id."""
        request = self.client.get(f"/conversations/{conversation_id}")
        return Conversation(**self._handle_response(request), _ouro=self.ouro)

    def list(
        self,
        org_id: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Conversation]:
        """List conversations with optional org filter and pagination."""
        params = {
            "limit": limit,
            "offset": offset,
        }
        if org_id is not None:
            params["org_id"] = org_id

        request = self.client.get("/conversations", params=params)
        return [
            Conversation(**c, _ouro=self.ouro)
            for c in self._handle_response(request)
        ]

    def update(self, conversation_id: str, **kwargs) -> Conversation:
        """Update a conversation."""
        request = self.client.put(
            f"/conversations/{conversation_id}", json={"conversation": kwargs}
        )
        return Conversation(**self._handle_response(request), _ouro=self.ouro)
