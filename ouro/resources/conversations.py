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
    def retrieve(self, conversation_id: str) -> Conversation:
        """Retrieve a conversation by id."""
        request = self.client.get(f"/conversations/{conversation_id}")
        return Conversation(**self._handle_response(request), _ouro=self.ouro)

    def list(self, **kwargs) -> List[Conversation]:
        """List all conversations."""
        request = self.client.get("/conversations", params=kwargs)
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
