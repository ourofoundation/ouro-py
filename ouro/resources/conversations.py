from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ouro._resource import (
    SyncAPIResource,
    _ensure_attribution,
    _optional_attribution,
    _strip_none,
)
from ouro.models import Conversation, Message, Page

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Conversations", "Messages"]


class Messages(SyncAPIResource):
    def create(self, conversation_id: str, **kwargs) -> Message:
        """Send a message. Pass ``text`` and/or a TipTap ``json`` document."""
        request = self.client.post(
            f"/conversations/{conversation_id}/messages/create",
            json={"message": _strip_none(kwargs)},
        )
        return self._parse(Message, self._handle_response(request))

    def update(self, conversation_id: str, message_id: str, **kwargs) -> Message:
        request = self.client.patch(
            f"/conversations/{conversation_id}/messages/{message_id}",
            json={"message": _strip_none(kwargs)},
        )
        return self._parse(Message, self._handle_response(request))

    def list(
        self,
        conversation_id: str,
        limit: int = 50,
        before: Optional[str] = None,
    ) -> Page[Message]:
        """List a page of messages in a conversation, oldest first.

        The backend pages this endpoint with a ``before`` timestamp cursor,
        not offset/limit. While ``page.has_more``, pass
        ``page.next_cursor["before"]`` as ``before`` to load older messages.

        Args:
            conversation_id: Conversation UUID.
            limit: Max messages to return (backend caps at 200; default 50).
            before: ISO timestamp cursor; messages strictly older than this
                are returned. Omit for the newest page.
        """
        params: Dict[str, Any] = {"limit": limit}
        if before is not None:
            params["before"] = before
        request = self.client.get(
            f"/conversations/{conversation_id}/messages", params=params
        )
        return self._page(Page[Message], self._handle_response(request, raw=True))


class ConversationMessages:
    """Messages scoped to one conversation (``conversation.messages``)."""

    def __init__(self, ouro, conversation_id: str):
        self._messages = Messages(ouro)
        self.conversation_id = conversation_id

    def create(self, **kwargs) -> Message:
        return self._messages.create(self.conversation_id, **kwargs)

    def list(self, **kwargs) -> Page[Message]:
        return self._messages.list(self.conversation_id, **kwargs)


class Conversations(SyncAPIResource):
    def create(
        self,
        member_user_ids: List[str],
        name: Optional[str] = None,
        summary: Optional[str] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> Conversation:
        """Create a conversation with the specified member user IDs."""
        conversation = _strip_none(
            {
                "name": name,
                "summary": summary,
                "org_id": org_id,
                "team_id": team_id,
                "license_id": license_id,
                "metadata": {"members": member_user_ids},
                **kwargs,
            }
        )
        conversation["attribution"] = _ensure_attribution(attribution)

        request = self.client.post(
            "/conversations/create",
            json={"conversation": conversation},
        )
        return self._parse(Conversation, self._handle_response(request))

    def retrieve(self, conversation_id: str) -> Conversation:
        """Retrieve a conversation by id."""
        request = self.client.get(f"/conversations/{conversation_id}")
        return self._parse(Conversation, self._handle_response(request))

    def list(
        self,
        org_id: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Page[Conversation]:
        """List a page of conversations, optionally within one organization."""
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if org_id is not None:
            params["org_id"] = org_id

        request = self.client.get("/conversations", params=params)
        return self._page(Page[Conversation], self._handle_response(request, raw=True))

    def update(
        self,
        conversation_id: str,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> Conversation:
        """Update a conversation."""
        conversation = _strip_none({
            "license_id": license_id,
            "attribution": _optional_attribution(attribution),
            **kwargs,
        })
        request = self.client.put(
            f"/conversations/{conversation_id}", json={"conversation": conversation}
        )
        return self._parse(Conversation, self._handle_response(request))

    def delete(self, conversation_id: str) -> None:
        """Delete (or leave) a conversation.

        Backend semantics: if the authenticated user is the only remaining
        member, the conversation and all its messages are deleted. Otherwise
        the user is removed from ``metadata.members`` and a ``member_left``
        event is appended — i.e. this doubles as "leave the conversation" for
        multi-member threads.
        """
        request = self.client.delete(f"/conversations/{conversation_id}")
        self._handle_response(request)
