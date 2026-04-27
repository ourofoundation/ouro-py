from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

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

    def list(
        self,
        conversation_id: str,
        limit: int = 50,
        before: Optional[str] = None,
        with_pagination: bool = False,
        **kwargs: Any,
    ) -> Union[List[dict], Dict[str, Any]]:
        """List messages in a conversation, newest-first then reversed.

        The backend pages this endpoint with a ``before`` timestamp cursor
        (not offset/limit) — pass the ``created_at`` of the oldest message
        in the previous page to load the next page.

        Args:
            conversation_id: Conversation UUID.
            limit: Max messages to return (backend caps at 200; default 50).
            before: ISO timestamp cursor; messages strictly older than this
                are returned. Omit for the newest page.
            with_pagination: If True, return
                ``{"data": [...], "pagination": {"limit", "hasMore",
                "nextCursor"}}``. The ``nextCursor`` is a dict like
                ``{"before": "<iso-timestamp>"}`` — pass its fields to the
                next call.

        Any extra ``**kwargs`` are forwarded as query params for forward
        compatibility, but note that the backend ignores unknown keys —
        ``offset`` specifically has no effect here.
        """
        params: Dict[str, Any] = {"limit": limit}
        if before is not None:
            params["before"] = before
        if kwargs:
            params.update(kwargs)

        request = self.client.get(
            f"/conversations/{conversation_id}/messages", params=params
        )
        body = self._handle_response(request, raw=True) or {}
        data = body.get("data") if isinstance(body, dict) else body
        if data is None:
            data = []
        if with_pagination:
            pagination: Dict[str, Any]
            if isinstance(body, dict) and isinstance(body.get("pagination"), dict):
                pagination = dict(body["pagination"])
            elif isinstance(body, dict) and "hasMore" in body:
                # Back-compat for older backends that returned top-level
                # ``hasMore`` instead of a ``pagination`` envelope.
                pagination = {"limit": limit, "hasMore": bool(body["hasMore"])}
            else:
                pagination = {"limit": limit, "hasMore": False}
            return {"data": data, "pagination": pagination}
        return data


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
        with_pagination: bool = False,
    ) -> Union[List[Conversation], Dict[str, Any]]:
        """List conversations with optional org filter and pagination.

        Returns a list of :class:`Conversation` by default. When
        ``with_pagination=True``, returns
        ``{"data": [Conversation, ...], "pagination": ...}`` so callers can
        implement their own paging.
        """
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if org_id is not None:
            params["org_id"] = org_id

        request = self.client.get("/conversations", params=params)
        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            if not isinstance(result, dict):
                return {"data": [], "pagination": None}
            items = result.get("data") or []
            result["data"] = [Conversation(**c, _ouro=self.ouro) for c in items]
            return result
        return [
            Conversation(**c, _ouro=self.ouro)
            for c in self._handle_response(request) or []
        ]

    def update(self, conversation_id: str, **kwargs) -> Conversation:
        """Update a conversation."""
        request = self.client.put(
            f"/conversations/{conversation_id}", json={"conversation": kwargs}
        )
        return Conversation(**self._handle_response(request), _ouro=self.ouro)

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
        return None
