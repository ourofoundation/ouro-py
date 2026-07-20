from __future__ import annotations

import logging
from typing import Any, Dict, List, Literal, Optional, Union

from ouro._resource import SyncAPIResource, _coerce_description, _ensure_attribution, _strip_none
from ouro.models import Entry, Quest, QuestItem

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Quests"]


class Quests(SyncAPIResource):
    def Content(self, **kwargs) -> "Content":
        """Create a Content instance connected to the Ouro client."""
        return Content(_ouro=self.ouro, **kwargs)

    def list(
        self,
        query: str = "",
        limit: int = 20,
        offset: int = 0,
        scope: Optional[str] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        sort: Optional[str] = None,
        time_window: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Quest]:
        """List quests, optionally filtered by search query and scope."""
        results = self.ouro.assets.search(
            query=query,
            asset_type="quest",
            limit=limit,
            offset=offset,
            scope=scope,
            org_id=org_id,
            team_id=team_id,
            sort=sort,
            time_window=time_window,
            **kwargs,
        )
        return [Quest(**item) for item in results]

    def list_assigned_items(
        self,
        *,
        status: Optional[Union[str, List[str]]] = None,
        assignee_id: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        with_pagination: bool = False,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List quest items assigned to a user.

        Defaults to the authenticated user and actionable statuses
        (``pending,in_progress``). Pass ``status="all"`` to include terminal
        items.
        """
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if status is not None:
            params["status"] = ",".join(status) if isinstance(status, list) else status
        if assignee_id:
            params["assignee_id"] = assignee_id
        if org_id:
            params["org_id"] = org_id
        if team_id:
            params["team_id"] = team_id

        request = self.client.get("/quests/assigned-items", params=params)
        data = self._handle_response(request)
        if with_pagination:
            return data
        if isinstance(data, dict):
            return data.get("data") or []
        return data or []

    def create(
        self,
        name: str,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        type: str = "closable",
        status: str = "open",
        items: Optional[List[Union[str, Dict]]] = None,
        **kwargs,
    ) -> Quest:
        """Create a new Quest with optional items.

        Args:
            type: ``"closable"`` (default) or ``"continuous"``. Closable quests
                allow one active entry per contributor per item (status
                ``submitted`` or ``accepted``). Continuous quests allow unlimited
                entries per item while the quest is open.
            status: Quest lifecycle status ("draft", "open", "closed", "cancelled").
            items: List of task descriptions (strings) or dicts with item fields
                   (description, expected_asset_type, reward_currency,
                   reward_amount, etc.).
        """
        quest = _strip_none(
            {
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "type": type,
                "status": status,
                "items": [
                    {"description": i} if isinstance(i, str) else i
                    for i in (items or [])
                ]
                or None,
                **kwargs,
                "source": "api",
            }
        )
        quest["attribution"] = _ensure_attribution(quest.pop("attribution", None))

        request = self.client.post(
            "/quests/create",
            json={"quest": quest},
        )
        return Quest(**self._handle_response(request))

    def retrieve(self, id: str) -> Quest:
        """Retrieve a Quest by its id, including items and progress."""
        request = self.client.get(f"/quests/{id}")
        return Quest(**self._handle_response(request))

    def update(
        self,
        id: str,
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        status: Optional[str] = None,
        type: Optional[str] = None,
        **kwargs,
    ) -> Quest:
        """Update a Quest by its id.

        `status` uses the canonical lifecycle: "draft", "open", "closed", "cancelled".
        """
        quest = _strip_none(
            {
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "status": status,
                "type": type,
                **kwargs,
            }
        )

        request = self.client.put(
            f"/quests/{id}",
            json={"quest": quest},
        )
        return Quest(**self._handle_response(request))

    def delete(self, id: str) -> None:
        """Delete a Quest by its id."""
        request = self.client.delete(f"/quests/{id}")
        self._handle_response(request, raw=True)

    # ── Quest Item methods ──

    def list_items(self, quest_id: str) -> List[QuestItem]:
        """List items for a quest, ordered by sort_order."""
        request = self.client.get(f"/quests/{quest_id}/items")
        data = self._handle_response(request)
        if isinstance(data, list):
            return [QuestItem(**item) for item in data]
        return []

    def create_items(
        self,
        quest_id: str,
        items: List[Union[str, Dict]],
    ) -> List[QuestItem]:
        """Batch-create items on a quest.

        Args:
            items: List of task descriptions (strings) or dicts with item fields.
        """
        rows = [
            {"description": i} if isinstance(i, str) else i for i in items
        ]
        request = self.client.post(
            f"/quests/{quest_id}/items",
            json={"items": rows},
        )
        data = self._handle_response(request)
        if isinstance(data, list):
            return [QuestItem(**item) for item in data]
        return []

    def update_item(self, quest_id: str, item_id: str, **kwargs) -> QuestItem:
        """Update an item's metadata, status, rewards, or notes."""
        request = self.client.put(
            f"/quests/{quest_id}/items/{item_id}",
            json={"item": _strip_none(kwargs)},
        )
        return QuestItem(**self._handle_response(request))

    def complete_item(
        self,
        quest_id: str,
        item_id: str,
        *,
        assets: Optional[dict] = None,
        description: Optional[Union[str, Content, dict]] = None,
    ) -> dict:
        """Self-complete an item. Creates an auto-accepted entry and marks item done.

        The quest must be ``open``; draft, closed, and cancelled quests do not
        accept entry-producing actions.

        Args:
            assets: Optional keyed submission inputs (e.g. ``{"file": "<uuid>"}``).
            description: Markdown, Content, or raw content dict describing what
                         was done, tried, and learned.
        """
        body = _strip_none(
            {
                "assets": assets,
                "description": _coerce_description(description),
            }
        )
        request = self.client.post(
            f"/quests/{quest_id}/items/{item_id}/complete",
            json=body,
        )
        return self._handle_response(request)

    def delete_item(self, quest_id: str, item_id: str) -> None:
        """Delete an item (blocked if it has entries)."""
        request = self.client.delete(
            f"/quests/{quest_id}/items/{item_id}",
        )
        self._handle_response(request, raw=True)

    # ── Quest Entry methods ──

    def create_entry(
        self,
        quest_id: str,
        *,
        item_id: str,
        assets: Optional[dict] = None,
        description: Optional[Union[str, Content, dict]] = None,
    ) -> Entry:
        """Submit an entry to a quest item.

        The quest must be ``open``. Draft quests are configuration-only and
        reject submissions until the owner publishes them.

        Pass ``item_id`` and, when attaching assets, ``assets`` keyed by submission
        input name (e.g. ``{"file": "<uuid>"}``). The API resolves ``asset_type``.
        Provide ``description`` with the contributor-facing explanation reviewers
        should use alongside deterministic judge signals. Private assets stay
        private until the author accepts the entry.

        **Submission limits** depend on the quest ``type`` (see ``create``):

        - **closable** — at most one active entry per ``(item_id, caller)`` while
          status is ``submitted`` or ``accepted``. Submit again only after rejection.
        - **continuous** — no per-user cap; each call creates a new entry.

        Raises an API error if a closable quest already has an active entry for
        the same item, or if any submitted asset is already on another active
        entry for this quest.
        """
        entry = _strip_none(
            {
                "item_id": item_id,
                "assets": assets,
                "description": _coerce_description(description),
            }
        )
        request = self.client.post(
            f"/quests/{quest_id}/entries/create",
            json={"entry": entry},
        )
        return Entry(**self._handle_response(request))

    def list_entries(
        self,
        quest_id: str,
        *,
        status: Optional[Literal["submitted", "accepted", "rejected"]] = None,
        limit: int = 50,
        offset: int = 0,
        with_pagination: bool = False,
    ) -> Union[List[Entry], dict]:
        """List entries for a quest, optionally including pagination metadata."""
        request = self.client.get(
            f"/quests/{quest_id}/entries",
            params=_strip_none(
                {
                    "status": status,
                    "limit": limit,
                    "offset": offset,
                }
            ),
        )
        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            return {
                "data": [Entry(**entry) for entry in result.get("data", [])],
                "pagination": result.get("pagination", {}),
            }

        data = self._handle_response(request)
        if isinstance(data, list):
            return [Entry(**entry) for entry in data]
        return []

    def review_entry(
        self,
        quest_id: str,
        entry_id: str,
        *,
        status: Literal["accepted", "rejected"],
        review: Optional[Union[str, Content, dict]] = None,
    ) -> Entry:
        """Accept or reject a quest entry."""
        request = self.client.put(
            f"/quests/{quest_id}/entries/{entry_id}/review",
            json=_strip_none(
                {
                    "status": status,
                    "review": _coerce_description(review),
                }
            ),
        )
        return Entry(**self._handle_response(request))
