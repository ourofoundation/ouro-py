from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from ouro._resource import SyncAPIResource, _coerce_description, _strip_none
from ouro.models import Quest, QuestItem

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
            status: Quest lifecycle status ("draft", "open", "closed", "cancelled").
            items: List of task descriptions (strings) or dicts with item fields
                   (description, expected_asset_type, reward_sats, etc.)
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
        asset_id: Optional[str] = None,
        asset_type: Optional[str] = None,
        description: Optional[Union[str, Content, dict]] = None,
    ) -> dict:
        """Self-complete an item. Creates an auto-accepted entry and marks item done.

        Args:
            asset_id: Optional produced asset to link.
            asset_type: Asset type (required if asset_id is set).
            description: Markdown, Content, or raw content dict describing what
                         was done, tried, and learned.
        """
        body = _strip_none(
            {
                "asset_id": asset_id,
                "asset_type": asset_type,
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
