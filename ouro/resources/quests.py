from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Literal, Optional, Union

from ouro._resource import (
    SyncAPIResource,
    _coerce_description,
    _ensure_attribution,
    _optional_attribution,
    _strip_none,
)
from ouro.models import DeleteResult, Entry, ItemCompletion, LeaderboardPage, Page, Quest, QuestItem

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Quests"]


def _parse_quest_item(value: Any) -> Optional[Dict[str, Any]]:
    """Return an item object embedded in JSON text, if present."""
    if isinstance(value, dict):
        if "description" in value:
            return value
        value = value.get("text")
    elif not isinstance(value, str):
        value = getattr(value, "text", None)
    if not isinstance(value, str) or not value.lstrip().startswith("{"):
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) and "description" in parsed else None


def _normalize_quest_item_input(item: Union[str, Dict]) -> Dict[str, Any]:
    """Lift plain strings / stringified item JSON and coerce description."""
    if isinstance(item, str):
        row = _parse_quest_item(item) or {"description": item}
    else:
        row = dict(item)

    nested = _parse_quest_item(row.get("description"))
    if nested:
        row.update(nested)

    if "description" in row and row["description"] is not None:
        row["description"] = _coerce_description(row["description"])
    return row


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
    ) -> Page[Quest]:
        """List quests, optionally filtered by search query and scope."""
        return self.ouro.assets._search(
            Quest,
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

    def list_assigned_items(
        self,
        *,
        status: Optional[Union[str, List[str]]] = None,
        assignee_id: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
    ) -> Page[QuestItem]:
        """List quest items assigned to a user, across quests.

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
        return self._page(Page[QuestItem], self._handle_response(request, raw=True))

    def create(
        self,
        name: str,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        type: str = "closable",
        status: str = "open",
        items: Optional[List[Union[str, Dict]]] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> Quest:
        """Create a new Quest with optional items.

        Args:
            type: ``"closable"`` (default) or ``"continuous"``. Closable quests
                allow one active entry per contributor per item (status
                ``submitted`` or ``accepted``). Continuous quests allow unlimited
                entries per item while the quest is open.
            status: Quest lifecycle status ("draft", "open", "closed", "cancelled").
            items: List of task descriptions (strings), TipTap Content dicts, or
                   full item objects (description, submission_assets,
                   reward_xp, reward_currency, reward_amount, etc.).
                   ``submission_assets`` declares what contributors attach,
                   keyed by input name, e.g.
                   ``{"file": {"asset_type": "file"}}``.
        """
        quest = _strip_none(
            {
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "type": type,
                "status": status,
                "items": [
                    _normalize_quest_item_input(i) for i in (items or [])
                ]
                or None,
                "license_id": license_id,
                **kwargs,
                "source": "api",
            }
        )
        quest["attribution"] = _ensure_attribution(attribution)

        request = self.client.post(
            "/quests/create",
            json={"quest": quest},
        )
        return self._parse(Quest, self._handle_response(request))

    def retrieve(self, id: str) -> Quest:
        """Retrieve a Quest by its id, including items and progress."""
        request = self.client.get(f"/quests/{id}")
        return self._parse(Quest, self._handle_response(request))

    def update(
        self,
        id: str,
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        status: Optional[str] = None,
        type: Optional[str] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
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
                "license_id": license_id,
                "attribution": _optional_attribution(attribution),
                **kwargs,
            }
        )

        request = self.client.put(
            f"/quests/{id}",
            json={"quest": quest},
        )
        return self._parse(Quest, self._handle_response(request))

    def delete(
        self, id: str, *, delete_children: bool = False, dry_run: bool = False
    ) -> DeleteResult:
        """Delete a Quest by its id.

        Args:
            id: Quest UUID.
            delete_children: When True, also delete child assets linked via
                ``parent_id``.
            dry_run: When True, return the delete summary without deleting.

        Returns:
            What was deleted, or would be when ``dry_run`` is true.
        """
        return self._delete(
            f"/quests/{id}", delete_children=delete_children, dry_run=dry_run
        )

    # ── Quest Item methods ──

    def list_items(self, quest_id: str) -> List[QuestItem]:
        """List items for a quest, ordered by sort_order."""
        request = self.client.get(f"/quests/{quest_id}/items")
        return self._parse_list(QuestItem, self._handle_response(request))

    def create_items(
        self,
        quest_id: str,
        items: List[Union[str, Dict]],
    ) -> List[QuestItem]:
        """Batch-create items on a quest.

        Args:
            items: List of task descriptions (strings), TipTap Content dicts, or
                   full item objects.
        """
        rows = [_normalize_quest_item_input(i) for i in items]
        request = self.client.post(
            f"/quests/{quest_id}/items",
            json={"items": rows},
        )
        return self._parse_list(QuestItem, self._handle_response(request))

    def update_item(self, quest_id: str, item_id: str, **kwargs) -> QuestItem:
        """Update an item's metadata, status, rewards, or notes."""
        item = _strip_none(kwargs)
        if "description" in item and item["description"] is not None:
            item["description"] = _coerce_description(item["description"])
        request = self.client.put(
            f"/quests/{quest_id}/items/{item_id}",
            json={"item": item},
        )
        return self._parse(QuestItem, self._handle_response(request))

    def complete_item(
        self,
        quest_id: str,
        item_id: str,
        *,
        assets: Optional[dict] = None,
        description: Optional[Union[str, Content, dict]] = None,
    ) -> ItemCompletion:
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
        return self._parse(ItemCompletion, self._handle_response(request))

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
        return self._parse(Entry, self._handle_response(request))

    def list_entries(
        self,
        quest_id: str,
        *,
        status: Optional[Literal["submitted", "accepted", "rejected"]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Page[Entry]:
        """List a page of entries for a quest."""
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
        return self._page(Page[Entry], self._handle_response(request, raw=True))

    def list_leaderboard(
        self,
        quest_id: str,
        item_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> LeaderboardPage:
        """List ranked scored entries for a leaderboard-enabled quest item.

        Rows are every non-rejected entry with a numeric ``eval_score``.
        Placement is 1-based and stable: score (direction from the item's
        ``leaderboard_order``), then earliest submission, then entry id.
        """
        request = self.client.get(
            f"/quests/{quest_id}/items/{item_id}/leaderboard",
            params=_strip_none({"limit": limit, "offset": offset}),
        )
        body = self._handle_response(request, raw=True) or {}
        return self._page(LeaderboardPage, body, item=body.get("item"))

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
        return self._parse(Entry, self._handle_response(request))
