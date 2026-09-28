from __future__ import annotations

import logging
from typing import List, Optional

from ouro._resource import (
    SyncAPIResource,
    _ensure_attribution,
    _optional_attribution,
    _strip_none,
)
from ouro.models import Comment, DeleteResult, Page

from .content import Content, Editor

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Comments"]


class Comments(SyncAPIResource):
    def Editor(self, **kwargs) -> Editor:
        """Create an Editor instance connected to the Ouro client."""
        return Editor(_ouro=self.ouro, **kwargs)

    def Content(self, **kwargs) -> "Content":
        """Create a Content instance connected to the Ouro client."""
        return Content(_ouro=self.ouro, **kwargs)

    def create(
        self,
        content: "Content",
        parent_id: str,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> Comment:
        """Create a new Comment."""
        comment = _strip_none({
            "license_id": license_id,
            **kwargs,
            "parent_id": parent_id,
            "source": "api",
            "asset_type": "comment",
        })
        comment["attribution"] = _ensure_attribution(attribution)

        request = self.client.post(
            "/comments/create",
            json={
                "comment": comment,
                "content": content.to_dict(),
            },
        )
        return self._parse(Comment, self._handle_response(request))

    def retrieve(self, id: str) -> Comment:
        """Retrieve a Comment by its id."""
        request = self.client.get(f"/comments/{id}")
        return self._parse(Comment, self._handle_response(request))

    def list(self, parent_id: str, limit: int = 50, offset: int = 0) -> Page[Comment]:
        """List a page of comments on an asset, or replies to a comment, oldest first.

        While ``page.has_more``, pass ``offset + len(page)`` to fetch the next page.

        Args:
            parent_id: Asset UUID for top-level comments, or comment UUID for replies.
            limit: Max comments to return (backend caps at 200; default 50).
            offset: Number of comments to skip.
        """
        request = self.client.get(
            f"/assets/{parent_id}/comments",
            params={"limit": limit, "offset": offset},
        )
        return self._page(Page[Comment], self._handle_response(request, raw=True))

    def list_by_parent(self, parent_id: str) -> List[Comment]:
        """List all comments for a parent asset or comment (one-level replies)."""
        request = self.client.get(f"/assets/{parent_id}/comments")
        return self._parse_list(Comment, self._handle_response(request))

    def list_replies(self, comment_id: str) -> List[Comment]:
        """List replies for a top-level comment (one-level deep)."""
        return self.list_by_parent(comment_id)

    def update(
        self,
        id: str,
        content: Optional["Content"] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> Comment:
        """Update a Comment by its id."""
        comment = _strip_none({
            "license_id": license_id,
            "attribution": _optional_attribution(attribution),
            **kwargs,
        })

        request = self.client.put(
            f"/comments/{id}",
            json={
                "comment": comment,
                "content": content.to_dict() if content is not None else None,
            },
        )
        return self._parse(Comment, self._handle_response(request))

    def delete(
        self, id: str, *, delete_children: bool = False, dry_run: bool = False
    ) -> DeleteResult:
        """Delete a Comment (and its reply thread) by its id.

        The backend's generic comment-delete path runs through
        ``DELETE /posts/:id`` — ``has_delete_permission`` is asset-type
        agnostic, and the deletion cascades into nested replies. This
        mirrors what the Ouro web app does. Dedicated ``DELETE /comments/:id``
        is not wired on the backend as of 2026-04-17; we'll point this at
        the dedicated route if/when that changes.
        """
        return self.ouro.posts.delete(
            id, delete_children=delete_children, dry_run=dry_run
        )
