from __future__ import annotations

import logging
from typing import List, Optional

from ouro._resource import SyncAPIResource, _strip_none
from ouro.models import Comment

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
        **kwargs,
    ) -> Comment:
        """Create a new Comment."""
        comment = _strip_none({
            **kwargs,
            "parent_id": parent_id,
            "source": "api",
            "asset_type": "comment",
        })

        request = self.client.post(
            "/comments/create",
            json={
                "comment": comment,
                "content": content.to_dict(),
            },
        )
        return Comment(**self._handle_response(request))

    def retrieve(self, id: str) -> Comment:
        """Retrieve a Comment by its id."""
        request = self.client.get(f"/comments/{id}")
        return Comment(**self._handle_response(request))

    def list_by_parent(self, parent_id: str) -> List[Comment]:
        """List all comments for a parent asset or comment (one-level replies)."""
        request = self.client.get(f"/assets/{parent_id}/comments")
        return [Comment(**c) for c in self._handle_response(request)]

    def list_replies(self, comment_id: str) -> List[Comment]:
        """List replies for a top-level comment (one-level deep)."""
        return self.list_by_parent(comment_id)

    def update(
        self,
        id: str,
        content: Optional["Content"] = None,
        **kwargs,
    ) -> Comment:
        """Update a Comment by its id."""
        comment = _strip_none({**kwargs})

        request = self.client.put(
            f"/comments/{id}",
            json={
                "comment": comment,
                "content": content.to_dict() if content is not None else None,
            },
        )
        return Comment(**self._handle_response(request))

    def delete(self, id: str) -> None:
        """Delete a Comment (and its reply thread) by its id.

        The backend's generic comment-delete path runs through
        ``DELETE /posts/:id`` — ``has_delete_permission`` is asset-type
        agnostic, and the deletion cascades into nested replies. This
        mirrors what the Ouro web app does. Dedicated ``DELETE /comments/:id``
        is not wired on the backend as of 2026-04-17; we'll point this at
        the dedicated route if/when that changes.
        """
        request = self.client.delete(f"/posts/{id}")
        self._handle_response(request)
        return None
