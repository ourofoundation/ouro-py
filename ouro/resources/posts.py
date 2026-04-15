from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, List, Optional, Union

from ouro._resource import SyncAPIResource, _coerce_description, _strip_none
from ouro.models import Post

from .content import Content, Editor

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Posts"]


class Posts(SyncAPIResource):
    def _resolve_content(
        self,
        content: Optional["Content"],
        content_markdown: Optional[str],
        content_path: Optional[str],
    ) -> "Content":
        provided = [
            ("content", content is not None),
            ("content_markdown", content_markdown is not None),
            ("content_path", content_path is not None),
        ]
        selected = [name for name, is_set in provided if is_set]
        if len(selected) > 1:
            raise ValueError(
                f"Provide only one of content, content_markdown, or content_path (got: {', '.join(selected)})."
            )

        if content is not None:
            return content

        markdown: Optional[str] = content_markdown
        if content_path is not None:
            path = Path(content_path).expanduser()
            if not path.exists():
                raise ValueError(f"content_path not found: {content_path}")
            if not path.is_file():
                raise ValueError(f"content_path must point to a file: {content_path}")
            if path.suffix.lower() not in {".md", ".markdown"}:
                raise ValueError("content_path must be a .md or .markdown file.")
            markdown = path.read_text(encoding="utf-8")

        if markdown is None:
            raise ValueError(
                "No post body provided. Pass one of: content, content_markdown, or content_path."
            )

        resolved_content = self.Content()
        resolved_content.from_markdown(markdown)
        return resolved_content

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
    ) -> List[Post]:
        """List posts, optionally filtered by search query and scope.

        Args:
            sort: "relevant" | "recent" | "popular" | "updated"
            time_window: For sort="popular": "day" | "week" | "month" | "all".
                         Default: "month".
        """
        results = self.ouro.assets.search(
            query=query,
            asset_type="post",
            limit=limit,
            offset=offset,
            scope=scope,
            org_id=org_id,
            team_id=team_id,
            sort=sort,
            time_window=time_window,
            **kwargs,
        )
        return [Post(**item) for item in results]

    def Editor(self, **kwargs) -> Editor:
        """Create an Editor instance connected to the Ouro client."""
        return Editor(_ouro=self.ouro, **kwargs)

    def Content(self, **kwargs) -> "Content":
        """Create a Content instance connected to the Ouro client."""
        return Content(_ouro=self.ouro, **kwargs)

    def create(
        self,
        name: str,
        content: Optional["Content"] = None,
        content_markdown: Optional[str] = None,
        content_path: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        **kwargs,
    ) -> Post:
        """Create a new Post.

        Provide one of content, content_markdown, or content_path.
        When using content_markdown or content_path, omit content (or pass None).
        """
        content = self._resolve_content(
            content=content,
            content_markdown=content_markdown,
            content_path=content_path,
        )

        post = _strip_none(
            {
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                **kwargs,
                "source": "api",
                "asset_type": "post",
            }
        )

        request = self.client.post(
            "/posts/create",
            json={
                "post": post,
                "content": content.to_dict(),
            },
        )
        return Post(**self._handle_response(request))

    def retrieve(self, id: str) -> Post:
        """Retrieve a Post by its id."""
        request = self.client.get(f"/posts/{id}")
        return Post(**self._handle_response(request))

    def update(
        self,
        id: str,
        content: Optional["Content"] = None,
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        **kwargs,
    ) -> Post:
        """Update a Post by its id."""
        post = _strip_none(
            {
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                **kwargs,
            }
        )

        request = self.client.put(
            f"/posts/{id}",
            json={
                "post": post,
                "content": content.to_dict() if content is not None else None,
            },
        )
        return Post(**self._handle_response(request))

    def delete(self, id: str) -> None:
        """Delete a Post by its id."""
        request = self.client.delete(f"/posts/{id}")
        self._handle_response(request, raw=True)
