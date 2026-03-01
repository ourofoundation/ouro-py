from __future__ import annotations

import logging
from typing import Optional, Union

from ouro._resource import SyncAPIResource, _coerce_description, _strip_none
from ouro.models import Post

from .content import Content, Editor

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Posts"]


class Posts(SyncAPIResource):
    def Editor(self, **kwargs) -> Editor:
        """Create an Editor instance connected to the Ouro client."""
        return Editor(_ouro=self.ouro, **kwargs)

    def Content(self, **kwargs) -> "Content":
        """Create a Content instance connected to the Ouro client."""
        return Content(_ouro=self.ouro, **kwargs)

    def create(
        self,
        content: "Content",
        name: str,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        **kwargs,
    ) -> Post:
        """Create a new Post."""
        post = _strip_none({
            "name": name,
            "description": _coerce_description(description),
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            **kwargs,
            "source": "api",
            "asset_type": "post",
        })

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
        post = _strip_none({
            "name": name,
            "description": _coerce_description(description),
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            **kwargs,
        })

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
