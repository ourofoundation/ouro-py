from typing import Any, Dict, List, Optional

from pydantic import Field

from .asset import Asset, RichText

__all__ = ["Comment", "Post"]


class Post(Asset):
    content: Optional[RichText] = None
    # Assets the content embeds or links, resolved by the server as the reader
    # (ones they can't open are left out). None when the server didn't resolve
    # them. Pass to description_to_markdown(embedded_assets=...) to annotate.
    assets: Optional[List[Dict[str, Any]]] = None
    comments: Optional[int] = Field(default=0)
    views: Optional[int] = Field(default=0)


class Comment(Asset):
    content: Optional[RichText] = None
    # Same as Post.assets.
    assets: Optional[List[Dict[str, Any]]] = None

    @property
    def text(self) -> str:
        """Plain comment body.

        Prefer full ``content.text``; fall back to the truncated
        ``description.text`` preview the list endpoint may return.
        """
        if self.content is not None and self.content.text:
            return self.content.text
        return self.description.text if self.description is not None else ""
