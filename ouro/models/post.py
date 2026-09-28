from typing import Optional

from pydantic import Field

from .asset import Asset, RichText

__all__ = ["Comment", "Post"]


class Post(Asset):
    content: Optional[RichText] = None
    comments: Optional[int] = Field(default=0)
    views: Optional[int] = Field(default=0)


class Comment(Asset):
    content: Optional[RichText] = None

    @property
    def text(self) -> str:
        """Plain comment body.

        Prefer full ``content.text``; fall back to the truncated
        ``description.text`` preview the list endpoint may return.
        """
        if self.content is not None and self.content.text:
            return self.content.text
        return self.description.text if self.description is not None else ""
