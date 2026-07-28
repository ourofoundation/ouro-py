"""Tests for Comment.text convenience property."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from uuid import uuid4

from ouro.models import Comment, PostContent


def _base_kwargs(**extra):
    now = datetime.now(timezone.utc)
    data = {
        "id": uuid4(),
        "user_id": uuid4(),
        "org_id": uuid4(),
        "team_id": uuid4(),
        "visibility": "public",
        "asset_type": "comment",
        "created_at": now,
        "last_updated": now,
        "name": "comment",
    }
    data.update(extra)
    return data


class CommentTextTests(unittest.TestCase):
    def test_prefers_content_text(self) -> None:
        comment = Comment(
            **_base_kwargs(
                content=PostContent.model_validate({"text": "full body", "json": {}}),
                description={"text": "preview only"},
            )
        )
        self.assertEqual(comment.text, "full body")

    def test_falls_back_to_description_dict(self) -> None:
        comment = Comment(**_base_kwargs(description={"text": "preview"}))
        self.assertEqual(comment.text, "preview")

    def test_empty_when_missing(self) -> None:
        comment = Comment(**_base_kwargs())
        self.assertEqual(comment.text, "")


if __name__ == "__main__":
    unittest.main()
