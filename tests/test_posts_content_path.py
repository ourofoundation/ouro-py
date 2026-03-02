from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ouro.resources.posts import Posts


class _FakeContent:
    def __init__(self) -> None:
        self.markdown = None

    def from_markdown(self, markdown: str) -> None:
        self.markdown = markdown


class _FakePostsSelf:
    def Content(self, **kwargs):  # noqa: N802 - mirrors SDK API name
        return _FakeContent()


class TestPostsResolveContent(unittest.TestCase):
    def test_returns_explicit_content_unchanged(self) -> None:
        sentinel = _FakeContent()

        resolved = Posts._resolve_content(
            _FakePostsSelf(),
            content=sentinel,
            content_markdown=None,
            content_path=None,
        )

        self.assertIs(resolved, sentinel)

    def test_rejects_multiple_content_inputs(self) -> None:
        with self.assertRaises(ValueError):
            Posts._resolve_content(
                _FakePostsSelf(),
                content=_FakeContent(),
                content_markdown="# hello",
                content_path=None,
            )

    def test_rejects_missing_all_content_inputs(self) -> None:
        with self.assertRaises(ValueError):
            Posts._resolve_content(
                _FakePostsSelf(),
                content=None,
                content_markdown=None,
                content_path=None,
            )

    def test_rejects_non_markdown_content_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "post.txt"
            path.write_text("hello", encoding="utf-8")

            with self.assertRaises(ValueError):
                Posts._resolve_content(
                    _FakePostsSelf(),
                    content=None,
                    content_markdown=None,
                    content_path=str(path),
                )

    def test_reads_markdown_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "post.md"
            path.write_text("# Hello from file\n", encoding="utf-8")

            resolved = Posts._resolve_content(
                _FakePostsSelf(),
                content=None,
                content_markdown=None,
                content_path=str(path),
            )

            self.assertEqual(resolved.markdown, "# Hello from file\n")


if __name__ == "__main__":
    unittest.main()
