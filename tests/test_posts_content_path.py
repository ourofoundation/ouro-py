from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ouro.resources.content import Content, parse_unconnected_text
from ouro.resources.posts import Posts


class _FakeContent:
    def __init__(self) -> None:
        self.markdown = None

    def from_markdown(self, markdown: str) -> None:
        self.markdown = markdown


class _FakePostsSelf:
    def Content(self, **kwargs):  # noqa: N802 - mirrors SDK API name
        return _FakeContent()

    ouro = object()


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

    def test_reparses_unconnected_content_text_as_markdown(self) -> None:
        source = "{@mmoderwell}, tarea.\n\n**Objetivo:** usted."
        raw = Content(text=source)

        with mock.patch.object(Content, "from_markdown", autospec=True) as from_markdown:
            resolved = Posts._resolve_content(
                _FakePostsSelf(),
                content=raw,
                content_markdown=None,
                content_path=None,
            )

        from_markdown.assert_called_once_with(resolved, source)
        self.assertIsNot(resolved, raw)
        self.assertEqual(raw.json["content"][0]["content"][0]["text"], "{@mmoderwell}, tarea.")


class TestParseUnconnectedText(unittest.TestCase):
    def _parse(self, content: Content) -> Content:
        with mock.patch.object(Content, "from_markdown", autospec=True):
            return parse_unconnected_text(content, ouro=object())

    def test_parses_plain_text_content(self) -> None:
        raw = Content(text="{@deleckir}, esta es tu primera tarea.")
        self.assertIsNot(self._parse(raw), raw)

    def test_keeps_structured_content(self) -> None:
        structured = Content(
            json={
                "type": "doc",
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": "already parsed"}],
                    }
                ],
            },
            text="{@mmoderwell} **not** reparsed",
        )
        self.assertIs(self._parse(structured), structured)

    def test_keeps_connected_content(self) -> None:
        connected = Content(text="hi")
        connected._ouro = object()
        self.assertIs(self._parse(connected), connected)

    def test_keeps_empty_content(self) -> None:
        empty = Content()
        self.assertIs(self._parse(empty), empty)


if __name__ == "__main__":
    unittest.main()
