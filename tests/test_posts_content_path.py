from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from ouro._resource import _coerce_description
from ouro.resources.comments import Comments
from ouro.resources.content import Content, rich_text_payload
from ouro.resources.posts import Posts

_STRUCTURED_DOC = {
    "type": "doc",
    "content": [
        {
            "type": "paragraph",
            "content": [{"type": "text", "text": "kept", "marks": [{"type": "bold"}]}],
        }
    ],
}


class _Sent(Exception):
    """Raised by the fake client to stop after capturing a request."""


class _CapturingClient:
    def __init__(self) -> None:
        self.requests: list[dict] = []

    def _capture(self, path: str, json=None, **_):
        self.requests.append({"path": path, "json": json})
        raise _Sent

    post = put = _capture


def _fake_ouro() -> SimpleNamespace:
    return SimpleNamespace(client=_CapturingClient(), websocket=None)


class TestPostsResolveContent(unittest.TestCase):
    def _resolve(self, **kwargs):
        args = {"content": None, "content_markdown": None, "content_path": None}
        args.update(kwargs)
        return Posts._resolve_content(object(), **args)

    def test_returns_explicit_content_unchanged(self) -> None:
        content = Content(text="hi")
        self.assertIs(self._resolve(content=content), content)

    def test_returns_markdown_for_the_server_to_parse(self) -> None:
        self.assertEqual(self._resolve(content_markdown="# hi"), "# hi")

    def test_rejects_multiple_content_inputs(self) -> None:
        with self.assertRaises(ValueError):
            self._resolve(content=Content(text="x"), content_markdown="# hello")

    def test_rejects_missing_all_content_inputs(self) -> None:
        with self.assertRaises(ValueError):
            self._resolve()

    def test_rejects_non_markdown_content_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "post.txt"
            path.write_text("hello", encoding="utf-8")

            with self.assertRaises(ValueError):
                self._resolve(content_path=str(path))

    def test_reads_markdown_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "post.md"
            path.write_text("# Hello from file\n", encoding="utf-8")

            self.assertEqual(
                self._resolve(content_path=str(path)), "# Hello from file\n"
            )


class TestRichTextPayload(unittest.TestCase):
    def test_strings_and_dicts_pass_through(self) -> None:
        shaped = {"json": _STRUCTURED_DOC, "text": "kept"}
        self.assertEqual(rich_text_payload("**hi** {@mmoderwell}"), "**hi** {@mmoderwell}")
        self.assertIs(rich_text_payload(shaped), shaped)

    def test_unconnected_text_content_is_sent_as_markdown(self) -> None:
        source = "{@mmoderwell}, tarea.\n\n**Objetivo:** usted."
        self.assertEqual(rich_text_payload(Content(text=source)), source)

    def test_structured_content_is_sent_as_json(self) -> None:
        content = Content(json=_STRUCTURED_DOC, text="**kept**")
        self.assertEqual(
            rich_text_payload(content), {"json": _STRUCTURED_DOC, "text": "**kept**"}
        )

    def test_connected_content_is_sent_as_json(self) -> None:
        content = Content(text="hi")
        content._ouro = object()
        self.assertEqual(rich_text_payload(content), content.to_dict())

    def test_empty_content_is_sent_as_json(self) -> None:
        self.assertEqual(rich_text_payload(Content()), Content().to_dict())


class TestCoerceDescription(unittest.TestCase):
    def test_none_stays_none(self) -> None:
        self.assertIsNone(_coerce_description(None))

    def test_string_is_sent_as_markdown(self) -> None:
        self.assertEqual(_coerce_description("Ask {@mmoderwell}"), "Ask {@mmoderwell}")

    def test_unconnected_content_is_sent_as_markdown(self) -> None:
        self.assertEqual(_coerce_description(Content(text="**hola**")), "**hola**")


class TestRequestBodies(unittest.TestCase):
    def test_post_create_sends_markdown_content(self) -> None:
        ouro = _fake_ouro()
        posts = Posts(ouro)
        posts._scope_create = lambda post: post

        with self.assertRaises(_Sent):
            posts.create(name="Plan", content_markdown="Hi {@deleckir}", description="**d**")

        body = ouro.client.requests[0]["json"]
        self.assertEqual(body["content"], "Hi {@deleckir}")
        self.assertEqual(body["post"]["description"], "**d**")

    def test_comment_create_sends_markdown_content(self) -> None:
        ouro = _fake_ouro()

        with self.assertRaises(_Sent):
            Comments(ouro).create(
                Content(text="{@deleckir}, esta es tu primera tarea."),
                parent_id="00000000-0000-0000-0000-000000000001",
            )

        body = ouro.client.requests[0]["json"]
        self.assertEqual(body["content"], "{@deleckir}, esta es tu primera tarea.")


if __name__ == "__main__":
    unittest.main()
