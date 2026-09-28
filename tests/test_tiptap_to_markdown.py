"""tiptap_to_markdown renders what agents write, so read-edit-write round trips."""

from ouro.utils.content import tiptap_to_markdown


def _doc(*blocks):
    return {"type": "doc", "content": list(blocks)}


def _paragraph(*inline):
    return {"type": "paragraph", "content": list(inline)}


def test_renders_inline_and_display_math():
    doc = _doc(
        _paragraph(
            {"type": "text", "text": "Energy "},
            {"type": "mathematics", "attrs": {"latex": "E=mc^2", "displayMode": "false"}},
            {"type": "text", "text": "."},
        ),
        _paragraph(
            {"type": "mathematics", "attrs": {"latex": "a^2+b^2=c^2", "displayMode": "true"}}
        ),
    )

    assert tiptap_to_markdown(doc) == "Energy \\(E=mc^2\\).\n\n\\[a^2+b^2=c^2\\]"


def test_renders_mentions_as_at_username():
    doc = _doc(
        _paragraph(
            {"type": "mention", "attrs": {"username": "hermes"}},
            {"type": "text", "text": " hi"},
        )
    )

    assert tiptap_to_markdown(doc) == "@hermes hi"


def test_renders_asset_embeds_as_single_line_json():
    doc = _doc(
        {
            "type": "assetComponent",
            "attrs": {"id": "abc", "assetType": "dataset", "viewMode": "preview"},
        }
    )

    assert tiptap_to_markdown(doc) == (
        '```assetComponent\n{"id": "abc", "assetType": "dataset", "viewMode": "preview"}\n```'
    )
