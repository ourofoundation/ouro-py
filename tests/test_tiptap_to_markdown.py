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


def _embed(asset_id, asset_type="dataset", **attrs):
    return {
        "type": "assetComponent",
        "attrs": {"id": asset_id, "assetType": asset_type, "viewMode": "card", **attrs},
    }


def _resolved(asset_id, **fields):
    return {"id": asset_id, "asset_type": "dataset", "name": "structures", **fields}


def test_embed_without_resolved_assets_is_unchanged():
    # No list means "not resolved": the markdown stays exactly what update_post takes.
    assert tiptap_to_markdown(_doc(_embed("abc"))) == (
        '```assetComponent\n{"id": "abc", "assetType": "dataset", "viewMode": "card"}\n```'
    )


def test_embed_gets_summary_from_resolved_assets():
    asset = _resolved(
        "abc",
        user={"username": "mmoderwell"},
        team={"name": "materials-science"},
        visibility="private",
        description={"json": _doc(_paragraph({"type": "text", "text": "One row per\n structure."})), "text": ""},
    )

    md = tiptap_to_markdown(_doc(_embed("abc")), embedded_assets=[asset])

    assert md.endswith(
        "\n↳ structures (dataset · @mmoderwell · #materials-science · private)"
        " — One row per structure."
    )


def test_public_and_inherited_embeds_omit_visibility_and_missing_fields():
    for visibility in ("public", "inherit"):
        md = tiptap_to_markdown(_doc(_embed("abc")), embedded_assets=[_resolved("abc", visibility=visibility)])

        assert md.splitlines()[-1] == "↳ structures (dataset)"


def test_unresolved_embed_says_it_is_not_available():
    md = tiptap_to_markdown(_doc(_embed("gone")), embedded_assets=[])

    assert md.splitlines()[-1] == "↳ Not available to you (deleted, or you don't have access)."


def test_partial_embed_gets_no_summary():
    md = tiptap_to_markdown(_doc(_embed("up", partial=True)), embedded_assets=[])

    assert "↳" not in md


def test_long_descriptions_are_cut_at_a_word():
    asset = _resolved("abc", description="word " * 100)

    line = tiptap_to_markdown(_doc(_embed("abc")), embedded_assets=[asset]).splitlines()[-1]
    description = line.split(" — ", 1)[1]

    assert description.endswith("word…")
    assert len(description) <= 201


def test_embeds_nested_in_lists_are_summarized():
    doc = _doc(
        {
            "type": "bulletList",
            "content": [{"type": "listItem", "content": [_paragraph({"type": "text", "text": "Data"}), _embed("abc")]}],
        }
    )

    assert "↳ structures (dataset)" in tiptap_to_markdown(doc, embedded_assets=[_resolved("abc")])


def test_description_to_markdown_passes_resolved_assets_through():
    from ouro.models import Post
    from ouro.utils.content import description_to_markdown

    post = Post.model_validate(
        {
            "id": "00000000-0000-0000-0000-000000000001",
            "user_id": "00000000-0000-0000-0000-000000000002",
            "org_id": "00000000-0000-0000-0000-000000000003",
            "team_id": "00000000-0000-0000-0000-000000000004",
            "asset_type": "post",
            "visibility": "public",
            "created_at": "2026-10-20T00:00:00Z",
            "last_updated": "2026-10-20T00:00:00Z",
            "content": {"json": _doc(_embed("00000000-0000-0000-0000-0000000000aa")), "text": ""},
            "assets": [_resolved("00000000-0000-0000-0000-0000000000aa")],
        }
    )

    assert description_to_markdown(post.content, embedded_assets=post.assets).endswith(
        "↳ structures (dataset)"
    )
