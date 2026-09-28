from __future__ import annotations

import pandas as pd
import pytest

from ouro.models import Comment, Post

MARKDOWN = """# Findings

Some **bold** text and a list:

- first
- second

```python
print("hi")
```
"""


@pytest.fixture(scope="module")
def post(ouro, track):
    return track.add(
        ouro.posts.create(
            name=track.name("post"),
            content_markdown=MARKDOWN,
            visibility="private",
            description="A post made by the integration suite",
        )
    )


def test_create_from_markdown(post):
    assert isinstance(post, Post)
    assert post.asset_type == "post"
    assert "Findings" in post.content.text
    node_types = [node["type"] for node in post.content.data["content"]]
    assert {"heading", "bulletList", "codeBlock"} <= set(node_types)


def test_retrieve_and_convert_back_to_markdown(ouro, post):
    fetched = ouro.posts.retrieve(str(post.id))
    content = ouro.posts.Content(json=fetched.content.data, text=fetched.content.text)
    md = content.to_markdown()
    assert "**bold**" in md
    assert "- first" in md
    assert 'print("hi")' in md


def test_leading_heading_is_preserved(ouro, post):
    fetched = ouro.posts.retrieve(str(post.id))
    heading = fetched.content.data["content"][0]
    assert heading["content"][0]["text"] == "Findings"
    assert fetched.content.text.lstrip("# ").startswith("Findings")


def test_create_from_path(ouro, track, tmp_path):
    path = tmp_path / "report.md"
    path.write_text("## From a file\n\nBody text.")
    created = track.add(ouro.posts.create(name=track.name("from-path"), content_path=str(path), visibility="private"))
    assert "From a file" in created.content.text


def test_content_arguments_are_exclusive(ouro, tmp_path):
    with pytest.raises(ValueError):
        ouro.posts.create(name="x", content_markdown="a", content_path="b.md")
    with pytest.raises(ValueError):
        ouro.posts.create(name="x")
    txt = tmp_path / "notes.txt"
    txt.write_text("x")
    with pytest.raises(ValueError):
        ouro.posts.create(name="x", content_path=str(txt))


def test_editor_with_embeds_and_partial_file(ouro, track):
    dataset = track.add(
        ouro.datasets.create(name=track.name("embedded"), visibility="private", data=[{"a": 1}])
    )
    editor = ouro.posts.Editor()
    editor.new_header(level=1, text="Built with the editor")
    editor.new_paragraph(text="A paragraph.")
    editor.new_code_block("x = 1", language="python")
    editor.new_table(pd.DataFrame({"k": ["a", "b"], "v": [1, 2]}))
    editor.new_inline_asset(str(dataset.id), asset_type="dataset", view_mode="preview")
    editor.new_partial_asset(
        ouro.files.partial_from_bytes(b"<h1>hi</h1>", "chart.html", name=track.name("partial"))
    )
    created = track.add(ouro.posts.create(name=track.name("editor"), content=editor, visibility="private"))

    nodes = created.content.data["content"]
    embeds = [n for n in nodes if n["type"] == "assetComponent"]
    assert any(n["attrs"]["id"] == str(dataset.id) for n in embeds)
    materialized = [n for n in embeds if n["attrs"].get("assetType") == "file"]
    assert materialized and not materialized[0]["attrs"].get("partial")

    summary = ouro.posts.delete(str(created.id), dry_run=True, delete_children=True)
    assert any(child.asset_type == "file" for child in summary.deleted_children)


def test_header_level_validation(ouro):
    with pytest.raises(ValueError):
        ouro.posts.Editor().new_header(level=4, text="too deep")


def test_update_content_and_name(ouro, post):
    content = ouro.posts.Content()
    content.from_markdown("Replaced body.")
    updated = ouro.posts.update(str(post.id), content=content, name=post.name + "-v2")
    assert updated.name == post.name + "-v2"
    assert "Replaced body." in ouro.posts.retrieve(str(post.id)).content.text


def test_list_posts(ouro, post):
    found = ouro.posts.list(scope="personal", sort="recent", limit=50)
    assert any(p.id == post.id for p in found)


def test_comment_thread(ouro, other, post):
    ouro.assets.share(str(post.id), other.user.id, role="read")

    comment = ouro.comments.create(content=ouro.comments.Content(text="First!"), parent_id=str(post.id))
    assert isinstance(comment, Comment)
    assert comment.text.strip() == "First!"

    reply = other.comments.create(content=other.comments.Content(text="A reply"), parent_id=str(comment.id))
    top_level = ouro.comments.list_by_parent(str(post.id))
    assert [c.id for c in top_level] == [comment.id]
    assert [r.id for r in ouro.comments.list_replies(str(comment.id))] == [reply.id]

    edited = ouro.comments.update(str(comment.id), content=ouro.comments.Content(text="First (edited)"))
    assert "edited" in edited.text
    assert "edited" in ouro.comments.retrieve(str(comment.id)).text

    ouro.comments.delete(str(comment.id))
    assert ouro.comments.list_by_parent(str(post.id)) == []
