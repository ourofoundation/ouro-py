from __future__ import annotations

import pandas as pd
import pytest

from ouro.models import Comment, Post
from ouro.utils.content import tiptap_to_markdown

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


CALLOUT = """Before the callout.

> [!WARNING]
> Check the units before trusting this number.

After the callout.
"""


def test_callout_round_trips_through_markdown(ouro, track):
    created = track.add(
        ouro.posts.create(name=track.name("callout"), content_markdown=CALLOUT, visibility="private")
    )
    fetched = ouro.posts.retrieve(str(created.id))
    callouts = [n for n in fetched.content.data["content"] if n["type"] == "callout"]
    assert len(callouts) == 1
    markdown = tiptap_to_markdown(fetched.content.data)
    assert "> [!WARNING]" in markdown
    assert "Check the units" in markdown


def test_post_downloads_as_markdown_without_credentials(ouro, track):
    import httpx

    post = track.add(
        ouro.posts.create(name=track.name("download"), content_markdown=MARKDOWN, visibility="private")
    )
    link = ouro.assets.create_download_url(str(post.id))
    assert link["asset_type"] == "post"
    assert link["file_name"].endswith(".md")
    response = httpx.get(link["download_url"], follow_redirects=True)
    assert response.status_code == 200
    assert "Findings" in response.text
    assert "- first" in response.text

    html = ouro.assets.create_download_url(str(post.id), format="html")
    response = httpx.get(html["download_url"], follow_redirects=True)
    assert response.status_code == 200
    assert "<" in response.text and "Findings" in response.text


def test_embedded_partials_become_children(ouro, track):
    editor = ouro.posts.Editor()
    editor.new_paragraph("Results below.")
    editor.new_partial_asset(
        ouro.datasets.partial(
            pd.DataFrame([{"x": 1, "y": 2}, {"x": 3, "y": 4}]), name=track.name("partial-ds")
        )
    )
    editor.new_partial_asset(ouro.posts.partial("## Method\n\nNotes.", name=track.name("partial-post")))
    parent = track.add(
        ouro.posts.create(name=track.name("with-partials"), content=editor, visibility="private")
    )
    embeds = [
        n["attrs"] for n in ouro.posts.retrieve(str(parent.id)).content.data["content"]
        if n["type"] == "assetComponent"
    ]
    assert sorted(e["assetType"] for e in embeds) == ["dataset", "post"]
    children = {str(c.id): c for c in ouro.assets.children(str(parent.id))}
    assert {e["id"] for e in embeds} <= set(children)
    # Children take their audience from the post that embeds them.
    for child in children.values():
        assert child.visibility == "inherit"
    dataset_id = next(e["id"] for e in embeds if e["assetType"] == "dataset")
    assert len(ouro.datasets.list_rows(dataset_id, limit=10).data) == 2
