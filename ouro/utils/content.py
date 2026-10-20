"""Utilities for converting Tiptap JSON content to markdown."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Union

if TYPE_CHECKING:
    from ouro.models import RichText

__all__ = ["tiptap_to_markdown", "description_to_markdown"]

# Embed summaries cut descriptions here so a post with many embeds stays short.
EMBED_DESCRIPTION_LIMIT = 200

_EmbedIndex = Dict[str, Any]


def tiptap_to_markdown(
    doc: dict,
    embedded_assets: Optional[Iterable[Any]] = None,
) -> str:
    """Convert a Tiptap JSON document to markdown.

    Handles paragraphs, headings, code blocks, lists, blockquotes, tables,
    images, horizontal rules, mentions, and asset components.

    ``embedded_assets`` is the list the server resolved alongside the content
    (``Post.assets``, ``Comment.assets``, ``QuestItem.embedded_assets``). When
    given, each asset block gets a one-line summary under it: name, type,
    owner, team and a short description, or a note that the reader can't open
    it. Leave it out for markdown that will be written back, so the summaries
    never end up in the stored content.
    """
    if not doc or not isinstance(doc, dict):
        return ""

    content = doc.get("content")
    if not content or not isinstance(content, list):
        return ""

    embeds = _index_embeds(embedded_assets)
    parts = []
    for node in content:
        md = _render_block(node, embeds=embeds)
        if md is not None:
            parts.append(md)

    return "\n\n".join(parts).strip()


def description_to_markdown(
    description: Union["RichText", str, dict, None],
    max_length: Optional[int] = None,
    embedded_assets: Optional[Iterable[Any]] = None,
) -> str:
    """Convert an asset description (any shape) to markdown text.

    Handles all the forms description can take:
    - None → ""
    - RichText → converts its tiptap doc, falling back to its text
    - str → returned as-is
    - dict with "json" key → converts the tiptap doc
    - dict with "type": "doc" → treats as tiptap doc directly
    - dict with "text" key → falls back to the text field

    ``embedded_assets`` annotates asset blocks; see ``tiptap_to_markdown``.
    """
    from ouro.models import RichText

    if description is None:
        return ""
    if isinstance(description, RichText):
        text = tiptap_to_markdown(description.data, embedded_assets) or description.text
    elif isinstance(description, str):
        text = description
    elif isinstance(description, dict):
        if "json" in description and isinstance(description["json"], dict):
            text = tiptap_to_markdown(description["json"], embedded_assets)
        elif description.get("type") == "doc":
            text = tiptap_to_markdown(description, embedded_assets)
        elif "text" in description:
            text = str(description["text"])
        else:
            text = ""
    else:
        text = str(description)

    if max_length and len(text) > max_length:
        text = text[:max_length] + "..."

    return text


# ---------------------------------------------------------------------------
# Internal rendering helpers
# ---------------------------------------------------------------------------


def _render_inline(nodes: list) -> str:
    """Render a list of inline nodes (text, mention, hardBreak, etc.)."""
    if not nodes:
        return ""
    parts = []
    for node in nodes:
        node_type = node.get("type", "")
        if node_type == "text":
            text = node.get("text", "")
            parts.append(_apply_marks(text, node.get("marks", [])))
        elif node_type == "mention":
            attrs = node.get("attrs", {})
            label = attrs.get("label") or attrs.get("username") or attrs.get("id", "")
            parts.append(f"@{label}")
        elif node_type == "mathematics":
            attrs = node.get("attrs", {})
            latex = attrs.get("latex", "")
            display = str(attrs.get("displayMode")).lower() == "true"
            parts.append(f"\\[{latex}\\]" if display else f"\\({latex}\\)")
        elif node_type == "hardBreak":
            parts.append("\n")
        elif node_type == "image":
            attrs = node.get("attrs", {})
            alt = attrs.get("alt", "")
            src = attrs.get("src", "")
            parts.append(f"![{alt}]({src})")
    return "".join(parts)


def _apply_marks(text: str, marks: list) -> str:
    """Wrap text with markdown formatting based on Tiptap marks."""
    if not marks:
        return text
    for mark in marks:
        mark_type = mark.get("type", "")
        if mark_type == "bold":
            text = f"**{text}**"
        elif mark_type == "italic":
            text = f"*{text}*"
        elif mark_type == "strike":
            text = f"~~{text}~~"
        elif mark_type == "code":
            text = f"`{text}`"
        elif mark_type == "link":
            href = mark.get("attrs", {}).get("href", "")
            text = f"[{text}]({href})"
    return text


def _render_block(
    node: dict, indent: str = "", embeds: Optional[_EmbedIndex] = None
) -> Optional[str]:
    """Render a single block-level node to markdown."""
    if not node or not isinstance(node, dict):
        return None

    node_type = node.get("type", "")
    content = node.get("content", [])
    attrs = node.get("attrs", {})

    if node_type == "paragraph":
        return indent + _render_inline(content)

    if node_type == "heading":
        level = attrs.get("level", 1)
        return "#" * level + " " + _render_inline(content)

    if node_type == "codeBlock":
        lang = attrs.get("language") or ""
        code = _render_inline(content)
        return f"```{lang}\n{code}\n```"

    if node_type == "blockquote":
        inner = _render_children(content, indent="", embeds=embeds)
        lines = inner.split("\n")
        return "\n".join(f"> {line}" for line in lines)

    if node_type == "callout":
        kind = str(attrs.get("kind") or "note").upper()
        lines = [f"[!{kind}]", *_render_children(content, indent="", embeds=embeds).split("\n")]
        return "\n".join(f"> {line}" if line else ">" for line in lines)

    if node_type == "bulletList":
        items = []
        for child in content:
            if child.get("type") == "listItem":
                items.append(_render_list_item(child, prefix=f"{indent}- ", indent=indent + "  ", embeds=embeds))
        return "\n".join(items)

    if node_type == "orderedList":
        items = []
        start = attrs.get("start", 1)
        for i, child in enumerate(content):
            if child.get("type") == "listItem":
                num = start + i
                items.append(_render_list_item(
                        child, prefix=f"{indent}{num}. ", indent=indent + "   ", embeds=embeds
                    ))
        return "\n".join(items)

    if node_type == "table":
        return _render_table(content, embeds=embeds)

    if node_type == "image":
        alt = attrs.get("alt", "")
        src = attrs.get("src", "")
        return f"![{alt}]({src})"

    if node_type == "horizontalRule":
        return "---"

    if node_type == "assetComponent":
        import json as _json

        asset_type = attrs.get("assetType", "asset")
        asset_id = attrs.get("id", "")
        view_mode = attrs.get("viewMode", "card")
        display_config = attrs.get("displayConfig")
        block = {"id": asset_id, "assetType": asset_type, "viewMode": view_mode}
        if display_config:
            # The markdown parser keeps displayConfig as the JSON text it was
            # written as; emitting that string would quote it again each trip.
            if isinstance(display_config, str):
                try:
                    display_config = _json.loads(display_config)
                except ValueError:
                    pass
            block["displayConfig"] = display_config
        rendered = f"```assetComponent\n{_json.dumps(block)}\n```"
        if embeds is not None and not attrs.get("partial"):
            rendered += "\n" + _embed_summary(embeds.get(str(asset_id)), asset_type)
        return rendered

    if node_type == "hardBreak":
        return ""

    if content:
        return _render_children(content, indent, embeds=embeds)

    return None


def _render_children(
    content: list, indent: str = "", embeds: Optional[_EmbedIndex] = None
) -> str:
    """Render a list of block nodes, joining with double newlines."""
    parts = []
    for child in content:
        md = _render_block(child, indent, embeds=embeds)
        if md is not None:
            parts.append(md)
    return "\n\n".join(parts)


def _render_list_item(
    node: dict, prefix: str, indent: str, embeds: Optional[_EmbedIndex] = None
) -> str:
    """Render a listItem node with the given prefix (- or 1.)."""
    children = node.get("content", [])
    if not children:
        return prefix

    lines = []
    for i, child in enumerate(children):
        md = _render_block(child, indent="" if i == 0 else indent, embeds=embeds)
        if md is not None:
            if i == 0:
                lines.append(prefix + md)
            else:
                lines.append(md)
    return "\n".join(lines)


def _render_table(rows: list, embeds: Optional[_EmbedIndex] = None) -> str:
    """Render a Tiptap table as a markdown table."""
    if not rows:
        return ""

    md_rows: list[list[str]] = []
    header_row = True

    for row in rows:
        if row.get("type") != "tableRow":
            continue
        cells = []
        is_header = False
        for cell in row.get("content", []):
            cell_type = cell.get("type", "")
            if cell_type == "tableHeader":
                is_header = True
            cell_text = _render_children(cell.get("content", []), embeds=embeds)
            cell_text = cell_text.replace("\n", " ").strip()
            cells.append(cell_text)
        md_rows.append(cells)
        if is_header and header_row:
            md_rows.append(["-" * max(3, len(c)) for c in cells])
            header_row = False

    if not md_rows:
        return ""

    col_count = max(len(r) for r in md_rows)
    normalized = [r + [""] * (col_count - len(r)) for r in md_rows]
    return "\n".join("| " + " | ".join(row) + " |" for row in normalized)


# ---------------------------------------------------------------------------
# Embedded asset summaries
# ---------------------------------------------------------------------------


def _field(obj: Any, *path: str) -> Any:
    """Read a nested field from a server dict or an SDK model."""
    for key in path:
        if obj is None:
            return None
        obj = obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)
    return obj


def _index_embeds(embedded_assets: Optional[Iterable[Any]]) -> Optional[_EmbedIndex]:
    """Key the server's resolved assets by id; None means "don't annotate"."""
    if embedded_assets is None:
        return None
    index: _EmbedIndex = {}
    for asset in embedded_assets:
        asset_id = _field(asset, "id")
        if asset_id is not None:
            index[str(asset_id)] = asset
    return index


def _one_line(text: str, limit: int) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0] or text[:limit]
    return cut.rstrip(" ,.;:") + "…"


def _embed_summary(asset: Any, block_type: str) -> str:
    """One line saying what an embed is, written under its block."""
    if asset is None:
        # The server resolves embeds as the reader, so a missing one was
        # deleted or is hidden from them.
        return "↳ Not available to you (deleted, or you don't have access)."

    # "Name (type · ...)" like the discovery rows; no bold, so it still reads
    # when a list collapses it into an already-bold lead.
    parts = [_field(asset, "asset_type") or block_type]
    username = _field(asset, "user", "username")
    if username:
        parts.append(f"@{username}")
    team = _field(asset, "team", "name")
    if team:
        parts.append(f"#{team}")
    # Only say so when it's narrower than the content around it: "inherit"
    # (a post's own files) means "same as this post".
    visibility = _field(asset, "visibility")
    if visibility and visibility not in ("public", "inherit"):
        parts.append(str(visibility))

    name = " ".join(str(_field(asset, "name") or "Untitled").split())
    line = f"↳ {name} ({' · '.join(parts)})"
    description = _one_line(description_to_markdown(_field(asset, "description")), EMBED_DESCRIPTION_LIMIT)
    if description:
        line += f" — {description}"
    return line
