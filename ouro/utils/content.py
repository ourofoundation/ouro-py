"""Utilities for converting Tiptap JSON content to markdown."""

from __future__ import annotations

from typing import Optional, Union

__all__ = ["tiptap_to_markdown", "description_to_markdown"]


def tiptap_to_markdown(doc: dict) -> str:
    """Convert a Tiptap JSON document to markdown.

    Handles paragraphs, headings, code blocks, lists, blockquotes, tables,
    images, horizontal rules, mentions, and asset components.
    """
    if not doc or not isinstance(doc, dict):
        return ""

    content = doc.get("content")
    if not content or not isinstance(content, list):
        return ""

    parts = []
    for node in content:
        md = _render_block(node)
        if md is not None:
            parts.append(md)

    return "\n\n".join(parts).strip()


def description_to_markdown(
    description: Union[str, dict, None],
    max_length: Optional[int] = None,
) -> str:
    """Convert an asset description (any shape) to markdown text.

    Handles all the forms description can take:
    - None → ""
    - str → returned as-is
    - dict with "json" key → converts the tiptap doc
    - dict with "type": "doc" → treats as tiptap doc directly
    - dict with "text" key → falls back to the text field
    """
    if description is None:
        return ""
    if isinstance(description, str):
        text = description
    elif isinstance(description, dict):
        if "json" in description and isinstance(description["json"], dict):
            text = tiptap_to_markdown(description["json"])
        elif description.get("type") == "doc":
            text = tiptap_to_markdown(description)
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


def _render_block(node: dict, indent: str = "") -> Optional[str]:
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
        inner = _render_children(content, indent="")
        lines = inner.split("\n")
        return "\n".join(f"> {line}" for line in lines)

    if node_type == "bulletList":
        items = []
        for child in content:
            if child.get("type") == "listItem":
                items.append(_render_list_item(child, prefix=f"{indent}- ", indent=indent + "  "))
        return "\n".join(items)

    if node_type == "orderedList":
        items = []
        start = attrs.get("start", 1)
        for i, child in enumerate(content):
            if child.get("type") == "listItem":
                num = start + i
                items.append(_render_list_item(child, prefix=f"{indent}{num}. ", indent=indent + "   "))
        return "\n".join(items)

    if node_type == "table":
        return _render_table(content)

    if node_type == "image":
        alt = attrs.get("alt", "")
        src = attrs.get("src", "")
        return f"![{alt}]({src})"

    if node_type == "horizontalRule":
        return "---"

    if node_type == "assetComponent":
        asset_type = attrs.get("assetType", "asset")
        asset_id = attrs.get("id", "")
        return f"[Embedded {asset_type}: {asset_id}]"

    if node_type == "hardBreak":
        return ""

    if content:
        return _render_children(content, indent)

    return None


def _render_children(content: list, indent: str = "") -> str:
    """Render a list of block nodes, joining with double newlines."""
    parts = []
    for child in content:
        md = _render_block(child, indent)
        if md is not None:
            parts.append(md)
    return "\n\n".join(parts)


def _render_list_item(node: dict, prefix: str, indent: str) -> str:
    """Render a listItem node with the given prefix (- or 1.)."""
    children = node.get("content", [])
    if not children:
        return prefix

    lines = []
    for i, child in enumerate(children):
        md = _render_block(child, indent="" if i == 0 else indent)
        if md is not None:
            if i == 0:
                lines.append(prefix + md)
            else:
                lines.append(md)
    return "\n".join(lines)


def _render_table(rows: list) -> str:
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
            cell_text = _render_children(cell.get("content", []))
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
