"""The editor rejects empty text nodes, so the Editor must not write them."""

from __future__ import annotations

import unittest

import pandas as pd

from ouro.resources.content import Editor


def _text_nodes(node: dict) -> list[dict]:
    found = [node] if node.get("type") == "text" else []
    for child in node.get("content", []):
        found.extend(_text_nodes(child))
    return found


class EditorEmptyTextTests(unittest.TestCase):
    def test_blank_table_cell_has_no_text_node(self) -> None:
        editor = Editor()
        editor.new_table(pd.DataFrame([{"Formula": "CoO", "Stable": ""}]))

        cell = editor.json["content"][0]["content"][1]["content"][1]
        self.assertEqual(cell["content"], [{"type": "paragraph", "content": []}])

    def test_no_builder_writes_empty_text(self) -> None:
        editor = Editor()
        editor.new_paragraph("")
        editor.new_header(2, "")
        editor.new_code_block("")
        editor.from_text("first\n\nthird")

        self.assertTrue(all(node["text"] for node in _text_nodes(editor.json)))


if __name__ == "__main__":
    unittest.main()
