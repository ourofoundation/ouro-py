from copy import deepcopy
from typing import TYPE_CHECKING, Optional

import pandas as pd

if TYPE_CHECKING:
    from ouro import Ouro

__all__ = ["Editor", "Content"]


DEFAULT_CONTENT_JSON = {
    "type": "doc",
    "content": [],
}


class Content:
    """A Post's content."""

    json: dict
    text: str
    _ouro: Optional["Ouro"]

    def __init__(
        self, json: dict = None, text: str = "", _ouro: Optional["Ouro"] = None
    ):
        self.json = deepcopy(json) if json else deepcopy(DEFAULT_CONTENT_JSON)
        self.text = text
        self._ouro = _ouro

        if not json and text:
            self.from_text(text)

    def to_dict(self) -> dict:
        return {"json": self.json, "text": self.text}

    def from_dict(self, data: dict) -> None:
        """Reconstruct Content from a dict with 'json' and/or 'text' keys."""
        self.json = data.get("json", deepcopy(DEFAULT_CONTENT_JSON))
        self.text = data.get("text", "")

    def from_text(self, text: str) -> None:
        """Convert plain text to content. Uses server-side markdown parsing
        when connected to an Ouro client, otherwise falls back to naive
        paragraph splitting.
        """
        if self._ouro:
            self.from_markdown(text)
            return

        self.text = text
        self.json = {
            "type": "doc",
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"text": line, "type": "text"}],
                }
                for line in text.split("\n")
            ],
        }

    def append(self, content: "Content") -> None:
        self.json["content"].extend(content.json["content"])
        self.text += "\n" + content.text

    def prepend(self, content: "Content") -> None:
        self.json["content"] = content.json["content"] + self.json["content"]
        self.text = content.text + "\n" + self.text

    def from_markdown(self, markdown: str) -> None:
        """Convert markdown to a JSON representation of the content.

        Parses custom Ouro syntax for inline assets and user mentions.
        Requires the Content object to be connected to an Ouro client.
        """
        if not self._ouro:
            raise RuntimeError(
                "Content object not connected to Ouro client. "
                "Pass _ouro to the constructor: Content(_ouro=ouro)"
            )
        response = self._ouro.client.post(
            "/utilities/convert/from-markdown", json={"markdown": markdown}
        )
        response.raise_for_status()
        conversion = response.json()

        self.json = conversion["json"]
        self.text = conversion["markdown"]

    def to_markdown(self) -> str:
        """Convert this content's JSON to markdown."""
        from ouro.utils.content import tiptap_to_markdown

        return tiptap_to_markdown(self.json)


class Editor(Content):
    """Class for creating and editing a Post's content.

    Inspired by https://github.com/didix21/mdutils
    """

    def new_header(self, level: int, text: str) -> None:
        if not 1 <= level <= 3:
            raise ValueError(f"Header level must be between 1 and 3, got {level}")

        element = {
            "type": "heading",
            "attrs": {"level": level},
            "content": [{"text": text, "type": "text"}],
        }
        self.json["content"].append(element)
        self.text += f"{'#' * level} {text}\n"

    def new_paragraph(self, text: str) -> None:
        element = {
            "type": "paragraph",
            "content": [{"text": text, "type": "text"}],
        }
        self.json["content"].append(element)
        self.text += f"{text}\n"

    def new_code_block(self, code: str, language: str = None) -> None:
        element = {
            "type": "codeBlock",
            "attrs": {"language": language},
            "content": [{"text": code, "type": "text"}],
        }
        self.json["content"].append(element)
        self.text += f"```{language}\n{code}\n```"

    def new_table(self, data: pd.DataFrame) -> None:
        element = {
            "type": "table",
            "content": [],
        }

        header_row = {
            "type": "tableRow",
            "content": [
                {
                    "type": "tableHeader",
                    "attrs": {"colspan": 1, "rowspan": 1, "colwidth": None},
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"text": str(col), "type": "text"}],
                        }
                    ],
                }
                for col in data.columns
            ],
        }

        rows = [
            {
                "type": "tableRow",
                "content": [
                    {
                        "type": "tableCell",
                        "attrs": {"colspan": 1, "rowspan": 1, "colwidth": None},
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [{"text": str(val), "type": "text"}],
                            }
                        ],
                    }
                    for val in row.values
                ],
            }
            for _, row in data.iterrows()
        ]

        element["content"] = [header_row, *rows]

        self.json["content"].append(element)
        self.text += f"{data.to_markdown()}\n"

    def new_inline_image(self, src: str, alt: str) -> None:
        element = {
            "type": "image",
            "attrs": {"src": src, "alt": alt},
        }
        self.json["content"].append(element)
        self.text += f"![{alt}]({src})"

    def new_inline_asset(
        self,
        id: str,
        asset_type: str,
        filters: dict = None,
        view_mode: str = "card",
    ) -> None:
        element = {
            "type": "assetComponent",
            "attrs": {
                "id": id,
                "assetType": asset_type,
                "filters": filters,
                "viewMode": view_mode,
            },
        }
        self.json["content"].append(element)
        self.text += f"{{asset:{id}}}"

    def new_partial_asset(
        self,
        partial_data: dict,
        *,
        id: Optional[str] = None,
        view_mode: str = "card",
        filters: Optional[dict] = None,
    ) -> None:
        """Embed a not-yet-uploaded file as an inline asset.

        ``partial_data`` is a dict returned by
        :meth:`~ouro.resources.files.Files.partial_from_bytes` or
        :meth:`~ouro.resources.files.Files.partial_from_file`.
        The backend materialises the file when the post is saved.

        >>> partial = ouro.files.partial_from_file("/tmp/report.html", name="Report")
        >>> editor.new_partial_asset(partial, view_mode="preview")
        """
        from ouro.utils import generate_uuid

        node_id = id or generate_uuid()
        asset_type = partial_data.get("asset_type", "file")
        element = {
            "type": "assetComponent",
            "attrs": {
                "id": node_id,
                "assetType": asset_type,
                "partial": True,
                "partialData": partial_data,
                "filters": filters or {},
                "viewMode": view_mode,
            },
        }
        self.json["content"].append(element)
        self.text += f"{{asset:{node_id}}}"
