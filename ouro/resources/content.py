from copy import deepcopy

import pandas as pd

__all__ = ["Editor", "Content"]


DEFAULT_CONTENT_JSON = {
    "type": "doc",
    "content": [],
}


class Content:
    """A Post's content."""

    json: dict
    text: str

    def __init__(self, json: dict = None, text: str = ""):
        self.json = deepcopy(json) if json else deepcopy(DEFAULT_CONTENT_JSON)
        self.text = text

        # If we only have text, get the JSON representation
        if not json and text:
            self.from_text(text)

    def to_dict(self):
        return {"json": self.json, "text": self.text}

    def from_dict(self, data: dict):
        pass

    # TODO; this should do parsing of markdown and html
    def from_text(self, text: str):
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
        return {"json": self.json, "text": self.text}

    def from_markdown(self, markdown: str):
        """
        Convert markdown to a JSON representation of the content.
        Parses custom Ouro syntax for inline assets and user mentions.
        """
        conversion = self.client.post(
            "/utilities/convert/from-markdown", json={"markdown": markdown}
        ).json()

        self.json = conversion["json"]
        self.text = conversion["markdown"]

    def from_html(self, html: str):
        pass


class Editor(Content):
    """Class for creating and editing a Post's content.

    Inspired by https://github.com/didix21/mdutils
    """

    # def __init__(self):
    #     super().__init__()

    def new_header(self, level: int, text: str):
        assert 1 <= level <= 3, "Header level must be between 1 and 3"

        element = {
            "type": "heading",
            "attrs": {"level": level},
            "content": [{"text": text, "type": "text"}],
        }
        self.json["content"].append(element)
        self.text += f"{'#' * level} {text}\n"

    def new_paragraph(self, text: str):
        element = {
            "type": "paragraph",
            "content": [{"text": text, "type": "text"}],
        }
        self.json["content"].append(element)
        self.text += f"{text}\n"

    def new_line(self):
        element = {
            "type": "paragraph",
            "content": [{"text": "", "type": "text"}],
        }
        self.json["content"].append(element)
        self.text += "\n"

    def new_code_block(self, code: str, language: str = None):
        element = {
            "type": "codeBlock",
            "attrs": {"language": language},
            "content": [{"text": code, "type": "text"}],
        }
        self.json["content"].append(element)
        self.text += f"```{language}\n{code}\n```"

    def new_table(self, data: pd.DataFrame):
        element = {
            "type": "table",
            "content": [],
        }

        # Generate the header row
        header_row = {
            "type": "tableRow",
            "content": list(
                map(
                    (
                        lambda x: {
                            "type": "tableHeader",
                            "attrs": {"colspan": 1, "rowspan": 1, "colwidth": None},
                            "content": [
                                {
                                    "type": "paragraph",
                                    "content": [{"text": str(x), "type": "text"}],
                                }
                            ],
                        }
                    ),
                    data.columns,
                )
            ),
        }
        # Generate the rows
        rows = list(
            map(
                (
                    lambda x: {
                        "type": "tableRow",
                        "content": list(
                            map(
                                (
                                    lambda y: {
                                        "type": "tableCell",
                                        "attrs": {
                                            "colspan": 1,
                                            "rowspan": 1,
                                            "colwidth": None,
                                        },
                                        "content": [
                                            {
                                                "type": "paragraph",
                                                "content": [
                                                    {
                                                        "text": str(y),
                                                        "type": "text",
                                                    }
                                                ],
                                            }
                                        ],
                                    }
                                ),
                                x[1].values,
                            )
                        ),
                    }
                ),
                data.iterrows(),
            )
        )
        # Add the header row and rows to the table
        element["content"] = [header_row, *rows]

        self.json["content"].append(element)
        self.text += f"{data.to_markdown()}\n"

    def new_inline_image(self, src: str, alt: str):
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
        view_mode: str = "default",
    ):
        element = {
            # "type": "paragraph",
            # "content": [
            #     {
            "type": "assetComponent",
            "attrs": {
                "id": id,
                "assetType": asset_type,
                "filters": filters,
                "viewMode": view_mode,
            },
            #     }
            # ],
        }
        self.json["content"].append(element)
        self.text += f"{{asset:{id}}}"
