import json
import pandas as pd


class MakeAirPost:
    """Create a new Air post. Formats the data to be viewed with the AirViewer."""

    def __init__(self):
        self.data = {
            "type": "doc",
            "content": [],
        }

    def new_header(self, level: int, title: str):
        element = {
            "type": "heading",
            "attrs": {"level": level},
            "content": [{"text": title, "type": "text"}],
        }
        self.data["content"].append(element)

    def new_paragraph(self, text: str):
        element = {
            "type": "paragraph",
            "content": [{"text": text, "type": "text"}],
        }
        self.data["content"].append(element)

    def new_line(self):
        element = {
            "type": "paragraph",
            "content": [{"text": "", "type": "text"}],
        }
        self.data["content"].append(element)

    def new_code_block(self, code: str, language: str = None):
        element = {
            "type": "codeBlock",
            "attrs": {"language": language},
            "content": [{"text": code, "type": "text"}],
        }
        self.data["content"].append(element)

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

        self.data["content"].append(element)

    def new_inline_image(self, src: str, alt: str):
        element = {
            "type": "image",
            "attrs": {"src": src, "alt": alt},
        }
        self.data["content"].append(element)

    def create_post(self):
        # with open("sample.json", "w") as outfile:
        #     outfile.write(json.dumps(self.data, indent=2))
        return self.data
