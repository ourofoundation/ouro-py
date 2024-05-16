


from supabase import Client
import time
import logging
import json
import os
import requests
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class Post:
    """Create a new Air post. Formats the data to be viewed with the AirViewer.

    Inspired by https://github.com/didix21/mdutils
    """

    def __init__(self, data: dict = {}, content: dict = {}):
        self.data = data        
        self.content = {
            "type": "doc",
            "content": [],
        }

    def new_header(self, level: int, title: str):
        element = {
            "type": "heading",
            "attrs": {"level": level},
            "content": [{"text": title, "type": "text"}],
        }
        self.content["content"].append(element)

    def new_paragraph(self, text: str):
        element = {
            "type": "paragraph",
            "content": [{"text": text, "type": "text"}],
        }
        self.content["content"].append(element)

    def new_line(self):
        element = {
            "type": "paragraph",
            "content": [{"text": "", "type": "text"}],
        }
        self.content["content"].append(element)

    def new_code_block(self, code: str, language: str = None):
        element = {
            "type": "codeBlock",
            "attrs": {"language": language},
            "content": [{"text": code, "type": "text"}],
        }
        self.content["content"].append(element)

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

        self.content["content"].append(element)

    def new_inline_image(self, src: str, alt: str):
        element = {
            "type": "image",
            "attrs": {"src": src, "alt": alt},
        }
        self.content["content"].append(element)

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
        self.content["content"].append(element)


class Air:
    def __init__(self, config):
        self.config = config
        self.Post = Post

    def create_post(self, post: Post):
        request = requests.post(f"{os.environ.get('OURO_BACKEND_URL')}/elements/air/create",
            headers={
                "Authorization": f"{self.config.token}",
                "Content-Type": "application/json",
            },
            json={
                "content": {"json": post.content, "text": ""},
                "post": post.data
            },
        )
        request.raise_for_status()

        return request.json()