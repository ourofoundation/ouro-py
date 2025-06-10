import logging
from typing import List, Optional

from ouro._resource import SyncAPIResource
from ouro.models import Comment

from .content import Content, Editor

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Comments"]


class Comments(SyncAPIResource):
    def __init__(self, client):
        super().__init__(client)

    @staticmethod
    def Editor(**kwargs) -> Editor:
        return Editor(**kwargs)

    @staticmethod
    def Content(**kwargs) -> Content:
        return Content(**kwargs)

    def create(
        self,
        content: Content,
        parent_id: str,
        **kwargs,
    ) -> Comment:
        """
        Create a new Comment
        """
        comment = {
            "parent_id": parent_id,
            **kwargs,
        }
        # Filter out None values
        comment = {k: v for k, v in comment.items() if v is not None}

        request = self.client.post(
            "/comments/create",
            json={
                "comment": comment,
                "content": content.to_dict(),
            },
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Comment(**response["data"])

    def retrieve(self, id: str) -> Comment:
        """
        Retrieve a Comment by its id
        """
        request = self.client.get(
            f"/comments/{id}",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        return Comment(**response["data"])

    def list_by_parent(self, parent_id: str) -> List[Comment]:
        """
        List all comments for a parent post
        """
        request = self.client.get(
            f"/posts/{parent_id}/comments",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        return [Comment(**comment) for comment in response["data"]]

    def update(
        self,
        id: str,
        content: Optional[Content] = None,
        **kwargs,
    ) -> Comment:
        """
        Update a Comment by its id
        """
        comment = {**kwargs}
        # Filter out None values
        comment = {k: v for k, v in comment.items() if v is not None}

        request = self.client.put(
            f"/comments/{id}",
            json={
                "comment": comment,
                "content": content.to_dict() if content is not None else None,
            },
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Comment(**response["data"])
