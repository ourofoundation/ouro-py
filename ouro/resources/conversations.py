import logging
from typing import List, Optional

from ouro._resource import SyncAPIResource
from ouro.models import Conversation

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Conversations", "Messages"]


class Messages(SyncAPIResource):
    def create(self, conversation_id: str, **kwargs):
        json = kwargs.get("json")
        text = kwargs.get("text")

        message = {
            "json": json,
            "text": text,
            **kwargs,
        }

        message = {k: v for k, v in message.items() if v is not None}
        request = self.client.post(
            f"/conversations/{conversation_id}/messages/create",
            json={"message": message},
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]

    # def retrieve(self, id: str):
    #     request = self.client.get(f"/messages/{id}")
    #     request.raise_for_status()
    #     response = request.json()
    #     if response["error"]:
    #         raise Exception(response["error"])
    #     return response["data"]

    # def update(self, id: str, content: Optional[Content] = None, **kwargs):
    #     message = {**kwargs}
    #     message = {k: v for k, v in message.items() if v is not None}
    #     request = self.client.put(
    #         f"/messages/{id}",
    #         json={"message": message, "content": content.to_dict() if content else None},
    #     )
    #     request.raise_for_status()
    #     response = request.json()
    #     if response["error"]:
    #         raise Exception(response["error"])
    #     return response["data"]

    # def delete(self, id: str):
    #     request = self.client.delete(f"/messages/{id}")
    #     request.raise_for_status()
    #     response = request.json()
    #     if response["error"]:
    #         raise Exception(response["error"])
    #     return response["data"]

    def list(self, conversation_id: str, **kwargs):
        request = self.client.get(
            f"/conversations/{conversation_id}/messages", params=kwargs
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]


class ConversationMessages:
    def __init__(self, conversation: "Conversation"):
        self.conversation = conversation
        self.ouro = conversation._ouro

    def create(self, **kwargs):
        return Messages(self.ouro).create(self.conversation.id, **kwargs)

    def retrieve(self, message_id: str):
        return Messages(self.ouro).retrieve(message_id)

    def update(self, message_id: str, content: Optional[Content] = None, **kwargs):
        return Messages(self.ouro).update(message_id, content, **kwargs)

    def delete(self, message_id: str):
        return Messages(self.ouro).delete(message_id)

    def list(self, **kwargs):
        return Messages(self.ouro).list(self.conversation.id, **kwargs)


class Conversations(SyncAPIResource):
    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self.messages = Messages(*args, **kwargs)

    def create(
        self,
        **kwargs,
    ) -> Conversation:
        """
        Create a new Conversation
        """

        conversation = {
            **kwargs,
        }
        # Filter out None values
        conversation = {k: v for k, v in conversation.items() if v is not None}

        request = self.client.post(
            "/conversations/create",
            json={
                **conversation,
            },
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        return Conversation(**response["data"], _ouro=self.ouro)

    def retrieve(self, id: str) -> Conversation:
        """
        Retrieve a Conversation by its id
        """
        request = self.client.get(
            f"/conversations/{id}",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        return Conversation(**response["data"], _ouro=self.ouro)

    # def update(
    #     self,
    #     id: str,
    #     name: Optional[str] = None,
    #     description: Optional[str] = None,
    #     visibility: Optional[str] = None,
    #     **kwargs,
    # ) -> Conversation:
    #     """
    #     Update a Conversation by its id
    #     """
    #     conversation = {
    #         "name": name,
    #         "description": description,
    #         "visibility": visibility,
    #         **kwargs,
    #     }
    #     conversation = {k: v for k, v in conversation.items() if v is not None}

    #     request = self.client.put(
    #         f"/conversations/{id}",
    #         json=conversation,
    #     )
    #     request.raise_for_status()
    #     response = request.json()
    #     if response["error"]:
    #         raise Exception(response["error"])
    #     return Conversation(**response["data"])

    # def delete(self, id: str):
    #     """
    #     Delete a Conversation by its id
    #     """
    #     request = self.client.delete(
    #         f"/conversations/{id}",
    #     )
    #     request.raise_for_status()
    #     response = request.json()
    #     if response["error"]:
    #         raise Exception(response["error"])

    #     return response["data"]
