import asyncio
import logging
from typing import Callable, List, Optional

from ouro._resource import SyncAPIResource
from ouro.models import Conversation
from ouro.realtime.websocket import OuroWebSocket

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Conversations", "Messages"]


class Messages(SyncAPIResource):
    def create(self, conversation_id: str, **kwargs):
        json = kwargs.get("json")
        text = kwargs.get("text")
        user_id = kwargs.get("user_id")
        message = {
            "json": json,
            "text": text,
            "user_id": user_id,
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
        self._ouro = conversation._ouro

    def create(self, **kwargs):
        return Messages(self._ouro).create(self.conversation.id, **kwargs)

    def retrieve(self, message_id: str):
        return Messages(self._ouro).retrieve(message_id)

    def update(self, message_id: str, content: Optional[Content] = None, **kwargs):
        return Messages(self._ouro).update(message_id, content, **kwargs)

    def delete(self, message_id: str):
        return Messages(self._ouro).delete(message_id)

    def list(self, **kwargs):
        return Messages(self._ouro).list(self.conversation.id, **kwargs)


class Conversations(SyncAPIResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.messages = Messages(*args, **kwargs)

    def retrieve(self, conversation_id: str):
        """
        Retrieve a conversation by id.
        """

        request = self.client.get(f"/conversations/{conversation_id}")
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        conversation = response["data"]
        return Conversation(**conversation, _ouro=self.ouro)

    def list(self, **kwargs):
        """
        List all conversations.
        """
        request = self.client.get("/conversations", params=kwargs)
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return [
            Conversation(**conversation, _ouro=self.ouro)
            for conversation in response["data"]
        ]

    def update(self, conversation_id: str, **kwargs):
        """
        Update a conversation.
        """
        request = self.client.put(
            f"/conversations/{conversation_id}", json={"conversation": kwargs}
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        return Conversation(**response["data"], _ouro=self.ouro)
