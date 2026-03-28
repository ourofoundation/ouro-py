import logging
import time
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

import socketio

log = logging.getLogger(__name__)


class OuroWebSocket:
    def __init__(self, ouro):
        self.ouro = ouro
        self._last_connect_error = None
        self.sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=5,
            reconnection_delay=1,
        )
        self.setup_event_handlers()

    @property
    def is_connected(self) -> bool:
        return self.sio.connected

    def setup_event_handlers(self):
        @self.sio.event
        def connect():
            self._last_connect_error = None
            log.info("Connected to websocket")

        @self.sio.event
        def disconnect():
            log.warning("Disconnected from websocket")

        @self.sio.event
        def connect_error(data):
            self._last_connect_error = data
            log.error(f"Connection error: {data}")

    def connect(self, access_token: Optional[str] = None) -> None:
        try:
            self.sio.connect(
                self.ouro.websocket_url,
                retry=True,
                namespaces=["/"],
                auth={
                    "access_token": access_token or self.ouro.access_token,
                },
            )
            self.sio.sleep(1)
            if not self.is_connected:
                raise RuntimeError(
                    f"Websocket connection failed: {self._last_connect_error or 'unknown error'}"
                )
        except Exception as e:
            log.error(f"Failed to connect to websocket: {e}")
            raise

    def disconnect(self):
        self.sio.disconnect()

    def refresh_connection(self, access_token: Optional[str] = None):
        self.disconnect()
        self.sio.sleep(1)
        self.connect(access_token)

    def handle_disconnect(self):
        max_retries = 5
        retry_delay = 1  # Start with 1 second delay

        for attempt in range(max_retries):
            log.info(f"Attempting to reconnect (attempt {attempt + 1}/{max_retries})")
            try:
                self.connect()
                if self.is_connected:
                    log.info("Reconnection successful")
                    return
            except Exception as e:
                log.error(f"Reconnection attempt failed: {e}")

            # Exponential backoff
            retry_delay *= 2
            log.info(f"Waiting {retry_delay} seconds before next attempt")
            time.sleep(retry_delay)

        log.error("Failed to reconnect after maximum attempts")

    @contextmanager
    def session(self) -> Iterator[None]:
        """Connect for the duration of a block, then disconnect.

        Safe to call when already connected -- in that case the existing
        connection is reused and left open when the block exits.
        """
        should_disconnect = not self.is_connected
        if should_disconnect:
            self.connect()
        try:
            yield
        finally:
            if should_disconnect and self.is_connected:
                # Allow pending emits to flush before tearing down the connection
                self.sio.sleep(0.5)
                self.disconnect()

    def on(self, event: str, handler: Callable):
        self.sio.on(event, handler)

    def emit(self, event, data):
        if not self.is_connected:
            log.warning("Attempted to emit event while disconnected. Reconnecting...")
            self.connect()
        if not self.is_connected:
            raise RuntimeError("Cannot emit websocket event while disconnected")
        return self.sio.emit(event, data)

    def emit_activity(
        self,
        *,
        recipient_id: str,
        conversation_id: str,
        status: str,
        active: bool,
        message: Optional[str] = None,
        user_id: Optional[str] = None,
    ):
        payload = {
            "user_id": user_id or str(self.ouro.user.id),
            "recipient_id": recipient_id,
            "conversation_id": conversation_id,
            "data": {
                "status": status,
                "active": active,
            },
        }
        if message:
            payload["data"]["message"] = message
        return self.emit("activity", payload)

    def emit_llm_response(
        self,
        *,
        recipient_id: str,
        conversation_id: str,
        content: str,
        message_id: str,
        user_id: Optional[str] = None,
        event_type: Optional[str] = None,
    ):
        data: dict = {
            "content": content,
            "id": message_id,
            "user_id": user_id or str(self.ouro.user.id),
        }
        if event_type:
            data["type"] = event_type
        return self.emit(
            "llm-response",
            {
                "user_id": user_id or str(self.ouro.user.id),
                "recipient_id": recipient_id,
                "conversation_id": conversation_id,
                "data": data,
            },
        )

    def emit_reasoning(
        self,
        *,
        recipient_id: str,
        conversation_id: str,
        content: str,
        message_id: str,
        user_id: Optional[str] = None,
    ):
        return self.emit_llm_response(
            recipient_id=recipient_id,
            conversation_id=conversation_id,
            content=content,
            message_id=message_id,
            user_id=user_id,
            event_type="reasoning",
        )

    def emit_tool_start(
        self,
        *,
        recipient_id: str,
        conversation_id: str,
        message_id: str,
        tool_name: str,
        tool_call_id: str,
        input_data: Optional[dict] = None,
        user_id: Optional[str] = None,
    ):
        data: dict = {
            "content": tool_name,
            "id": message_id,
            "user_id": user_id or str(self.ouro.user.id),
            "type": "tool-start",
            "toolName": tool_name,
            "toolCallId": tool_call_id,
        }
        if input_data is not None:
            data["input"] = input_data
        return self.emit(
            "llm-response",
            {
                "user_id": user_id or str(self.ouro.user.id),
                "recipient_id": recipient_id,
                "conversation_id": conversation_id,
                "data": data,
            },
        )

    def emit_tool_result(
        self,
        *,
        recipient_id: str,
        conversation_id: str,
        message_id: str,
        tool_call_id: str,
        output_data: Optional[dict] = None,
        user_id: Optional[str] = None,
    ):
        data: dict = {
            "content": "",
            "id": message_id,
            "user_id": user_id or str(self.ouro.user.id),
            "type": "tool-result",
            "toolCallId": tool_call_id,
        }
        if output_data is not None:
            data["output"] = output_data
        return self.emit(
            "llm-response",
            {
                "user_id": user_id or str(self.ouro.user.id),
                "recipient_id": recipient_id,
                "conversation_id": conversation_id,
                "data": data,
            },
        )

    def emit_llm_response_end(
        self,
        *,
        recipient_id: str,
        conversation_id: str,
        message_id: str,
        user_id: Optional[str] = None,
        message: Optional[dict] = None,
    ):
        return self.emit(
            "llm-response-end",
            {
                "user_id": user_id or str(self.ouro.user.id),
                "recipient_id": recipient_id,
                "conversation_id": conversation_id,
                # Same id as llm-response chunks; use to clear streaming UI when data.id is the persisted row uuid.
                "stream_message_id": message_id,
                # When message is None, clients only receive id + user_id; UIs should merge or wait for DB Realtime for full row.
                "data": (
                    message
                    if message is not None
                    else {
                        "id": message_id,
                        "user_id": user_id or str(self.ouro.user.id),
                    }
                ),
            },
        )

    def __del__(self):
        if self.is_connected:
            self.disconnect()
