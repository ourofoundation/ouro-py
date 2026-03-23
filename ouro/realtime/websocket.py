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
    ):
        return self.emit(
            "llm-response",
            {
                "user_id": user_id or str(self.ouro.user.id),
                "recipient_id": recipient_id,
                "conversation_id": conversation_id,
                "data": {
                    "content": content,
                    "id": message_id,
                    "user_id": user_id or str(self.ouro.user.id),
                },
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
