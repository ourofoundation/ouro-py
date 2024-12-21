import logging
import time
from typing import Callable, Optional

import socketio

log = logging.getLogger(__name__)


class OuroWebSocket:
    def __init__(self, ouro):
        self.ouro = ouro
        self.sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=5,
            reconnection_delay=1,
        )
        self.setup_event_handlers()

    @property
    def is_connected(self):
        if not self.sio.connected:
            return False
        return True

    def setup_event_handlers(self):
        @self.sio.event
        def connect():
            log.info("Connected to websocket")

        @self.sio.event
        def disconnect():
            log.warning("Disconnected from websocket")

        @self.sio.event
        def connect_error(data):
            log.error(f"Connection error: {data}")

    def connect(self, access_token: Optional[str] = None) -> None:
        try:
            self.sio.connect(
                self.ouro.websocket_url,
                retry=True,
                namespaces=["/"],
                auth={
                    "access_token": access_token or self.ouro.access_token,
                    # "refresh_token": access_token or self.ouro.refresh_token,
                },
            )
            self.sio.sleep(1)
        except Exception as e:
            log.error(f"Failed to connect to websocket: {e}")

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
            print("Attempting to reconnect", self.sio.connected, self.is_connected)
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

    def on(self, event: str, handler: Callable):
        self.sio.on(event, handler)

    def emit(self, event, data):
        if not self.is_connected:
            log.warning("Attempted to emit event while disconnected. Reconnecting...")
            self.connect()
        return self.sio.emit(event, data)

    def __del__(self):
        if self.is_connected:
            self.disconnect()
