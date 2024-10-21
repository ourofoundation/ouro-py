import logging
from typing import Any, Callable

import socketio

log = logging.getLogger(__name__)


class OuroWebSocket:
    def __init__(self, ouro):
        self.ouro = ouro
        self.sio = socketio.Client()

    @property
    def is_connected(self):
        return self.sio.connected

    def connect(self):
        try:
            self.sio.connect(
                self.ouro.websocket_url, auth={"access_token": self.ouro.access_token}
            )
            log.info("Connected to websocket")
        except Exception as e:
            log.error(f"Failed to connect to websocket: {e}")

    def disconnect(self):
        self.sio.disconnect()

    def refresh_connection(self):
        self.disconnect()
        self.connect()

    def handle_disconnect(self):
        # Implement reconnection logic here if needed
        pass

    def on(self, event: str, handler: Callable):
        self.sio.on(event, handler)

    def emit(self, event, data):
        return self.sio.emit(event, data)

    def __del__(self):
        if self.is_connected:
            self.disconnect()
