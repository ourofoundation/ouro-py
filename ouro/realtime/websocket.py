import asyncio
import logging
from typing import Any, Callable

import socketio

log = logging.getLogger(__name__)


class OuroWebSocket:
    def __init__(self, ouro):
        self.ouro = ouro
        self.sio = socketio.AsyncClient()
        self.websocket_connected = asyncio.Event()
        self.connection_lock = asyncio.Lock()

    @property
    def is_connected(self):
        return self.websocket_connected.is_set()

    async def connect(self, url: str):
        async with self.connection_lock:
            if not self.is_connected:
                try:
                    await self.sio.connect(
                        url, auth={"access_token": self.ouro.access_token}
                    )
                    self.websocket_connected.set()

                    # Add some default event listeners
                    @self.sio.on("connect")
                    def connect_handler():
                        log.info("Connected to websocket")

                    @self.sio.on("disconnect")
                    def disconnect_handler():
                        log.info("Disconnected from websocket")
                        asyncio.create_task(self.handle_disconnect())

                except Exception as e:
                    log.error(f"Failed to connect to websocket: {e}")
                    raise

    async def handle_disconnect(self):
        self.websocket_connected.clear()
        # Implement reconnection logic here if needed

    async def disconnect(self):
        if self.is_connected:
            await self.sio.disconnect()
            self.websocket_connected.clear()

    def on(self, event: str, handler: Callable):
        self.sio.on(event, handler)

    async def emit(self, event: str, data: Any):
        if not self.is_connected:
            raise RuntimeError("Cannot emit event: WebSocket is not connected")
        await self.sio.emit(event, data)
