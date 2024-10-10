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

    @property
    def is_connected(self):
        return self.websocket_connected.is_set()

    async def connect(self, url: str):
        if not self.is_connected:
            await self.sio.connect(url, auth={"access_token": self.ouro.access_token})
            self.websocket_connected.set()

            # Add some default event listeners
            @self.sio.on("connect")
            def connect_handler():
                log.info("Connected to websocket")

            @self.sio.on("disconnect")
            def disconnect_handler():
                log.info("Disconnected from websocket")

    async def disconnect(self):
        if self.is_connected:
            await self.sio.disconnect()
            self.websocket_connected.clear()

    def on(self, event: str, handler: Callable):
        self.sio.on(event, handler)

    async def emit(self, event: str, data: Any):
        await self.sio.emit(event, data)
