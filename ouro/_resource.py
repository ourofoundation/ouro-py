from __future__ import annotations

import httpx
from ouro.realtime.websocket import OuroWebSocket
from supabase import Client

# from ouro import Ouro


class SyncAPIResource:
    client: httpx.Client
    supabase: Client
    websocket: OuroWebSocket

    def __init__(self, ouro) -> None:
        self.client = ouro.client
        self.websocket = ouro.websocket
        self.supabase = ouro.supabase
        self.ouro = ouro
