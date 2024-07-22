from __future__ import annotations

import httpx
from supabase import Client

# from ouro import Ouro


class SyncAPIResource:
    client: httpx.Client
    database: Client
    supabase: Client

    def __init__(self, ouro) -> None:
        self.client = ouro.client
        self.database = ouro.database
        self.supabase = ouro.supabase

        self.ouro = ouro

        # self._get = ouro.client.get
        # self._post = ouro.client.post
        # self._patch = ouro.client.patch
        # self._put = ouro.client.put
        # self._delete = ouro.client.delete
