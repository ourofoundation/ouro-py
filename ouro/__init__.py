import os

import requests
from dotenv import load_dotenv
from supabase.client import ClientOptions

from supabase import Client, create_client

load_dotenv()


class Ouro:
    def __init__(self):
        self.client = None

    def login(self, api_key: str):
        url: str = os.environ.get("SUPABASE_URL")
        key: str = os.environ.get("SUPABASE_ANON_KEY")

        if not api_key:
            raise Exception("No API key found")

        req = requests.post(
            "http://localhost:3000/api/admin/get-token-from-pat",
            json={"pat": api_key},
        )
        token = req.json()["token"]
        self.client: Client = create_client(
            url,
            key,
            options=ClientOptions(
                schema="datasets",
                auto_refresh_token=False,
                persist_session=False,
            ),
        )

        if not token:
            raise Exception("No user found for this API key")

        self.client.postgrest.auth(token)

        user = self.client.auth.get_user(token).user
        print(f"Successfully logged in as {user.email}.")
