import logging
import os
import time

import requests
from dotenv import load_dotenv
from supabase.client import ClientOptions

from supabase import Client, create_client
from ouro.air import MakeAirPost
from ouro.earth import Earth

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

postgres_logger = logging.getLogger("httpx")
postgres_logger.setLevel(logging.WARNING)


load_dotenv()


class Ouro:
    def __init__(self):
        self.client = None
        self.public_client = None
        self.user = None

        self.earth = None

        # Class Instances
        self.MakeAirPost = MakeAirPost

    def login(self, api_key: str):
        url: str = os.environ.get("SUPABASE_URL")
        key: str = os.environ.get("SUPABASE_ANON_KEY")

        if not api_key:
            raise Exception("No API key found")

        # Send a request to Ouro Backend to get an access token
        req = requests.post(
            "http://localhost:8003/users/get-token",
            json={"pat": api_key},
        )
        json = req.json()
        token = json["token"]
        self.client: Client = create_client(
            url,
            key,
            options=ClientOptions(
                schema="datasets",
                auto_refresh_token=True,
                persist_session=False,
            ),
        )
        self.public_client: Client = create_client(
            url,
            key,
            options=ClientOptions(
                auto_refresh_token=True,
                persist_session=False,
            ),
        )

        if not token:
            raise Exception("No user found for this API key")

        self.client.postgrest.auth(token)
        self.public_client.postgrest.auth(token)

        self.user = self.client.auth.get_user(token).user
        print(f"Successfully logged in as {self.user.email}.")

        # Instanciate classes
        self.earth = Earth(self.client, self.public_client)

    def get_dataset(self, name: str):
        res = (
            self.public_client.table("datasets")
            .select("*")
            .eq("name", name)
            .single()
            .execute()
        )
        return res.data

    def load_dataset(self, table_name: str, schema: str = "datasets"):
        start = time.time()

        row_count = self.client.table(table_name).select("*", count="exact").execute()
        row_count = row_count.count

        logger.info(f"Loading {row_count} rows from {schema}.{table_name}...")
        # Batch load the data if it's too big
        if row_count > 1_000_000:
            data = []
            for i in range(0, row_count, 1_000_000):
                logger.debug(f"Loading rows {i} to {i+1_000_000}")
                res = (
                    self.client.table(table_name)
                    .select("*")
                    .range(i, i + 1_000_000)
                    .execute()
                )
                data.extend(res.data)
        else:
            res = self.client.table(table_name).select("*").limit(1_000_000).execute()
            data = res.data

        end = time.time()
        logger.info(f"Finished loading data in {round(end - start, 2)} seconds.")

        self.data = data
        return data
