import logging
import os
import time

import requests
from dotenv import load_dotenv
from supabase.client import ClientOptions

from supabase import Client, create_client


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
