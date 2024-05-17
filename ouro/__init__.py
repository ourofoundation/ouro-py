VERSION = "0.0.3"


import logging
import os


import requests
from dotenv import load_dotenv
from supabase.client import ClientOptions

from supabase import Client, create_client
from ouro.air import Air
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
        self.token = None

        self.earth = None
        self.water = None
        self.air = None
        self.fire = None

        # Class Instances
        # self.MakeAirPost = MakeAirPost

    def login(self, api_key: str):
        url: str = os.environ.get("SUPABASE_URL")
        key: str = os.environ.get("SUPABASE_ANON_KEY")

        if not api_key:
            raise Exception("No API key found")

        # Send a request to Ouro Backend to get an access token
        req = requests.post(
            f"{os.environ.get('OURO_BACKEND_URL')}/users/get-token",
            json={"pat": api_key},
        )
        json = req.json()
        self.token = json["token"]
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

        if not self.token:
            raise Exception("No user found for this API key")

        self.client.postgrest.auth(self.token)
        self.public_client.postgrest.auth(self.token)

        self.user = self.client.auth.get_user(self.token).user
        print(f"Successfully logged in as {self.user.email}.")

        # Instanciate classes
        self.earth = Earth(config=self)
        self.air = Air(config=self)
