from __future__ import annotations

import logging
import os

import httpx
import supabase
from ouro.config import Config
from ouro.realtime.websocket import OuroWebSocket
from ouro.resources import (
    Assets,
    Comments,
    Conversations,
    Datasets,
    Files,
    Posts,
    Routes,
    Services,
    Users,
)
from supabase.client import ClientOptions
from typing_extensions import override

from .__version__ import __version__
from ._exceptions import (
    APIStatusError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    InternalServerError,
    NotFoundError,
    OuroError,
    PermissionDeniedError,
    RateLimitError,
    UnprocessableEntityError,
)
from ._utils import is_mapping  # is_given,

__all__ = ["Ouro"]


log: logging.Logger = logging.getLogger("ouro")

# Get httpx log level set to debug
logging.getLogger("httpx").setLevel(logging.DEBUG)


class Ouro:
    # Resources
    datasets: Datasets
    files: Files
    posts: Posts
    conversations: Conversations
    users: Users
    assets: Assets
    comments: Comments
    services: Services
    routes: Routes

    # Client options
    api_key: str
    organization: str | None
    project: str | None

    # Clients
    client: httpx.Client
    supabase: supabase.Client  # public schema
    websocket: OuroWebSocket

    # Auth config
    access_token: str | None
    refresh_token: str | None

    # Connection options
    base_url: str | None
    database_url: str | None
    database_anon_key: str | None

    def __init__(
        self,
        *,
        api_key: str | None = None,
        organization: str | None = None,
        project: str | None = None,
        base_url: str | None = None,
        database_url: str | None = None,
        database_anon_key: str | None = None,
        # max_retries: int = DEFAULT_MAX_RETRIES,
        # default_headers: Mapping[str, str] | None = None,
        # default_query: Mapping[str, object] | None = None,
        # Configure a custom httpx client.
        # We provide a `DefaultHttpxClient` class that you can pass to retain the default values we use for `limits`, `timeout` & `follow_redirects`.
        # See the [httpx documentation](https://www.python-httpx.org/api/#client) for more details.
        # http_client: httpx.Client | None = None,
    ) -> None:
        """Construct a new synchronous ouro client instance.

        This automatically infers the following arguments from their corresponding environment variables if they are not provided:
        - `api_key` from `OURO_API_KEY`
        - `organization` from `OURO_ORG_ID`
        - `project` from `OURO_PROJECT_ID`
        """
        if api_key is None:
            api_key = os.environ.get("OURO_API_KEY")
        if api_key is None:
            raise OuroError(
                "The api_key client option must be set either by passing api_key to the client or by setting the OURO_API_KEY environment variable"
            )
        self.api_key = api_key

        if organization is None:
            organization = os.environ.get("OURO_ORG_ID")
        self.organization = organization

        if project is None:
            project = os.environ.get("OURO_PROJECT_ID")
        self.project = project

        # Mark the expiration of the last token refresh so we can deduplicate token refresh events
        self.last_token_refresh_expiration = None

        # Set config for Supabase client and Ouro client
        self.base_url = base_url or Config.OURO_BACKEND_URL
        self.websocket_url = f"{'wss' if self.base_url.startswith('https://') else 'ws'}://{self.base_url.replace('http://', '').replace('https://', '')}/socket.io/"
        self.database_url = database_url or Config.SUPABASE_URL
        self.database_anon_key = database_anon_key or Config.SUPABASE_ANON_KEY

        # Initialize WebSocket
        self.websocket = OuroWebSocket(self)

        # Initialize httpx client
        self.client = httpx.Client(
            base_url=self.base_url,
            # timeout=self.timeout,
        )
        # Perform initial token exchange
        self.exchange_api_key()
        self.initialize_clients()

        # Initialize resources
        self.conversations = Conversations(self)
        self.datasets = Datasets(self)
        self.files = Files(self)
        self.posts = Posts(self)
        self.assets = Assets(self)
        self.users = Users(self)
        self.comments = Comments(self)
        self.services = Services(self)
        self.routes = Routes(self)

    def _make_status_error(
        self,
        err_msg: str,
        *,
        body: object,
        response: httpx.Response,
    ) -> APIStatusError:
        data = body.get("error", body) if is_mapping(body) else body
        if response.status_code == 400:
            return BadRequestError(err_msg, response=response, body=data)
        if response.status_code == 401:
            return AuthenticationError(err_msg, response=response, body=data)
        if response.status_code == 403:
            return PermissionDeniedError(err_msg, response=response, body=data)
        if response.status_code == 404:
            return NotFoundError(err_msg, response=response, body=data)
        if response.status_code == 409:
            return ConflictError(err_msg, response=response, body=data)
        if response.status_code == 422:
            return UnprocessableEntityError(err_msg, response=response, body=data)
        if response.status_code == 429:
            return RateLimitError(err_msg, response=response, body=data)
        if response.status_code >= 500:
            return InternalServerError(err_msg, response=response, body=data)
        return APIStatusError(err_msg, response=response, body=data)

    def initialize_clients(self):
        self.supabase = supabase.create_client(
            self.database_url,
            self.database_anon_key,
            options=ClientOptions(
                auto_refresh_token=True,
                persist_session=False,
                headers={"Authorization": f"Bearer {self.access_token}"},
            ),
        )
        # Set the session for the supabase client
        auth = self.supabase.auth.set_session(self.access_token, self.refresh_token)
        # auth = self.supabase.auth.refresh_session()

        self.access_token = auth.session.access_token
        self.refresh_token = auth.session.refresh_token

        # Store the authenticated user
        self.user = auth.user
        log.info(f"Successfully authenticated as {self.user.email}")

        # Mark the expiration of the token
        self.last_token_refresh_expiration = auth.session.expires_at

        # Set the Authorization header for the client
        self.client.headers["Authorization"] = f"{self.access_token}"
        # Set refresh token as a cookie
        # self.client.cookies.set("refresh_token", self.refresh_token)

        # When the session changes, update the access token and refresh token
        def on_auth_state_change(event, session):
            if event == "TOKEN_REFRESHED":
                self.access_token = session.access_token
                self.refresh_token = session.refresh_token
                self.client.headers["Authorization"] = f"{session.access_token}"
                # self.client.cookies.set("refresh_token", session.refresh_token)
                # Reconnect the websocket and resubscribe to channels
                self.websocket.refresh_connection(session.access_token)
            else:
                print(event, session)

        self.supabase.auth.on_auth_state_change(on_auth_state_change)

    def exchange_api_key(self):
        response = self.client.post("/users/get-token", json={"pat": self.api_key})
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            raise Exception(data["error"])
        if not data.get("access_token"):
            raise Exception("No user found for this API key")

        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
