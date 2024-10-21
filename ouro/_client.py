from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from typing import Any, Callable, Union

import httpx
import supabase
from ouro.config import Config
from ouro.realtime.websocket import OuroWebSocket
from ouro.resources import Conversations, Datasets, Files, Posts, Users
from supabase.client import ClientOptions
from typing_extensions import override

from .__version__ import __version__
from ._constants import (  # DEFAULT_TIMEOUT,; MAX_RETRY_DELAY,; INITIAL_RETRY_DELAY,; RAW_RESPONSE_HEADER,; OVERRIDE_CAST_TO_HEADER,; DEFAULT_CONNECTION_LIMITS,
    DEFAULT_MAX_RETRIES,
)
from ._exceptions import APIStatusError, OuroError
from ._utils import is_mapping  # is_given,

__all__ = ["Ouro"]


log: logging.Logger = logging.getLogger("ouro")


class OuroAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, api_key, refresh_url):
        self.api_key = api_key
        self.refresh_url = refresh_url
        self.access_token = None

    def auth_flow(self, request):
        # If we don't have an access token, we need to get one
        if not self.access_token:
            refresh_request = yield self.build_refresh_request()
            refresh_request.raise_for_status()
            response = refresh_request.json()
            if response["error"]:
                raise Exception(response["error"])
            self.update_tokens(response)

        request.headers["Authorization"] = self.access_token

        yield request

    def build_refresh_request(self):
        # Return an `httpx.Request` for refreshing tokens.
        return httpx.Request("POST", self.refresh_url, json={"pat": self.api_key})

    def update_tokens(self, data):
        self.access_token = data["token"]


class Ouro:
    # Resources
    datasets: Datasets
    files: Files
    posts: Posts
    conversations: Conversations
    users: Users

    # Client options
    api_key: str
    organization: str | None
    project: str | None

    # Clients
    client: httpx.Client
    database: supabase.Client  # datasets schema
    supabase: supabase.Client  # public schema
    websocket: OuroWebSocket

    # Auth config
    access_token: str | None
    refresh_token: str | None

    # Connection options
    database_url: str | httpx.URL | None
    database_anon_key: str | None
    base_url: str | httpx.URL | None

    def __init__(
        self,
        *,
        api_key: str | None = None,
        organization: str | None = None,
        project: str | None = None,
        base_url: str | httpx.URL | None = None,
        timeout: Union[float, None] = None,
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

        # Set config for Supabase client and Ouro client
        self.base_url = base_url or Config.OURO_BACKEND_URL
        self.websocket_url = f"ws://{self.base_url.replace('http://', '').replace('https://', '')}/socket.io/"
        self.database_url = Config.SUPABASE_URL
        self.database_anon_key = Config.SUPABASE_ANON_KEY

        # Initialize WebSocket
        self.websocket = OuroWebSocket(self)

        # Initialize httpx client
        self.client = httpx.Client(
            base_url=self.base_url,
            # timeout=self.timeout,
        )
        # Perform initial token exchange
        self.refresh_tokens()
        self.initialize_supabase_clients()

        # Initialize resources
        self.conversations = Conversations(self)
        self.datasets = Datasets(self)
        self.files = Files(self)
        self.posts = Posts(self)
        # self.users = Users(self)

        # Start token refresh thread
        self.token_refresh_interval = 3600  # 1 hour in seconds
        self.stop_refresh_thread = threading.Event()
        self.token_refresh_thread = threading.Thread(
            target=self._token_refresh_loop, daemon=True
        )
        self.token_refresh_thread.start()

    def _run_event_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    @override
    def _make_status_error(
        self,
        err_msg: str,
        *,
        body: object,
        response: httpx.Response,
    ) -> APIStatusError:
        data = body.get("error", body) if is_mapping(body) else body
        if response.status_code == 400:
            return _exceptions.BadRequestError(err_msg, response=response, body=data)

        if response.status_code == 401:
            return _exceptions.AuthenticationError(
                err_msg, response=response, body=data
            )

        if response.status_code == 403:
            return _exceptions.PermissionDeniedError(
                err_msg, response=response, body=data
            )

        if response.status_code == 404:
            return _exceptions.NotFoundError(err_msg, response=response, body=data)

        if response.status_code == 409:
            return _exceptions.ConflictError(err_msg, response=response, body=data)

        if response.status_code == 422:
            return _exceptions.UnprocessableEntityError(
                err_msg, response=response, body=data
            )

        if response.status_code == 429:
            return _exceptions.RateLimitError(err_msg, response=response, body=data)

        if response.status_code >= 500:
            return _exceptions.InternalServerError(
                err_msg, response=response, body=data
            )
        return APIStatusError(err_msg, response=response, body=data)

    def initialize_supabase_clients(self):
        self.database = supabase.create_client(
            self.database_url,
            self.database_anon_key,
            options=ClientOptions(
                schema="datasets",
                auto_refresh_token=True,
                persist_session=True,
            ),
        )
        self.supabase = supabase.create_client(
            self.database_url,
            self.database_anon_key,
            options=ClientOptions(
                auto_refresh_token=True,
                persist_session=True,
            ),
        )

        self.database.postgrest.auth(self.access_token)
        self.supabase.postgrest.auth(self.access_token)
        self.database.auth.set_session(self.access_token, self.refresh_token)
        self.supabase.auth.set_session(self.access_token, self.refresh_token)

        self.user = self.supabase.auth.get_user(self.access_token).user
        log.info(f"Successfully authenticated as {self.user.email}")

    def refresh_tokens(self):
        response = self.client.post("/users/get-token", json={"pat": self.api_key})
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            raise Exception(data["error"])
        if not data.get("access_token"):
            raise Exception("No user found for this API key")

        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        log.info("Refreshed tokens")

        # Update httpx and supabase clients with new session
        self._update_clients()
        # Refresh websocket connection
        self.websocket.refresh_connection()

    def _update_clients(self):
        # Update client headers
        self.client.headers["Authorization"] = f"{self.access_token}"

        # Update supabase clients
        if hasattr(self, "supabase"):
            self.supabase.postgrest.auth(self.access_token)
            self.supabase.auth.set_session(self.access_token, self.refresh_token)
        if hasattr(self, "database"):
            self.database.postgrest.auth(self.access_token)
            self.database.auth.set_session(self.access_token, self.refresh_token)

    def _token_refresh_loop(self):
        while not self.stop_refresh_thread.is_set():
            time.sleep(self.token_refresh_interval)
            if not self.stop_refresh_thread.is_set():
                try:
                    self.refresh_tokens()
                except Exception as e:
                    log.error(f"Failed to refresh tokens: {e}")

    def __del__(self):
        self.stop_refresh_thread.set()
        if hasattr(self, "token_refresh_thread"):
            self.token_refresh_thread.join(timeout=10)
        self.client.close()
