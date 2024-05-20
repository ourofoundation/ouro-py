from __future__ import annotations

import logging
import os
from typing import Any, Union

import httpx
import ouro.resources as resources
import supabase
from ouro.config import Config
from supabase.client import ClientOptions
from typing_extensions import override

from .__version__ import __version__
from ._constants import (  # DEFAULT_TIMEOUT,; MAX_RETRY_DELAY,; INITIAL_RETRY_DELAY,; RAW_RESPONSE_HEADER,; OVERRIDE_CAST_TO_HEADER,; DEFAULT_CONNECTION_LIMITS,
    DEFAULT_MAX_RETRIES,
)
from ._exceptions import APIStatusError, OpenAIError
from ._utils import is_mapping  # is_given,

__all__ = ["Ouro"]


log: logging.Logger = logging.getLogger(__name__)


class OuroAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, api_key, refresh_url):
        self.api_key = api_key
        self.refresh_url = refresh_url
        self.access_token = None

    def auth_flow(self, request):
        # If we don't have an access token, we need to get one
        if not self.access_token:
            refresh_response = yield self.build_refresh_request()
            self.update_tokens(refresh_response)

        request.headers["Authorization"] = self.access_token
        yield request

    def build_refresh_request(self):
        # Return an `httpx.Request` for refreshing tokens.
        return httpx.Request("POST", self.refresh_url, json={"pat": self.api_key})

    def update_tokens(self, response):
        data = response.json()
        self.access_token = data["token"]


class Ouro:
    # Resources
    elements: resources.Elements

    # client options
    api_key: str
    organization: str | None
    project: str | None

    # Clients
    client: httpx.Client
    database: supabase.Client  # datasets schema
    supabase: supabase.Client  # public schema

    # Auth config
    access_token: str | None

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
            raise OpenAIError(
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
        self.base_url = Config.OURO_BACKEND_URL
        self.database_url = Config.SUPABASE_URL
        self.database_anon_key = Config.SUPABASE_ANON_KEY

        # Make a private property for the access token
        self.access_token = None

        # Create the httpx client
        self.client = httpx.Client(
            auth=OuroAuth(api_key, refresh_url=self.base_url + "/users/get-token"),
            # version=__version__,
            base_url=self.base_url,
            # max_retries=max_retries,
            timeout=timeout,
            # http_client=http_client,
            # headers=default_headers,
            # custom_query=default_query,
        )

        # Run the login flow
        self.login()

        self.elements = resources.Elements(self)

    # @property
    # @override
    # def auth_headers(self) -> dict[str, str]:
    #     api_key = self.api_key
    #     return {"Authorization": f"Bearer {api_key}"}

    # @property
    # @override
    # def default_headers(self) -> dict[str, str | Omit]:
    #     return {
    #         **super().default_headers,
    #         "Ouro-Organization": (
    #             self.organization if self.organization is not None else Omit()
    #         ),
    #         "Ouro-Project": self.project if self.project is not None else Omit(),
    #         **self._custom_headers,
    #     }

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

    def login(self):
        url: str = self.database_url
        key: str = self.database_anon_key

        api_key = self.api_key

        # Send a request to Ouro Backend to get an access token
        req = self.client.post(
            "/users/get-token",
            json={"pat": api_key},
        )
        json = req.json()
        self.access_token = json["token"]

        if not self.access_token:
            raise Exception("No user found for this API key")

        self.database: supabase.Client = supabase.create_client(
            url,
            key,
            options=ClientOptions(
                schema="datasets",
                auto_refresh_token=True,
                persist_session=False,
            ),
        )
        self.supabase: supabase.Client = supabase.create_client(
            url,
            key,
            options=ClientOptions(
                auto_refresh_token=True,
                persist_session=False,
            ),
        )
        self.database.postgrest.auth(self.access_token)
        self.supabase.postgrest.auth(self.access_token)
        self.user = self.supabase.auth.get_user(self.access_token).user

        log.info(f"Successfully logged in as {self.user.email}.")
