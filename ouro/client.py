from __future__ import annotations

import logging
import os
import time
from base64 import urlsafe_b64decode
from types import SimpleNamespace

import httpx
from ouro._logs import setup_logging
from ouro.config import Config
from ouro.realtime.websocket import OuroWebSocket
from ouro.resources import (
    Assets,
    Comments,
    Conversations,
    Datasets,
    Files,
    Money,
    Notifications,
    Organizations,
    Posts,
    Quests,
    Routes,
    Services,
    Teams,
    Users,
)

from .__version__ import __version__
from ._constants import DEFAULT_CONNECTION_LIMITS, DEFAULT_TIMEOUT
from ._exceptions import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
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

# Refresh token 5 minutes before expiry
TOKEN_REFRESH_BUFFER_SECONDS = 300

__all__ = ["Ouro"]


log: logging.Logger = logging.getLogger("ouro")


def _request_for_exception(
    exc: httpx.HTTPError, method: str, url: str
) -> httpx.Request:
    """Best-effort recovery of the `httpx.Request` attached to a transport error.

    `httpx.RequestError` normally carries `.request`, but when the exception was
    raised before request construction (rare, e.g. misconfigured transport) the
    attribute isn't set. In that case we synthesize a minimal `httpx.Request`
    so our SDK exception still has a meaningful `request` field.
    """
    try:
        request = getattr(exc, "request", None)
        if isinstance(request, httpx.Request):
            return request
    except RuntimeError:
        pass
    return httpx.Request(method, url)


def _translate_httpx_errors(
    call,
    method: str,
    url: str,
):
    """Invoke ``call()`` translating httpx transport errors to SDK exceptions."""
    try:
        return call()
    except httpx.TimeoutException as exc:
        raise APITimeoutError(
            request=_request_for_exception(exc, method, url)
        ) from exc
    except httpx.TransportError as exc:
        raise APIConnectionError(
            message=str(exc) or "Connection error.",
            request=_request_for_exception(exc, method, url),
        ) from exc


class AutoRefreshClient:
    """
    A wrapper around httpx.Client that automatically refreshes tokens before requests.

    This ensures that long-running processes don't encounter JWT expiration errors.

    It also translates httpx transport errors (timeouts, connect errors, etc.)
    into the SDK's typed :class:`~ouro.APITimeoutError` /
    :class:`~ouro.APIConnectionError` so callers can handle them via
    ``except OuroError``.
    """

    def __init__(self, client: httpx.Client, ouro: "Ouro"):
        self._client = client
        self._ouro = ouro

    def _ensure_valid_token(self):
        """Check and refresh token if needed before making a request."""
        if self._ouro._token_needs_refresh():
            log.info("Token expiring soon, refreshing proactively...")
            self._ouro.refresh_session()

    def _url_for(self, args, kwargs) -> str:
        url = kwargs.get("url")
        if url is None and args:
            url = args[0]
        return str(url) if url is not None else ""

    def _do(self, method: str, args, kwargs) -> httpx.Response:
        self._ensure_valid_token()
        fn = getattr(self._client, method)
        return _translate_httpx_errors(
            lambda: fn(*args, **kwargs),
            method.upper(),
            self._url_for(args, kwargs),
        )

    def get(self, *args, **kwargs) -> httpx.Response:
        return self._do("get", args, kwargs)

    def post(self, *args, **kwargs) -> httpx.Response:
        return self._do("post", args, kwargs)

    def put(self, *args, **kwargs) -> httpx.Response:
        return self._do("put", args, kwargs)

    def patch(self, *args, **kwargs) -> httpx.Response:
        return self._do("patch", args, kwargs)

    def delete(self, *args, **kwargs) -> httpx.Response:
        return self._do("delete", args, kwargs)

    def request(self, *args, **kwargs) -> httpx.Response:
        self._ensure_valid_token()
        method = kwargs.get("method")
        if method is None and args:
            method = args[0]
        url = kwargs.get("url")
        if url is None and len(args) > 1:
            url = args[1]
        return _translate_httpx_errors(
            lambda: self._client.request(*args, **kwargs),
            str(method or "").upper(),
            str(url or ""),
        )

    @property
    def headers(self):
        return self._client.headers

    @property
    def cookies(self):
        return self._client.cookies


class Ouro:
    # Resources
    datasets: Datasets
    files: Files
    posts: Posts
    quests: Quests
    conversations: Conversations
    users: Users
    assets: Assets
    comments: Comments
    services: Services
    routes: Routes
    money: Money
    notifications: Notifications
    organizations: Organizations
    teams: Teams

    # Client options
    api_key: str
    organization: str | None
    project: str | None

    # Clients
    client: AutoRefreshClient
    websocket: OuroWebSocket

    # Auth config
    access_token: str | None
    refresh_token: str | None

    # Connection options
    base_url: str | None

    def __init__(
        self,
        *,
        api_key: str | None = None,
        organization: str | None = None,
        project: str | None = None,
        base_url: str | None = None,
    ) -> None:
        """Construct a new synchronous ouro client instance.

        This automatically infers the following arguments from their corresponding environment variables if they are not provided:
        - `api_key` from `OURO_API_KEY`
        - `organization` from `OURO_ORG_ID`
        - `project` from `OURO_PROJECT_ID`
        """
        setup_logging()

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

        # Initialize WebSocket
        self.websocket = OuroWebSocket(self)

        self._raw_client = httpx.Client(
            base_url=self.base_url,
            headers={
                "User-Agent": f"ouro-py/{__version__}",
            },
            timeout=DEFAULT_TIMEOUT,
            limits=DEFAULT_CONNECTION_LIMITS,
        )
        # Perform initial token exchange (uses _raw_client)
        self.exchange_api_key()
        self._bootstrap_authenticated_client()

        # Wrap the client with auto-refresh capability
        self.client = AutoRefreshClient(self._raw_client, self)

        # Initialize resources
        self.conversations = Conversations(self)
        self.datasets = Datasets(self)
        self.files = Files(self)
        self.posts = Posts(self)
        self.quests = Quests(self)
        self.assets = Assets(self)
        self.users = Users(self)
        self.comments = Comments(self)
        self.routes = Routes(self)
        self.services = Services(self)
        self.money = Money(self)
        self.notifications = Notifications(self)
        self.organizations = Organizations(self)
        self.teams = Teams(self)

    def _make_status_error(
        self,
        err_msg: str,
        *,
        body: object,
        response: httpx.Response,
        status_override: int | None = None,
    ) -> APIStatusError:
        """Map a status code to a typed exception.

        ``status_override`` lets callers force the dispatch when the HTTP
        status of the response and the semantic status carried in the body
        envelope disagree. Specifically, the Ouro backend historically
        returned 200 OK with ``{ data: null, error: ... }`` for failures;
        when the body's error carries an explicit ``status`` field we
        prefer it so clients still see ``NotFoundError`` /
        ``PermissionDeniedError`` instead of a generic ``APIStatusError``.
        """
        status = status_override if status_override is not None else response.status_code
        data = body
        if status == 400:
            return BadRequestError(err_msg, response=response, body=data)
        if status == 401:
            return AuthenticationError(err_msg, response=response, body=data)
        if status == 403:
            return PermissionDeniedError(err_msg, response=response, body=data)
        if status == 404:
            return NotFoundError(err_msg, response=response, body=data)
        if status == 409:
            return ConflictError(err_msg, response=response, body=data)
        if status == 422:
            return UnprocessableEntityError(err_msg, response=response, body=data)
        if status == 429:
            return RateLimitError(err_msg, response=response, body=data)
        if status >= 500:
            return InternalServerError(err_msg, response=response, body=data)
        return APIStatusError(err_msg, response=response, body=data)

    def _jwt_expiration(self, token: str | None) -> int | None:
        if not token:
            return None
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            payload = parts[1]
            padding = "=" * (-len(payload) % 4)
            decoded = urlsafe_b64decode(payload + padding).decode("utf-8")
            import json

            data = json.loads(decoded)
            exp = data.get("exp")
            return int(exp) if exp is not None else None
        except Exception:
            return None

    def _bootstrap_authenticated_client(self):
        self._raw_client.headers["Authorization"] = f"{self.access_token}"
        self.last_token_refresh_expiration = self._jwt_expiration(self.access_token)

        # Store authenticated user details from backend.
        user_response = self._raw_client.get("/user")
        user_response.raise_for_status()
        user_payload = user_response.json()
        user_data = user_payload.get("data")
        self.user = SimpleNamespace(**user_data) if user_data else None
        if not self.user:
            raise AuthenticationError(
                "Failed to read authenticated user",
                response=user_response,
                body=user_payload,
            )
        log.info(f"Successfully authenticated as {self.user.email}")

    def exchange_api_key(self):
        response = self._raw_client.post("/users/get-token", json={"pat": self.api_key})
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            err = data["error"]
            if isinstance(err, dict):
                err_msg = err.get("message") or str(err)
            else:
                err_msg = str(err)
            raise AuthenticationError(err_msg, response=response, body=data)
        if not data.get("access_token"):
            raise AuthenticationError(
                "No user found for this API key", response=response, body=data
            )

        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]

        self._raw_client.headers["X-Ouro-Client"] = f"ouro-py/{__version__}"
        api_key_name = data.get("api_key_name")
        if api_key_name:
            self.api_key_name = api_key_name
            self._raw_client.headers["X-Ouro-Key-Name"] = api_key_name

    def _token_needs_refresh(self) -> bool:
        """Check if the token is expired or will expire soon."""
        if self.last_token_refresh_expiration is None:
            return False
        # Check if current time is within buffer of expiration
        return time.time() >= (
            self.last_token_refresh_expiration - TOKEN_REFRESH_BUFFER_SECONDS
        )

    def refresh_session(self) -> None:
        """
        Manually refresh the authentication session.

        Call this method if you encounter JWT expiration errors, or periodically
        for long-running processes.
        """
        log.info("Refreshing authentication session...")
        try:
            self.exchange_api_key()
            self._raw_client.headers["Authorization"] = f"{self.access_token}"
            self.last_token_refresh_expiration = self._jwt_expiration(self.access_token)
            self.websocket.refresh_connection(self.access_token)
            log.info("Session refreshed successfully")
        except Exception as e:
            log.warning(f"Failed to refresh session: {e}")
            raise

    def ensure_valid_token(self) -> None:
        """
        Ensure the token is valid, refreshing if needed.

        Call this before making API requests in long-running processes.
        """
        if self._token_needs_refresh():
            log.info("Token expiring soon, refreshing proactively...")
            self.refresh_session()
