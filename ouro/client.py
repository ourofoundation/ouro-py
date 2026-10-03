from __future__ import annotations

import logging
import os
import threading
import time
from base64 import urlsafe_b64decode
from types import SimpleNamespace
from uuid import UUID

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


def response_needs_auth_retry(response: httpx.Response) -> bool:
    """True when the backend failed to resolve the caller from the JWT."""
    if response.status_code == 401:
        return True
    if response.status_code < 500:
        return False
    try:
        body = response.json()
    except Exception:
        return False
    error = body.get("error") if isinstance(body, dict) else None
    message = error.get("message") if isinstance(error, dict) else error
    auth_error = str(message or "").strip().lower()
    return "no user context" in auth_error or auth_error == "no user"


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


def _is_uuid(value: str) -> bool:
    try:
        UUID(value)
        return True
    except (ValueError, AttributeError, TypeError):
        return False


# The organization every user is in; naming it means the personal context.
PERSONAL_ORG_ID = "00000000-0000-0000-0000-000000000000"


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

    def _url_for(self, args, kwargs) -> str:
        url = kwargs.get("url")
        if url is None and args:
            url = args[0]
        return str(url) if url is not None else ""

    def _send(self, call, method: str, url: str) -> httpx.Response:
        self._ouro.ensure_valid_token()
        token = self._ouro.access_token
        response = _translate_httpx_errors(call, method, url)
        if self._ouro.can_refresh and response_needs_auth_retry(response):
            log.info("Auth failed; re-exchanging API key and retrying once")
            self._ouro._refresh_unless_changed(token)
            response = _translate_httpx_errors(call, method, url)
        return response

    def _do(self, method: str, args, kwargs) -> httpx.Response:
        fn = getattr(self._client, method)
        return self._send(
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
        method = kwargs.get("method")
        if method is None and args:
            method = args[0]
        url = kwargs.get("url")
        if url is None and len(args) > 1:
            url = args[1]
        return self._send(
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
    api_key: str | None
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

    # Product identity for X-Ouro-Client (survives token refresh).
    # User-Agent is always ouro-py/<ver>; backend stores it as logs.sdk.
    _ouro_client: str
    _user_agent: str

    def __init__(
        self,
        *,
        api_key: str | None = None,
        organization: str | None = None,
        team: str | None = None,
        project: str | None = None,
        base_url: str | None = None,
        client: str | None = None,
        access_token: str | None = None,
    ) -> None:
        """Construct a new synchronous ouro client instance.

        This automatically infers the following arguments from their corresponding environment variables if they are not provided:
        - `api_key` from `OURO_API_KEY`
        - `organization` from `OURO_ORG_ID`
        - `team` from `OURO_TEAM_ID`
        - `project` from `OURO_PROJECT_ID`

        ``organization`` (a UUID or the org's name) pins the client to one
        organization: everything it creates goes there, its requests run in
        that organization's context (so an organization that pays for its
        members' usage pays for this client's), and creating in or moving to
        another organization raises ``OuroError``. An unpinned client runs in
        the personal context. An API key bound to an organization pins the
        client to it; a key bound to the personal context can't be pinned to
        an organization. Reads are not
        restricted. ``team`` (a UUID) is where new assets go when a call
        doesn't name a team; it defaults to the organization's default team.
        Pass ``organization=""`` to ignore ``OURO_ORG_ID`` and stay unpinned.
        Visibility left unset follows the team: public in a public team,
        organization-only in an internal one.

        ``access_token`` authenticates with an Ouro access token issued
        elsewhere (for example by the OAuth flow) instead of exchanging an API
        key. The client cannot refresh it; whoever issued it owns renewal.

        ``client`` sets ``X-Ouro-Client`` (product identity for activity logs).
        Wrappers should pass their own name/version (e.g. ``"ouro-mcp/0.7.10"``).
        ``User-Agent`` is always ``ouro-py/<ver>`` and is stored separately as ``sdk``.
        """
        setup_logging()

        if api_key is not None and access_token is not None:
            raise OuroError("Pass either api_key or access_token, not both")
        if api_key is None and access_token is None:
            api_key = os.environ.get("OURO_API_KEY")
        if api_key is None and access_token is None:
            raise OuroError(
                "The api_key client option must be set either by passing api_key to the client or by setting the OURO_API_KEY environment variable"
            )
        self.api_key = api_key

        if organization is None:
            organization = os.environ.get("OURO_ORG_ID")
        if team is None:
            team = os.environ.get("OURO_TEAM_ID")
        self.organization = None
        self.team = None
        self._default_team_id = None

        if project is None:
            project = os.environ.get("OURO_PROJECT_ID")
        self.project = project

        self._user_agent = f"ouro-py/{__version__}"
        self._ouro_client = (client or "").strip() or self._user_agent

        # Mark the expiration of the last token refresh so we can deduplicate token refresh events
        self.last_token_refresh_expiration = None
        self._refresh_lock = threading.RLock()

        # Set config for Supabase client and Ouro client
        self.base_url = base_url or Config.OURO_BACKEND_URL
        self.websocket_url = f"{'wss' if self.base_url.startswith('https://') else 'ws'}://{self.base_url.replace('http://', '').replace('https://', '')}/socket.io/"

        # Initialize WebSocket
        self.websocket = OuroWebSocket(self)

        self._raw_client = httpx.Client(
            base_url=self.base_url,
            headers={
                "User-Agent": self._user_agent,
                "X-Ouro-Client": self._ouro_client,
            },
            timeout=DEFAULT_TIMEOUT,
            limits=DEFAULT_CONNECTION_LIMITS,
        )
        self._apply_org_context()
        if access_token is not None:
            self.access_token = access_token
            self.refresh_token = None
        else:
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

        # A key bound to one organization can't act anywhere else, so it
        # decides the pin unless the caller named one
        key_org = getattr(self, "api_key_org_id", None)
        if not organization and key_org and key_org != PERSONAL_ORG_ID:
            organization = key_org
        if organization:
            self.use_organization(organization, team=team or None)

    def use_organization(
        self, organization: str | None, team: str | None = None
    ) -> None:
        """Pin this client to an organization, or unpin it with ``None``.

        ``organization`` is a UUID or the organization's name. ``team`` is the
        UUID of the team new assets go to when a call doesn't name one; it
        defaults to the organization's default team.
        """
        if not organization:
            self.organization = None
            self.team = None
            self._default_team_id = None
            self._apply_org_context()
            return

        organization = str(organization).strip()
        if not _is_uuid(organization):
            response = self.client.get(f"/organizations/by-name/{organization}")
            data = (response.json() or {}).get("data") if response.is_success else None
            if not data or not data.get("id"):
                raise OuroError(f"Organization '{organization}' was not found")
            organization = str(data["id"])

        organization = organization.lower()
        # The backend refuses this on every request; say so once, up front
        key_org = getattr(self, "api_key_org_id", None)
        if key_org and organization != key_org:
            bound_to = (
                "your personal context"
                if key_org == PERSONAL_ORG_ID
                else f"organization {key_org}"
            )
            raise OuroError(
                f"This API key is bound to {bound_to} and can't act in "
                f"organization {organization}. Use a key made for it."
            )

        self.organization = organization
        self.team = str(team).strip() if team else None
        self._default_team_id = None
        self._apply_org_context()

    def _apply_org_context(self) -> None:
        """Name the organization this client's requests run in.

        A pinned client names its organization on every request. An unpinned
        one names none and runs in the personal context (or wherever its API
        key is bound).
        """
        if self.organization:
            self._raw_client.headers["X-Ouro-Org"] = self.organization
        else:
            self._raw_client.headers.pop("X-Ouro-Org", None)

    def _check_organization(self, org_id: object) -> None:
        """Refuse a write aimed at an organization other than the pinned one."""
        if not self.organization or org_id is None:
            return
        if str(org_id).lower() != self.organization:
            raise OuroError(
                f"This client is pinned to organization {self.organization} and "
                f"can't write to {org_id}. Call use_organization() to switch."
            )

    def _resolve_default_team(self) -> str | None:
        if self.team:
            return self.team
        if self._default_team_id is None:
            response = self.client.get(f"/organizations/{self.organization}")
            data = (response.json() or {}).get("data") if response.is_success else None
            default_team = (data or {}).get("default_team") or {}
            if not default_team.get("id"):
                raise OuroError(
                    f"Couldn't read the default team of organization {self.organization}. "
                    "Check that this account is a member, or pass team=."
                )
            self._default_team_id = str(default_team["id"])
        return self._default_team_id

    def _scope_create(self, asset: dict, *, default_team: bool = True) -> dict:
        """Place a new asset in the pinned organization (no-op when unpinned)."""
        if not self.organization:
            return asset
        self._check_organization(asset.get("org_id"))
        asset["org_id"] = self.organization
        if default_team and not asset.get("team_id"):
            asset["team_id"] = self._resolve_default_team()
        return asset

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

        # Re-apply on every exchange so token refresh cannot clobber a
        # wrapper identity (e.g. ouro-mcp) set via the constructor.
        self._raw_client.headers["X-Ouro-Client"] = self._ouro_client
        self._raw_client.headers["User-Agent"] = self._user_agent
        # The context the key is bound to; None when it is unbound
        self.api_key_org_id = data.get("api_key_org_id")
        api_key_name = data.get("api_key_name")
        if api_key_name:
            self.api_key_name = api_key_name
            self._raw_client.headers["X-Ouro-Key-Name"] = api_key_name

    @property
    def can_refresh(self) -> bool:
        """Whether this client can mint a new access token on its own."""
        return self.api_key is not None

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
        if not self.can_refresh:
            raise OuroError(
                "This client was created with an access token and cannot refresh it. "
                "Create a new client with a fresh token."
            )
        log.info("Refreshing authentication session...")
        with self._refresh_lock:
            try:
                self.exchange_api_key()
                self._raw_client.headers["Authorization"] = f"{self.access_token}"
                self.last_token_refresh_expiration = self._jwt_expiration(self.access_token)
                if self.websocket.is_connected:
                    self.websocket.refresh_connection(self.access_token)
                log.info("Session refreshed successfully")
            except Exception as e:
                log.warning(f"Failed to refresh session: {e}")
                raise

    def _refresh_unless_changed(self, token: str | None) -> None:
        """Refresh only if ``token`` is still current.

        The backend's API-key exchange is single-use, so concurrent refreshes
        fail; threads that lose the race reuse the winner's new token.
        """
        with self._refresh_lock:
            if self.access_token == token:
                self.refresh_session()

    def ensure_valid_token(self) -> None:
        """
        Ensure the token is valid, refreshing if needed.

        Call this before making API requests in long-running processes.
        """
        if not (self.can_refresh and self._token_needs_refresh()):
            return
        with self._refresh_lock:
            if self._token_needs_refresh():
                log.info("Token expiring soon, refreshing proactively...")
                self.refresh_session()
