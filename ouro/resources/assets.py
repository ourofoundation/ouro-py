from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Type, Union
from urllib.parse import unquote
from uuid import UUID

from ouro._exceptions import NotFoundError
from ouro._resource import M, SyncAPIResource, _strip_none
from ouro.models import (
    Asset,
    AssetActions,
    AssetCounts,
    AssetImpact,
    AssetTag,
    Comment,
    Connection,
    Dataset,
    DeleteResult,
    Download,
    File,
    Page,
    Post,
    Quest,
    Route,
    Service,
)

log: logging.Logger = logging.getLogger(__name__)

# Models fill unused search filters with blanks or string-nulls. Postgres
# rejects "" and "/null" for uuid/enum columns.
_ABSENT_OPTIONAL_STRINGS = frozenset({"null", "none", "undefined", "/null"})


def _present_optional(value: Any) -> Any:
    """Return value, or None when it means "this filter is unset"."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in _ABSENT_OPTIONAL_STRINGS:
            return None
        return stripped
    return value


__all__ = ["Assets"]


def _extract_download_filename(
    content_disposition: Optional[str],
    fallback: str,
) -> str:
    """Parse a safe filename from Content-Disposition."""
    if not content_disposition:
        return fallback

    for part in [segment.strip() for segment in content_disposition.split(";")]:
        if part.lower().startswith("filename*="):
            raw_value = part.split("=", 1)[1].strip().strip('"')
            _, _, encoded_value = raw_value.partition("''")
            candidate = unquote(encoded_value or raw_value)
            name = Path(candidate).name
            return name or fallback

    for part in [segment.strip() for segment in content_disposition.split(";")]:
        if part.lower().startswith("filename="):
            candidate = part.split("=", 1)[1].strip().strip('"')
            name = Path(candidate).name
            return name or fallback

    return fallback


def _resolve_download_path(
    output_path: Optional[str],
    filename: str,
) -> Path:
    """Resolve an output file path, allowing directory targets."""
    if output_path is None:
        target = Path.cwd() / filename
    else:
        candidate = Path(output_path).expanduser()
        output_text = output_path.rstrip()
        is_directory_target = (
            candidate.exists() and candidate.is_dir()
        ) or output_text.endswith(("/", "\\"))
        target = candidate / filename if is_directory_target else candidate

    target.parent.mkdir(parents=True, exist_ok=True)
    return target


# Server-enforced maximum page size for /search/assets.
SEARCH_PAGE_MAX = 200


class Assets(SyncAPIResource):
    def search(self, query: str = "", **kwargs: Any) -> Page[Asset]:
        """
        Search or browse assets.

        When ``query`` is provided, performs hybrid semantic + full-text search.
        When ``query`` is omitted or empty, returns recent assets (browse mode)
        sorted by creation date.  Passing a UUID as the query looks up that
        single asset directly.

        Pagination is transparent: the server caps each request at 200 results,
        so ``limit`` values above 200 — or ``limit=None`` for *all* matches —
        are fulfilled by paginating internally. For exhaustive collection use
        browse mode (empty ``query``); semantic search caps its candidate pool
        and cannot enumerate everything.

        Keyword arguments (all optional):
            asset_type:  "dataset", "post", "file", "service", "route", "quest"
                         (may also be a list, e.g. ["file", "dataset"])
            scope:       "personal" | "org" | "global" | "all"
            org_id:      scope to an organization (UUID)
            team_id:     scope to a team within an org (UUID)
            user_id:     filter by asset owner (UUID)
            visibility:  "public" | "private" | "organization" | "monetized"
            source:      "web" | "api"
            top_level_only: True to exclude child assets
            metadata_filters: dict of metadata key/values, e.g.
                {"file_type": "image", "extension": "csv"}
            sort:        "relevant" | "recent" | "popular" | "updated"
                         Defaults to "relevant" when query is present,
                         "recent" when browsing.
            time_window: "day" | "week" | "month" | "all"
                         Only used when sort="popular". Default: "month".
            limit:  max results to return (default 20). Values above 200
                    paginate internally; ``None`` fetches all matches.
            offset: pagination offset (default 0)
        """
        return self._search(Asset, query, **kwargs)

    def _search(self, model: Type[M], query: str = "", **kwargs: Any) -> Page[M]:
        """Search assets, parsing each hit as *model*."""
        limit = kwargs.pop("limit", 20)
        start = offset = int(kwargs.pop("offset", 0))

        collected: List[dict] = []
        pagination: dict = {}
        while True:
            remaining = None if limit is None else limit - len(collected)
            page_limit = SEARCH_PAGE_MAX if remaining is None else min(remaining, SEARCH_PAGE_MAX)
            body = self._search_page(query, page_limit, offset, dict(kwargs))
            data = body.get("data") or []
            collected.extend(data)
            pagination = body.get("pagination") or {}
            # A limit within the server cap is a single page: the caller pages.
            if (
                not data
                or not pagination.get("hasMore")
                or (limit is not None and (limit <= SEARCH_PAGE_MAX or len(collected) >= limit))
            ):
                break
            offset += len(data)

        return self._page(
            Page[model],
            {"data": collected, "pagination": {**pagination, "offset": start, "limit": limit}},
        )

    def _search_page(self, query: str, limit: int, offset: int, kwargs: dict) -> dict:
        """Fetch one raw ``{data, pagination}`` page from /search/assets."""
        params: dict[str, Any] = {}
        if query:
            params["query"] = query

        params["limit"] = limit
        params["offset"] = offset

        scope = _present_optional(kwargs.pop("scope", None))
        if scope is not None:
            params["scope"] = scope

        sort = _present_optional(kwargs.pop("sort", None))
        if sort is not None:
            params["sort"] = sort

        time_window = _present_optional(kwargs.pop("time_window", None))
        if time_window is not None:
            params["time_window"] = time_window

        metadata_filters = kwargs.pop("metadata_filters", None)
        if metadata_filters is not None:
            params["metadata_filters"] = json.dumps(metadata_filters)

        filter_keys = ("asset_type", "org_id", "team_id", "user_id", "visibility", "source", "top_level_only")
        filters: dict[str, Any] = {}
        for key in filter_keys:
            if key not in kwargs:
                continue
            value = _present_optional(kwargs.pop(key))
            # Blank / string-null filters are "unset" — Postgres rejects them
            # for uuid/enum cols ("" and "/null").
            if value is None:
                continue
            filters[key] = value
        if filters:
            params["filters"] = json.dumps(filters)

        params.update(kwargs)
        request = self.client.get("/search/assets", params=params)
        return self._handle_response(request, raw=True) or {}

    def retrieve(
        self,
        id: str,
    ) -> Union[Post, Comment, File, Dataset, Service, Route, Quest, Asset]:
        """
        Retrieve any asset by its ID, regardless of asset type.
        Automatically determines the asset type and routes to the
        appropriate resource's retrieve method.
        """
        request = self.client.get(f"/assets/{id}/type")
        data = self._handle_response(request)

        asset_type = data.get("asset_type") if data else None

        if not asset_type:
            raise NotFoundError(
                f"Asset with id {id} has no asset_type",
                response=request,
                body=data,
            )

        self._mark_viewed(id)

        if asset_type == "post":
            return self.ouro.posts.retrieve(id)
        elif asset_type == "comment":
            return self.ouro.comments.retrieve(id)
        elif asset_type == "file":
            return self.ouro.files.retrieve(id)
        elif asset_type == "dataset":
            return self.ouro.datasets.retrieve(id)
        elif asset_type == "service":
            return self.ouro.services.retrieve(id)
        elif asset_type == "route":
            return self.ouro.routes.retrieve(id)
        elif asset_type == "quest":
            return self.ouro.quests.retrieve(id)
        else:
            log.warning(
                f"Unknown asset type: {asset_type}. Cannot retrieve full asset details via API."
            )
            raise ValueError(
                f"Asset type '{asset_type}' is not supported by the unified retrieve method. "
                f"Please use the specific resource's retrieve method if available."
            )

    def download(
        self,
        id: str,
        output_path: Optional[str] = None,
        asset_type: Optional[str] = None,
        format: Optional[str] = None,
    ) -> Download:
        """Download an asset to disk and describe the saved file.

        Files are downloaded as their original bytes, datasets as CSV, and posts
        as HTML, or as markdown with ``format="markdown"``. If ``output_path``
        points to a directory (or is omitted), the server-provided filename is
        used inside that directory.
        """
        self.ouro.ensure_valid_token()
        body = _strip_none({"asset_type": asset_type, "format": format}) or None

        with self.ouro._raw_client.stream(
            "POST",
            f"/assets/{id}/download",
            json=body,
        ) as response:
            if response.is_error:
                try:
                    body = response.json()
                except Exception:
                    body = None
                error_msg = ""
                if isinstance(body, dict):
                    error_msg = self._extract_error_message(body.get("error", body))
                raise self.ouro._make_status_error(
                    error_msg or f"HTTP {response.status_code}",
                    response=response,
                    body=body,
                )

            filename = _extract_download_filename(
                response.headers.get("content-disposition"),
                fallback=f"{id}.bin",
            )
            target_path = _resolve_download_path(output_path, filename)
            content_type = response.headers.get("content-type")

            bytes_written = 0
            with target_path.open("wb") as fh:
                for chunk in response.iter_bytes():
                    if not chunk:
                        continue
                    fh.write(chunk)
                    bytes_written += len(chunk)

        return Download(
            id=id,
            path=str(target_path.resolve()),
            filename=target_path.name,
            content_type=content_type,
            size=bytes_written,
        )

    def create_download_url(
        self,
        id: str,
        asset_type: Optional[str] = None,
        format: Optional[str] = None,
    ) -> dict:
        """Get a link that downloads an asset without credentials.

        For a caller that fetches the bytes itself, such as an agent running
        ``curl``. Files keep their original bytes, datasets download as CSV,
        and posts as markdown (or ``format="html"``).

        Returns ``download_url``, ``file_name``, ``content_type``,
        ``expires_in`` (seconds) and ``asset_type``.
        """
        request = self.client.post(
            f"/assets/{id}/download-url",
            json=_strip_none({"asset_type": asset_type, "format": format}),
        )
        return self._handle_response(request)

    def share(
        self,
        id: str,
        user_id: Union[UUID, str],
        role: Literal["read", "write", "admin"] = "read",
    ) -> None:
        """Grant a user direct permission on an asset.

        Caller must be an admin on the asset. Mentions and links do not grant
        access — private assets stay invisible until shared.
        """
        request = self.client.put(
            f"/assets/{id}/share",
            json={
                "permission": {
                    "user": {"user_id": str(user_id)},
                    "role": role,
                }
            },
        )
        self._handle_response(request)

    def counts(self, id: str) -> AssetCounts:
        """Fetch engagement counts (views, comments, reactions, downloads) for an asset."""
        request = self.client.get(f"/assets/{id}/counts")
        return self._parse(AssetCounts, self._handle_response(request))

    def impact(
        self,
        ids: list[str] | str,
        *,
        since: Optional[str] = None,
    ) -> List[AssetImpact]:
        """Batch impact metrics for one or more assets.

        Returns external-vs-self engagement, bot-filtered quality views, and
        quest provenance when available.
        """
        if isinstance(ids, str):
            id_list = [ids]
        else:
            id_list = [str(i) for i in ids if i]
        params: Dict[str, Any] = {"ids": ",".join(id_list)}
        if since:
            params["since"] = since
        request = self.client.get("/assets/impact", params=params)
        data = self._handle_response(request) or {}
        return self._parse_list(AssetImpact, data.get("assets"))

    def connections(
        self, id: str, *, limit: Optional[int] = None, offset: int = 0
    ) -> Page[Connection]:
        """Fetch a page of the asset's connection graph (references, components, derivatives, etc.)."""
        params = _strip_none({"limit": limit, "offset": offset or None})
        request = self.client.get(f"/assets/{id}/connections", params=params)
        return self._page(Page[Connection], self._handle_response(request, raw=True))

    def actions(
        self,
        id: str,
        *,
        role: Literal["input", "output", "both"] = "both",
        status: Optional[str] = None,
        side_effects: Optional[bool] = None,
        include_response: bool = False,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> AssetActions:
        """List route actions linked to an asset.

        Args:
            id: Asset UUID.
            role:
                - ``"input"``: actions that used this asset as an input
                - ``"output"``: the action that produced this asset (if any)
                - ``"both"`` (default): both directions in one call
            status: Optional filter for input-side actions
                (``queued`` / ``in-progress`` / ``success`` / ``error`` /
                ``timed-out``).
            side_effects: Optional filter for input-side actions.
            include_response: When ``True``, include each action's
                ``response`` / ``metadata`` payloads (needed for calculated
                properties). Defaults to ``False`` for compact browsing.
            limit: Max as_input actions per request (server max 200). When
                ``None`` (default), pages through until exhausted.
            offset: Pagination offset for as_input (ignored when ``limit``
                is ``None``).

        Returns:
            ``created_by`` (the producing action, if any) and ``as_input``.
            Unused sides are ``None`` / ``[]`` when ``role`` is narrowed;
            ``has_more`` reports whether more ``as_input`` actions remain.
        """
        if role not in {"input", "output", "both"}:
            raise ValueError(
                f"role must be 'input', 'output', or 'both'; got {role!r}"
            )

        def fetch(req_role: str, page_limit: Optional[int], page_offset: int) -> dict:
            params = _strip_none(
                {
                    "role": req_role,
                    "status": status,
                    "side_effects": (
                        None
                        if side_effects is None
                        else ("true" if side_effects else "false")
                    ),
                    "include_response": "true" if include_response else "false",
                    "limit": page_limit,
                    "offset": page_offset if page_limit is not None else None,
                }
            )
            request = self.client.get(f"/assets/{id}/actions", params=params)
            return self._handle_response(request, raw=True) or {}

        if role == "output":
            data = fetch("output", None, 0).get("data")
            return self._parse(AssetActions, {"created_by": data})

        # "both" returns {created_by, as_input}; "input" returns the list alone.
        def split(body: dict) -> tuple[Optional[dict], List[dict], bool]:
            data = body.get("data")
            has_more = bool((body.get("pagination") or {}).get("hasMore"))
            if isinstance(data, dict):
                return data.get("created_by"), data.get("as_input") or [], has_more
            return None, data or [], has_more

        page_size = limit if limit is not None else SEARCH_PAGE_MAX
        created_by, as_input, has_more = split(fetch(role, page_size, offset))
        while limit is None and has_more:
            offset += page_size
            _, rows, has_more = split(fetch("input", page_size, offset))
            as_input.extend(rows)

        return self._parse(
            AssetActions,
            {"created_by": created_by, "as_input": as_input, "has_more": has_more},
        )

    def tags(self, id: str) -> List[AssetTag]:
        """Fetch tags attached to an asset."""
        request = self.client.get(f"/assets/{id}/tags")
        return self._parse_list(AssetTag, self._handle_response(request))

    def compatible_routes(
        self,
        id: str,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
        sort: str = "popular",
        output_type: Optional[str] = None,
        output_asset_type: Optional[str] = None,
        output_file_extension: Optional[str] = None,
        contains_file_extension: Optional[str] = None,
    ) -> Page[Route]:
        """Fetch routes that can operate on this asset.

        Routes default to popularity order (most used first). Without a
        ``limit`` every compatible route is returned in one page. Output
        filters match both the primary route output and any structured
        ``output_assets`` metadata.
        """
        params = {
            "limit": limit,
            "offset": offset if limit is not None else None,
            "sort": sort,
            "output_type": output_type,
            "output_asset_type": output_asset_type,
            "output_file_extension": output_file_extension,
            "contains_file_extension": contains_file_extension,
        }
        request = self.client.get(
            f"/assets/{id}/compatible-routes", params=_strip_none(params)
        )
        return self._page(Page[Route], self._handle_response(request, raw=True))

    def children(self, id: str) -> List[Asset]:
        """Fetch child assets (e.g. routes of a service)."""
        request = self.client.get(f"/assets/{id}/children")
        return self._parse_list(Asset, self._handle_response(request))

    def delete(
        self,
        id: str,
        *,
        delete_children: Optional[bool] = None,
        dry_run: bool = False,
    ) -> DeleteResult:
        """Delete any asset by ID, cascading children when requested.

        Auto-detects asset type and routes to the type-specific delete
        endpoint. When ``delete_children`` is omitted, services default to
        cascading routes and other types leave children alone.

        Args:
            dry_run: When True, return the delete summary without deleting.

        Returns:
            What was deleted, or would be when ``dry_run`` is true.
        """
        request = self.client.get(f"/assets/{id}/type")
        data = self._handle_response(request) or {}
        asset_type = data.get("asset_type")
        if not asset_type:
            raise NotFoundError(
                f"Asset with id {id} has no asset_type",
                response=request,
                body=data,
            )

        if delete_children is None:
            delete_children = asset_type == "service"

        resources = {
            "post": "posts",
            "comment": "comments",
            "file": "files",
            "dataset": "datasets",
            "service": "services",
            "quest": "quests",
            "route": "routes",
        }
        attr = resources.get(asset_type)
        if attr is None:
            raise ValueError(
                f"Cannot delete asset of type '{asset_type}' via assets.delete"
            )
        return getattr(self.ouro, attr).delete(
            id, delete_children=delete_children, dry_run=dry_run
        )

    def _mark_viewed(self, asset_id: str) -> None:
        """Best-effort view recording to keep unread counts in sync."""
        try:
            self.client.post(
                f"/assets/{asset_id}/view",
                json={
                    "source": "api",
                    "type": "full",
                    "pathname": f"/assets/{asset_id}",
                },
            )
        except Exception:
            log.debug("Failed to record view for asset %s", asset_id, exc_info=True)
