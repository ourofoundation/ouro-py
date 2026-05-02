from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, List, Optional, Union
from urllib.parse import unquote

from ouro._exceptions import NotFoundError
from ouro._resource import SyncAPIResource
from ouro.models import Asset, Comment, Dataset, File, Post, Quest, Route, Service

log: logging.Logger = logging.getLogger(__name__)


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


class Assets(SyncAPIResource):
    def search(
        self,
        query: str = "",
        with_pagination: bool = False,
        **kwargs: Any,
    ) -> Union[List[dict], dict]:
        """
        Search or browse assets.

        When ``query`` is provided, performs hybrid semantic + full-text search.
        When ``query`` is omitted or empty, returns recent assets (browse mode)
        sorted by creation date.  Passing a UUID as the query looks up that
        single asset directly.

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
            limit:  page size (default 20, max 200)
            offset: pagination offset (default 0)

        Returns a list of asset dicts, or a dict with ``data`` and
        ``pagination`` keys when ``with_pagination=True``.
        """
        params: dict[str, Any] = {}
        if query:
            params["query"] = query

        limit = kwargs.pop("limit", 20)
        offset = kwargs.pop("offset", 0)
        params["limit"] = limit
        params["offset"] = offset

        scope = kwargs.pop("scope", None)
        if scope is not None:
            params["scope"] = scope

        sort = kwargs.pop("sort", None)
        if sort is not None:
            params["sort"] = sort

        time_window = kwargs.pop("time_window", None)
        if time_window is not None:
            params["time_window"] = time_window

        metadata_filters = kwargs.pop("metadata_filters", None)
        if metadata_filters is not None:
            params["metadata_filters"] = json.dumps(metadata_filters)

        filter_keys = ("asset_type", "org_id", "team_id", "user_id", "visibility", "source", "top_level_only")
        filters: dict[str, Any] = {}
        for key in filter_keys:
            if key in kwargs:
                filters[key] = kwargs.pop(key)
        if filters:
            params["filters"] = json.dumps(filters)

        params.update(kwargs)
        request = self.client.get("/search/assets", params=params)
        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            if not isinstance(result, dict):
                return {"data": [], "pagination": None}
            result["data"] = result.get("data") or []
            return result
        return self._handle_response(request) or []

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
    ) -> dict[str, Any]:
        """Download an asset to disk and return metadata about the saved file.

        Files are downloaded as their original bytes, datasets as CSV, and posts
        as HTML. If ``output_path`` points to a directory (or is omitted), the
        server-provided filename is used inside that directory.
        """
        self.ouro.ensure_valid_token()
        body = {"asset_type": asset_type} if asset_type else None

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

        return {
            "id": id,
            "path": str(target_path.resolve()),
            "filename": target_path.name,
            "content_type": content_type,
            "bytes": bytes_written,
        }

    def counts(self, id: str) -> dict:
        """Fetch engagement counts (views, comments, reactions, downloads) for an asset."""
        request = self.client.get(f"/assets/{id}/counts")
        return self._handle_response(request) or {}

    def connections(self, id: str) -> List[dict]:
        """Fetch the connection graph for an asset (references, components, derivatives, etc.)."""
        request = self.client.get(f"/assets/{id}/connections")
        return self._handle_response(request) or []

    def creation_actions(self, id: str) -> Optional[dict]:
        """Fetch the creation-action provenance for an asset (which route created it, with what inputs)."""
        request = self.client.get(f"/assets/{id}/creation-actions")
        return self._handle_response(request)

    def tags(self, id: str) -> List[dict]:
        """Fetch tags attached to an asset."""
        request = self.client.get(f"/assets/{id}/tags")
        return self._handle_response(request) or []

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
        with_pagination: bool = False,
    ) -> Union[List[dict], dict]:
        """Fetch routes that can operate on this asset.

        Routes default to popularity order (most used first). Pass ``limit`` and
        ``offset`` to request a page; set ``with_pagination=True`` to include the
        server pagination envelope. Output filters match both the primary route
        output and any structured ``output_assets`` metadata.
        """
        if with_pagination and limit is None:
            limit = 20

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
            f"/assets/{id}/compatible-routes",
            params={k: v for k, v in params.items() if v is not None},
        )
        if with_pagination:
            return self._handle_response(request, with_pagination=True) or {}
        return self._handle_response(request) or []

    def children(self, id: str) -> List[dict]:
        """Fetch child assets (e.g. routes of a service)."""
        request = self.client.get(f"/assets/{id}/children")
        return self._handle_response(request) or []

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
