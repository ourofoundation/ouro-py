from __future__ import annotations

import json
import logging
from typing import Any, List, Union

from ouro._resource import SyncAPIResource
from ouro.models import Asset, Comment, Dataset, File, Post, Route, Service

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Assets"]


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
    ) -> Union[Post, Comment, File, Dataset, Service, Route, Asset]:
        """
        Retrieve any asset by its ID, regardless of asset type.
        Automatically determines the asset type and routes to the
        appropriate resource's retrieve method.
        """
        try:
            request = self.client.get(f"/assets/{id}/type")
            data = self._handle_response(request)

            asset_type = data.get("asset_type") if data else None

            if not asset_type:
                raise Exception(f"Asset with id {id} has no asset_type")

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
            else:
                log.warning(
                    f"Unknown asset type: {asset_type}. Cannot retrieve full asset details via API."
                )
                raise Exception(
                    f"Asset type '{asset_type}' is not supported by the unified retrieve method. "
                    f"Please use the specific resource's retrieve method if available."
                )
        except Exception as e:
            error_str = str(e).lower()
            if "not found" in error_str or "404" in error_str or "no rows" in error_str:
                raise Exception(f"Asset with id {id} not found") from e
            raise
