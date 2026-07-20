from __future__ import annotations

import logging
from typing import Dict, List, Optional, Union

from ouro._resource import (
    SyncAPIResource,
    _attribution_payload,
    _coerce_description,
    _optional_attribution_payload,
    _strip_none,
)
from ouro.models import Route, Service
from ouro.resources.routes import Routes

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Services"]


def _service_metadata(
    *,
    base_url: Optional[str] = None,
    authentication: Optional[str] = None,
    version: Optional[str] = None,
    spec_path: Optional[str] = None,
    spec_url: Optional[str] = None,
    auth_url: Optional[str] = None,
) -> dict:
    return _strip_none(
        {
            "base_url": base_url,
            "authentication": authentication,
            "version": version,
            "spec_path": spec_path,
            "spec_url": spec_url,
            "auth_url": auth_url,
        }
    )


class Services(SyncAPIResource):
    @property
    def routes(self) -> Routes:
        """Deprecated alias for ``ouro.routes``.

        Kept for backwards compatibility. Prefer ``ouro.routes`` directly — both
        point to the same underlying ``Routes`` instance now.
        """
        return self.ouro.routes

    def create(
        self,
        name: str,
        base_url: str,
        visibility: str = "public",
        authentication: str = "None",
        description: Optional[Union[str, "Content"]] = None,
        spec_path: Optional[str] = None,
        spec_url: Optional[str] = None,
        version: Optional[str] = None,
        auth_url: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        license_id: str = "MIT",
        originality: Optional[str] = None,
        github_url: Optional[str] = None,
        paper_url: Optional[str] = None,
        doi_url: Optional[str] = None,
        external_url: Optional[str] = None,
        relation_type: Optional[str] = None,
        **kwargs,
    ) -> Service:
        """Create a Service — an external API published as an Ouro asset.

        ``base_url`` must be unique across Ouro. ``authentication`` is one of
        "None", "Ouro", "Personal Access Token", or "OAuth 2.0".

        Pass ``spec_url`` (or ``spec_path`` for an already-uploaded file) to
        register the service from an OpenAPI spec — its routes are parsed and
        created automatically. Omit both to create a service with no routes yet.

        Attribution (stored on ``attribution``, not ``metadata``): ``license_id``
        (SPDX id, default MIT), ``originality`` (``original`` | ``derivative`` |
        ``third-party``, default ``original``), optional ``github_url`` /
        ``paper_url`` / ``doi_url`` / ``external_url``, and optional
        ``relation_type`` (DataCite relation to the related paper).
        """
        metadata = _service_metadata(
            base_url=base_url,
            authentication=authentication,
            version=version,
            spec_path=spec_path,
            spec_url=spec_url,
            auth_url=auth_url,
        )
        attribution = kwargs.pop("attribution", None) or _attribution_payload(
            originality=originality,
            github_url=github_url,
            paper_url=paper_url,
            doi_url=doi_url,
            external_url=external_url,
            relation_type=relation_type,
        )
        service = _strip_none(
            {
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                "license_id": license_id,
                **kwargs,
                "source": "api",
                "asset_type": "service",
                "metadata": metadata,
                "attribution": attribution,
            }
        )

        endpoint = (
            "/services/create/from-file"
            if spec_path or spec_url
            else "/services/create/from-form"
        )
        request = self.client.post(endpoint, json={"service": service})
        return Service(**self._handle_response(request), _ouro=self.ouro)

    def update(
        self,
        id: str,
        name: Optional[str] = None,
        visibility: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        base_url: Optional[str] = None,
        authentication: Optional[str] = None,
        spec_path: Optional[str] = None,
        spec_url: Optional[str] = None,
        version: Optional[str] = None,
        auth_url: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        license_id: Optional[str] = None,
        originality: Optional[str] = None,
        github_url: Optional[str] = None,
        paper_url: Optional[str] = None,
        doi_url: Optional[str] = None,
        external_url: Optional[str] = None,
        relation_type: Optional[str] = None,
        **kwargs,
    ) -> Service:
        """Update a Service by its ID.

        Service config fields (``base_url``, ``authentication``, …) merge into
        ``metadata``. Provenance fields merge into ``attribution``. Providing
        ``spec_path`` or ``spec_url`` re-parses the OpenAPI spec and syncs routes.
        """
        metadata = _service_metadata(
            base_url=base_url,
            authentication=authentication,
            version=version,
            spec_path=spec_path,
            spec_url=spec_url,
            auth_url=auth_url,
        )
        attribution = kwargs.pop("attribution", None) or _optional_attribution_payload(
            originality=originality,
            github_url=github_url,
            paper_url=paper_url,
            doi_url=doi_url,
            external_url=external_url,
            relation_type=relation_type,
        )
        service = _strip_none(
            {
                "id": str(id),
                "name": name,
                "description": _coerce_description(description),
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                "license_id": license_id,
                **kwargs,
                "metadata": metadata or None,
                "attribution": attribution,
            }
        )
        # The backend derives the URL slug from the name on every update, so
        # fall back to the current name for metadata-only updates.
        if "name" not in service:
            service["name"] = self.retrieve(id).name

        endpoint = (
            f"/services/{id}/update/from-file"
            if spec_path or spec_url
            else f"/services/{id}"
        )
        request = self.client.put(endpoint, json={"service": service})
        return Service(**self._handle_response(request), _ouro=self.ouro)

    def delete(self, id: str) -> None:
        """Delete a Service and its routes by ID."""
        request = self.client.delete(f"/services/{id}")
        self._handle_response(request, raw=True)

    def retrieve(self, id: str) -> Service:
        """Retrieve a Service by its ID."""
        request = self.client.get(f"/services/{id}")
        return Service(**self._handle_response(request), _ouro=self.ouro)

    def list(self) -> List[Service]:
        """List all services in the current context."""
        request = self.client.get("/services")
        return [Service(**s, _ouro=self.ouro) for s in self._handle_response(request)]

    def read_spec(self, id: str) -> Dict:
        """Get the OpenAPI specification for a service."""
        request = self.client.get(f"/services/{id}/spec")
        return self._handle_response(request)

    def read_routes(self, id: str) -> List[Route]:
        """Get all routes for a service."""
        request = self.client.get(f"/services/{id}/routes")
        return [Route(**r, _ouro=self.ouro) for r in self._handle_response(request)]
