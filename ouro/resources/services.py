from __future__ import annotations

import logging
from typing import Dict, List, Optional, Union

from ouro._resource import (
    SyncAPIResource,
    _attribution_payload,
    _coerce_description,
    _ensure_attribution,
    _optional_attribution,
    _optional_attribution_payload,
    _strip_none,
)
from ouro.models import DeleteResult, Route, Service, ServiceAuthentication

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
        price_currency: Optional[str] = None,
        license_id: str = "MIT",
        attribution: Optional[dict] = None,
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
        if attribution is not None:
            attribution = _ensure_attribution(attribution)
        else:
            attribution = _attribution_payload(
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
                "price_currency": price_currency,
                "license_id": license_id,
                **kwargs,
                "source": "api",
                "asset_type": "service",
                "metadata": metadata,
                "attribution": attribution,
            }
        )

        service = self._scope_create(service)

        endpoint = (
            "/services/create/from-file"
            if spec_path or spec_url
            else "/services/create/from-form"
        )
        request = self.client.post(endpoint, json={"service": service})
        return self._parse(Service, self._handle_response(request))

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
        price_currency: Optional[str] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        originality: Optional[str] = None,
        github_url: Optional[str] = None,
        paper_url: Optional[str] = None,
        doi_url: Optional[str] = None,
        external_url: Optional[str] = None,
        relation_type: Optional[str] = None,
        refresh_spec: bool = False,
        **kwargs,
    ) -> Service:
        """Update a Service by its ID.

        Service config fields (``base_url``, ``authentication``, …) merge into
        ``metadata``. Provenance fields merge into ``attribution``. Providing
        ``spec_path`` or ``spec_url`` re-parses the OpenAPI spec and syncs routes.
        Set ``refresh_spec=True`` to re-fetch the service's stored remote
        ``spec_url`` without having to pass it again.
        """
        current = None
        if refresh_spec and spec_path is None and spec_url is None:
            current = self.retrieve(id)
            spec_url = current.metadata.spec_url if current.metadata else None
            if not spec_url:
                raise ValueError(
                    "refresh_spec=True requires the service to have a stored spec_url"
                )

        metadata = _service_metadata(
            base_url=base_url,
            authentication=authentication,
            version=version,
            spec_path=spec_path,
            spec_url=spec_url,
            auth_url=auth_url,
        )
        if attribution is not None:
            attribution = _optional_attribution(attribution)
        else:
            attribution = _optional_attribution_payload(
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
                "price_currency": price_currency,
                "license_id": license_id,
                **kwargs,
                "metadata": metadata or None,
                "attribution": attribution,
            }
        )
        # The backend derives the URL slug from the name on every update, so
        # fall back to the current name for metadata-only updates.
        if "name" not in service:
            current = current or self.retrieve(id)
            service["name"] = current.name

        endpoint = (
            f"/services/{id}/update/from-file"
            if spec_path or spec_url
            else f"/services/{id}"
        )
        self._scope_update(service)
        request = self.client.put(endpoint, json={"service": service})
        return self._parse(Service, self._handle_response(request))

    def delete(
        self, id: str, *, delete_children: bool = True, dry_run: bool = False
    ) -> DeleteResult:
        """Delete a Service by ID.

        Args:
            id: Service UUID.
            delete_children: When True (default), also delete child routes.
                Services with routes require this to be True.
            dry_run: When True, return the delete summary without deleting.

        Returns:
            What was deleted, or would be when ``dry_run`` is true.
        """
        return self._delete(
            f"/services/{id}", delete_children=delete_children, dry_run=dry_run
        )

    def retrieve(self, id: str) -> Service:
        """Retrieve a Service by its ID."""
        request = self.client.get(f"/services/{id}")
        return self._parse(Service, self._handle_response(request))

    def list(self) -> List[Service]:
        """List all services in the current context."""
        request = self.client.get("/services")
        return self._parse_list(Service, self._handle_response(request))

    def read_spec(self, id: str) -> Dict:
        """Get the OpenAPI specification for a service."""
        request = self.client.get(f"/services/{id}/spec")
        return self._handle_response(request)

    def read_routes(self, id: str) -> List[Route]:
        """Get all routes for a service."""
        request = self.client.get(f"/services/{id}/routes")
        return self._parse_list(Route, self._handle_response(request))

    def set_authentication(
        self,
        id: str,
        secret: str,
        method: str = "Ouro",
    ) -> ServiceAuthentication:
        """Upsert the service owner's authentication secret (idempotent).

        Used for ``authentication="Ouro"`` (Basic token) and
        ``"Personal Access Token"`` services. Only the service owner may call
        this. ``rotated`` is False when the plaintext already matched.
        """
        request = self.client.put(
            f"/services/{id}/authentication",
            json={"method": method, "secret": secret},
        )
        return self._parse(ServiceAuthentication, self._handle_response(request))
