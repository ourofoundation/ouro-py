from __future__ import annotations

import logging
import mimetypes
import os
from base64 import b64encode
from typing import Any, List, Literal, Optional, Union
from uuid import UUID

import httpx

from ouro._exceptions import APIConnectionError, APIStatusError
from ouro._resource import (
    SyncAPIResource,
    _coerce_description,
    _ensure_attribution,
    _optional_attribution,
    _strip_none,
)
from ouro.models import DeleteResult, File, FileData, Page

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Files"]


def _build_file_metadata(
    file_id: str,
    file_name: str,
    bucket: str,
    path_on_storage: str,
    mime_type: str | None,
    server_metadata: dict,
    file_size: int,
) -> dict:
    """Merge local upload info with server-extracted metadata.

    The backend metadata endpoint can return partial values for some file types.
    Ensure required file metadata fields always exist for CreateFileSchema.
    """
    resolved_type = (
        server_metadata.get("type")
        or server_metadata.get("mimeType")
        or server_metadata.get("mime_type")
        or mime_type
        or "application/octet-stream"
    )

    resolved_extension = server_metadata.get("extension")
    if not resolved_extension:
        extension = os.path.splitext(file_name)[1] or os.path.splitext(path_on_storage)[1]
        if extension.startswith("."):
            extension = extension[1:]
        resolved_extension = extension or None
    if not resolved_extension and resolved_type:
        guessed_extension = mimetypes.guess_extension(resolved_type)
        if guessed_extension:
            resolved_extension = guessed_extension.lstrip(".")
    if not resolved_extension:
        resolved_extension = "bin"

    resolved_size = server_metadata.get("size")
    if resolved_size is None:
        resolved_size = server_metadata.get("contentLength")
    if resolved_size is None:
        resolved_size = file_size
    try:
        resolved_size = int(resolved_size)
    except (TypeError, ValueError):
        resolved_size = file_size

    return {
        **server_metadata,
        "id": file_id,
        "name": file_name,
        "bucket": bucket,
        "path": path_on_storage,
        "type": resolved_type,
        "mimeType": (
            server_metadata.get("mimeType")
            or server_metadata.get("mime_type")
            or mime_type
            or resolved_type
        ),
        "extension": resolved_extension,
        "size": resolved_size,
    }


def _resolve_content_type(
    file_name: str | None,
    content_type: str | None = None,
) -> str:
    """Return a storage-safe MIME type, never None.

    Python's ``mimetypes.guess_type`` returns None for unknown extensions
    (``.cif`` among them). Supabase storage then rejects the upload with
    ``Invalid Content-Type header``.
    """
    if content_type:
        return content_type
    if file_name:
        guessed = mimetypes.guess_type(file_name)[0]
        if guessed:
            return guessed
    return "application/octet-stream"


def _infer_partial_metadata(
    file_name: str,
    content_type: str | None,
    size: int,
) -> tuple[str, str, dict]:
    """Return (resolved_content_type, extension, metadata) for a partial payload."""
    resolved_type = _resolve_content_type(file_name, content_type)

    ext = os.path.splitext(file_name)[1]
    if ext.startswith("."):
        ext = ext[1:]
    if not ext:
        guessed = mimetypes.guess_extension(resolved_type)
        ext = guessed.lstrip(".") if guessed else "bin"

    metadata = {
        "name": file_name,
        "type": resolved_type,
        "extension": ext,
        "size": size,
    }
    return resolved_type, ext, metadata


def _normalize_extension(
    extension: Optional[Union[str, List[str]]],
) -> Optional[Union[str, List[str]]]:
    """Strip leading dots from extension filter values."""
    if extension is None:
        return None
    if isinstance(extension, list):
        cleaned = [ext.lstrip(".").lower() for ext in extension if ext]
        return cleaned or None
    cleaned = extension.lstrip(".").lower()
    return cleaned or None


def _merge_file_metadata_filters(
    *,
    extension: Optional[Union[str, List[str]]] = None,
    file_type: Optional[str] = None,
    metadata_filters: Optional[dict] = None,
) -> Optional[dict]:
    """Merge first-class file filters into ``metadata_filters`` for assets.search."""
    merged: dict = dict(metadata_filters or {})
    normalized = _normalize_extension(extension)
    if normalized is not None:
        merged["extension"] = normalized
    if file_type:
        merged["file_type"] = file_type
    return merged or None


class Files(SyncAPIResource):
    @staticmethod
    def partial_from_bytes(
        content: bytes,
        file_name: str,
        *,
        name: str,
        description: str = "",
        content_type: str | None = None,
    ) -> dict:
        """Build a backend-compatible partial file payload from raw bytes.

        The returned dict is passed to ``Editor.new_partial_asset()`` so the
        backend can materialise the file when the post is saved.  MIME type
        and extension are inferred from *file_name* when *content_type* is
        omitted.

        >>> partial = ouro.files.partial_from_bytes(
        ...     b"<html>...</html>", "report.html",
        ...     name="Energy curve", description="Energy vs. step",
        ... )
        """
        _, _, metadata = _infer_partial_metadata(file_name, content_type, len(content))
        return {
            "asset_type": "file",
            "name": name,
            "description": description,
            "metadata": metadata,
            "base64": b64encode(content).decode("ascii"),
        }

    @staticmethod
    def partial_from_file(
        file_path: str | os.PathLike,
        *,
        name: str,
        description: str = "",
        content_type: str | None = None,
    ) -> dict:
        """Build a backend-compatible partial file payload from a local file.

        Reads the file at *file_path*, infers the MIME type from the
        extension (unless *content_type* is given), and delegates to
        :meth:`partial_from_bytes`.

        >>> partial = ouro.files.partial_from_file(
        ...     "/tmp/report.html",
        ...     name="Energy curve", description="Energy vs. step",
        ... )
        """
        path = os.fspath(file_path)
        with open(path, "rb") as f:
            content = f.read()
        return Files.partial_from_bytes(
            content,
            os.path.basename(path),
            name=name,
            description=description,
            content_type=content_type,
        )

    def _upload_content(
        self,
        content: bytes,
        file_name: str,
        visibility: str,
        mime_type: str,
    ) -> dict:
        """Upload raw bytes to Ouro's file storage."""
        file_base64 = b64encode(content).decode("ascii")
        upload = self.client.post(
            "/files/upload",
            json={
                "file_name": file_name,
                "file_base64": file_base64,
                "visibility": visibility,
                "content_type": mime_type,
            },
        )
        payload = self._handle_response(upload, raw=True) or {}
        data = payload.get("data") or {}
        if not data.get("id"):
            raise RuntimeError("Upload failed: missing file object id")
        return data

    def _upload_local_file(
        self,
        file_path: str,
        visibility: str,
        mime_type: str,
    ) -> dict:
        with open(file_path, "rb") as f:
            content = f.read()
        return self._upload_content(
            content, os.path.basename(file_path), visibility, mime_type,
        )

    def list(
        self,
        query: str = "",
        limit: int = 20,
        offset: int = 0,
        scope: Optional[str] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        sort: Optional[str] = None,
        time_window: Optional[str] = None,
        extension: Optional[Union[str, List[str]]] = None,
        file_type: Optional[str] = None,
        **kwargs: Any,
    ) -> Page[File]:
        """List files, optionally filtered by search query and scope.

        Prefer :meth:`search` for the full set of file-specific filters.

        Args:
            sort: "relevant" | "recent" | "popular" | "updated"
            time_window: For sort="popular": "day" | "week" | "month" | "all".
                         Default: "month".
            extension: File extension filter, e.g. ``"cif"`` or ``["cif", "xyz"]``.
            file_type: File category filter: ``"image"`` | ``"video"`` | ``"audio"`` | ``"pdf"``.
        """
        return self.search(
            query=query,
            limit=limit,
            offset=offset,
            scope=scope,
            org_id=org_id,
            team_id=team_id,
            sort=sort,
            time_window=time_window,
            extension=extension,
            file_type=file_type,
            **kwargs,
        )

    def search(
        self,
        query: str = "",
        *,
        extension: Optional[Union[str, List[str]]] = None,
        file_type: Optional[str] = None,
        limit: Optional[int] = 20,
        offset: int = 0,
        scope: Optional[str] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        user_id: Optional[str] = None,
        visibility: Optional[str] = None,
        sort: Optional[str] = None,
        time_window: Optional[str] = None,
        metadata_filters: Optional[dict] = None,
        **kwargs: Any,
    ) -> Page[File]:
        """Search or browse file assets with file-specific filters.

        Always scopes to ``asset_type="file"``. Use ``extension`` to find CIFs
        and other typed files without hand-building ``metadata_filters``.

        Pagination is transparent: ``limit`` values above the 200-per-request
        server cap — or ``limit=None`` for *all* matches — are fulfilled by
        paginating internally. Use browse mode (empty ``query``) for exhaustive
        collection; semantic search caps its candidate pool.

        Examples::

            # Every CIF file visible to you
            cifs = ouro.files.search(extension="cif", scope="all", limit=None)

            # First page of a team
            page = ouro.files.search(
                extension="cif", team_id=pm_team_id, scope="all", limit=100
            )
            for file in page: ...
            if page.has_more: ...

        Args:
            query: Hybrid search query, or empty to browse by recency.
            extension: File extension without a leading dot, e.g. ``"cif"``,
                ``"csv"``. Pass a list to match any of several extensions.
            file_type: Category filter: ``"image"`` | ``"video"`` | ``"audio"`` | ``"pdf"``.
            limit: Max results (default 20). Values above 200 paginate
                internally; ``None`` fetches all matches.
            offset: Pagination offset.
            scope: ``"personal"`` | ``"org"`` | ``"global"`` | ``"all"``.
            org_id / team_id / user_id / visibility: Standard asset filters.
            sort: ``"relevant"`` | ``"recent"`` | ``"popular"`` | ``"updated"``.
            time_window: For ``sort="popular"``: ``"day"`` | ``"week"`` | ``"month"`` | ``"all"``.
            metadata_filters: Extra metadata key/value filters (merged with
                ``extension`` / ``file_type``; those kwargs win on conflict).
        """
        merged = _merge_file_metadata_filters(
            extension=extension,
            file_type=file_type,
            metadata_filters=metadata_filters,
        )
        # Do not let callers override asset_type away from file.
        kwargs.pop("asset_type", None)

        # assets.search packs any present filter keys into JSON — omit Nones so
        # we don't send {"org_id": null, ...} which the API treats as no matches.
        search_kwargs: dict[str, Any] = {
            "asset_type": "file",
            "limit": limit,
            "offset": offset,
            **kwargs,
        }
        if scope is not None:
            search_kwargs["scope"] = scope
        if org_id is not None:
            search_kwargs["org_id"] = org_id
        if team_id is not None:
            search_kwargs["team_id"] = team_id
        if user_id is not None:
            search_kwargs["user_id"] = user_id
        if visibility is not None:
            search_kwargs["visibility"] = visibility
        if sort is not None:
            search_kwargs["sort"] = sort
        if time_window is not None:
            search_kwargs["time_window"] = time_window
        if merged is not None:
            search_kwargs["metadata_filters"] = merged

        return self.ouro.assets._search(File, query=query, **search_kwargs)

    def create(
        self,
        name: str,
        visibility: str,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None,
        file_name: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        price_currency: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> File:
        """Create a File.

        Provide file data via *one* of:
        - ``file_path`` — path to a local file.
        - ``file_content`` + ``file_name`` — raw bytes and the original
          filename (with extension, e.g. ``"report.pdf"``).
        """
        log.debug("Creating a file")
        if file_path and file_content is not None:
            raise ValueError("Provide file_path or file_content, not both.")
        if not file_path and file_content is None:
            raise ValueError("Provide file_path, or file_content with file_name.")
        if file_content is not None and not file_name:
            raise ValueError("file_name is required when using file_content.")

        if file_path:
            mime_type = _resolve_content_type(file_path)
            local_file_size = os.path.getsize(file_path)
            upload_data = self._upload_local_file(file_path, visibility, mime_type)
        else:
            mime_type = _resolve_content_type(file_name)
            local_file_size = len(file_content)
            upload_data = self._upload_content(
                file_content, file_name, visibility, mime_type,
            )

        file_id = upload_data["id"]
        bucket = upload_data["bucket"]
        path_on_storage = upload_data["path"]
        storage_name = os.path.basename(path_on_storage)
        meta_data = self._handle_response(
            self.client.get(f"/files/{file_id}/metadata")
        )
        server_metadata = (meta_data or {}).get("metadata") or {}

        metadata = _build_file_metadata(
            file_id, storage_name, bucket, path_on_storage,
            mime_type, server_metadata, local_file_size,
        )

        file = {
            "id": file_id,
            "name": name,
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            "price_currency": price_currency,
            "description": _coerce_description(description),
            "license_id": license_id,
            **kwargs,
            "source": "api",
            "metadata": metadata,
            "preview": (meta_data or {}).get("preview"),
            "asset_type": "file",
        }

        file = _strip_none(file)
        file["attribution"] = _ensure_attribution(attribution)

        request = self.client.post("/files/create", json={"file": file})
        return self._parse(File, self._handle_response(request))

    def retrieve(self, id: str, *, include_data: bool = True) -> File:
        """Retrieve a File by its ID.

        Args:
            id: The File's UUID.
            include_data: When ``True`` (default), also fetch ``/files/{id}/data``
                and attach it as ``File.data``. Set to ``False`` to skip the
                second request (e.g. for metadata-only lookups, or large binary
                files where the data endpoint is expensive).

                If the data fetch itself fails with a 4xx/5xx or a transient
                transport error, ``File.data`` is set to ``None`` and a warning
                is logged; non-HTTP exceptions (bugs, cancellations) propagate
                as before.
        """
        file = self._parse(File, self._handle_response(self.client.get(f"/files/{id}")))
        file.data = None
        if include_data:
            try:
                file.data = self.read_data(id)
            except (APIStatusError, APIConnectionError, httpx.HTTPError) as exc:
                log.warning(
                    "Failed to fetch /files/%s/data (%s); returning File "
                    "without .data. Use include_data=False to skip this "
                    "request entirely.",
                    id,
                    exc.__class__.__name__,
                )
        return file

    def read_data(self, id: str) -> FileData:
        """Fetch a signed download URL for a file's bytes."""
        request = self.client.get(f"/files/{id}/data")
        return self._parse(FileData, self._handle_response(request))

    def update(
        self,
        id: str,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None,
        file_name: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        price_currency: Optional[str] = None,
        license_id: Optional[str] = None,
        attribution: Optional[dict] = None,
        **kwargs,
    ) -> File:
        """Update a file by ID.

        Pass *one* of ``file_path`` or ``file_content`` + ``file_name`` to
        replace the file data in place (same storage path). Pass name,
        description, visibility, or pricing to update metadata.
        """
        log.debug("Updating a file")
        if file_path and file_content is not None:
            raise ValueError("Provide file_path or file_content, not both.")
        if file_content is not None and not file_name:
            raise ValueError("file_name is required when using file_content.")

        update_params = _strip_none({
            "name": name,
            "description": _coerce_description(description),
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            "price_currency": price_currency,
            "license_id": license_id,
            "attribution": _optional_attribution(attribution),
        })
        update_params.update(kwargs)

        has_upload = bool(file_path) or file_content is not None
        if has_upload:
            if file_path:
                resolved_name = file_name or os.path.basename(file_path)
                mime_type = _resolve_content_type(resolved_name)
                with open(file_path, "rb") as f:
                    content = f.read()
            else:
                resolved_name = file_name
                mime_type = _resolve_content_type(resolved_name)
                content = file_content

            body: dict[str, Any] = {
                "file_name": resolved_name,
                "file_base64": b64encode(content).decode("ascii"),
                "content_type": mime_type,
            }
            if update_params:
                body["file"] = {"id": str(id), **update_params}

            request = self.client.put(f"/files/{id}/content", json=body)
            return self._parse(File, self._handle_response(request))

        file = _strip_none({"id": str(id), **update_params})
        request = self.client.put(f"/files/{id}", json={"file": file})
        return self._parse(File, self._handle_response(request))

    def delete(
        self, id: str, *, delete_children: bool = False, dry_run: bool = False
    ) -> DeleteResult:
        """Delete a file.

        Args:
            id: File UUID.
            delete_children: When True, also delete child assets linked via
                ``parent_id``.
            dry_run: When True, return the delete summary without deleting.

        Returns:
            What was deleted, or would be when ``dry_run`` is true.
        """
        return self._delete(
            f"/files/{id}", delete_children=delete_children, dry_run=dry_run
        )

    def share(
        self,
        file_id: str,
        user_id: Union[UUID, str],
        role: Literal["read", "write", "admin"] = "read",
    ) -> None:
        """Share a file with another user."""
        self.ouro.assets.share(file_id, user_id, role=role)
