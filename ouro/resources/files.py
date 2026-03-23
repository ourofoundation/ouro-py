from __future__ import annotations

import logging
import mimetypes
import os
import uuid
from base64 import b64encode
from typing import Literal, Optional, Union

from ouro._resource import SyncAPIResource, _coerce_description, _strip_none
from ouro.models import File

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


class Files(SyncAPIResource):
    def _upload_content(
        self,
        content: bytes,
        file_name: str,
        visibility: str,
        mime_type: str | None,
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
        mime_type: str | None,
    ) -> dict:
        with open(file_path, "rb") as f:
            content = f.read()
        return self._upload_content(
            content, os.path.basename(file_path), visibility, mime_type,
        )

    def create(
        self,
        name: str,
        visibility: str,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None,
        file_name: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[Union[str, "Content"]] = None,
        **kwargs,
    ) -> File:
        """Create a File.

        Provide file data via *one* of:
        - ``file_path`` — absolute path to a local file.
        - ``file_content`` + ``file_name`` — raw bytes and the original
          filename (with extension, e.g. ``"report.pdf"``).
        - Neither — creates an in-progress stub to be updated later.
        """
        log.debug("Creating a file")
        if file_path and file_content is not None:
            raise ValueError("Provide file_path or file_content, not both.")
        if file_content is not None and not file_name:
            raise ValueError("file_name is required when using file_content.")

        has_upload = bool(file_path) or file_content is not None

        if not has_upload:
            log.warning("No file data provided, creating a file stub. Update it later.")
            file = {
                "id": str(uuid.uuid4()),
                "name": name,
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                "description": _coerce_description(description),
                **kwargs,
                "asset_type": "file",
                "state": "in-progress",
                "source": "api",
            }
        else:
            if file_path:
                mime_type = mimetypes.guess_type(file_path)[0]
                local_file_size = os.path.getsize(file_path)
                upload_data = self._upload_local_file(file_path, visibility, mime_type)
            else:
                mime_type = mimetypes.guess_type(file_name)[0]
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
                "description": _coerce_description(description),
                **kwargs,
                "source": "api",
                "metadata": metadata,
                "preview": (meta_data or {}).get("preview"),
                "asset_type": "file",
            }

        file = _strip_none(file)

        request = self.client.post("/files/create", json={"file": file})
        data = self._handle_response(request)
        return File(**data, _ouro=self.ouro)

    def retrieve(self, id: str) -> File:
        """Retrieve a File by its ID."""
        data = self._handle_response(self.client.get(f"/files/{id}"))

        try:
            file_data = self._handle_response(self.client.get(f"/files/{id}/data"))
        except Exception:
            file_data = None

        data["data"] = file_data
        return File(**data, _ouro=self.ouro)

    def update(
        self,
        id: str,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None,
        file_name: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        **kwargs,
    ) -> File:
        """Update a file by ID.

        Pass *one* of ``file_path`` or ``file_content`` + ``file_name`` to
        replace the file data.  Pass name, description, or visibility to
        update metadata.
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
        })
        update_params.update(kwargs)

        file: dict = {"id": str(id), **update_params}
        existing = self.retrieve(id)

        has_upload = bool(file_path) or file_content is not None
        if has_upload:
            visibility_for_upload = visibility or existing.visibility

            if file_path:
                mime_type = mimetypes.guess_type(file_path)[0]
                local_file_size = os.path.getsize(file_path)
                upload_data = self._upload_local_file(
                    file_path, visibility_for_upload, mime_type,
                )
            else:
                mime_type = mimetypes.guess_type(file_name)[0]
                local_file_size = len(file_content)
                upload_data = self._upload_content(
                    file_content, file_name, visibility_for_upload, mime_type,
                )

            file_id = upload_data["id"]
            bucket = upload_data["bucket"]
            path_on_storage = upload_data["path"]
            storage_name = os.path.basename(path_on_storage)
            meta_info = self._handle_response(
                self.client.get(f"/files/{file_id}/metadata")
            )
            server_metadata = (meta_info or {}).get("metadata") or {}

            metadata = _build_file_metadata(
                file_id, storage_name, bucket, path_on_storage,
                mime_type, server_metadata, local_file_size,
            )

            file = {
                **file,
                "metadata": metadata,
                "preview": (meta_info or {}).get("preview"),
                "asset_type": "file",
            }

        file = _strip_none(file)

        request = self.client.put(f"/files/{id}", json={"file": file})
        data = self._handle_response(request)
        return File(**data, data=None, _ouro=self.ouro)

    def delete(self, id: str) -> None:
        """Delete a file."""
        request = self.client.delete(f"/files/{id}")
        self._handle_response(request)

    def share(
        self,
        file_id: str,
        user_id: Union[uuid.UUID, str],
        role: Literal["read", "write", "admin"] = "read",
    ) -> None:
        """Share a file with another user."""
        request = self.client.put(
            f"/elements/common/{file_id}/share",
            json={"permission": {"user": {"user_id": str(user_id)}, "role": role}},
        )
        self._handle_response(request)
