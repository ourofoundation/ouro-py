from __future__ import annotations

import logging
import mimetypes
import os
import uuid
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
) -> dict:
    """Merge local upload info with server-extracted metadata."""
    return {
        "id": file_id,
        "name": file_name,
        "bucket": bucket,
        "path": path_on_storage,
        "type": mime_type,
        "mimeType": mime_type,
        **server_metadata,
    }


class Files(SyncAPIResource):
    def create(
        self,
        name: str,
        visibility: str,
        file_path: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[Union[str, "Content"]] = None,
        **kwargs,
    ) -> File:
        """Create a File."""
        log.debug("Creating a file")
        if not file_path:
            log.warning("No file path provided, creating a file stub. Update it later.")
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
            id = str(uuid.uuid4())
            with open(file_path, "rb") as f:
                mime_type = mimetypes.guess_type(file_path)[0]
                file_extension = os.path.splitext(file_path)[1]

                bucket = "public-files" if visibility == "public" else "files"
                bucket_folder = f"{self.ouro.user.id}"
                file_name = f"{id}{file_extension}"
                path_on_storage = f"{bucket_folder}/{file_name}"

                log.info(f"Uploading file to {path_on_storage} in the {bucket} bucket")
                self.ouro.supabase.storage.from_(bucket).upload(
                    file=f,
                    path=path_on_storage,
                    file_options={"content-type": mime_type},
                )

                response = self.ouro.supabase.storage.from_(bucket).list(
                    bucket_folder,
                    {
                        "limit": 1,
                        "offset": 0,
                        "sortBy": {"column": "name", "order": "desc"},
                        "search": id,
                    },
                )
                file = response[0]
                if file["name"] != file_name:
                    raise RuntimeError(
                        f"Upload verification failed: expected '{file_name}', "
                        f"got '{file['name']}'"
                    )

            file_id = file["id"]
            meta_data = self._handle_response(
                self.client.get(f"/files/{file_id}/metadata")
            )

            metadata = _build_file_metadata(
                file_id, file_name, bucket, path_on_storage,
                mime_type, meta_data["metadata"],
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
                "preview": meta_data["preview"],
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
        name: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        visibility: Optional[str] = None,
        **kwargs,
    ) -> File:
        """Update a file by ID.

        Pass file_path to replace the file data with a new file from the local filesystem.
        Pass name, description, or visibility to update metadata.
        """
        log.debug("Updating a file")

        update_params = _strip_none({
            "name": name,
            "description": _coerce_description(description),
            "visibility": visibility,
        })
        update_params.update(kwargs)

        file: dict = {"id": str(id), **update_params}
        existing = self.retrieve(id)

        if file_path:
            with open(file_path, "rb") as f:
                mime_type = mimetypes.guess_type(file_path)[0]
                file_extension = os.path.splitext(file_path)[1]

                bucket = "public-files" if existing.visibility == "public" else "files"
                path_on_storage = f"{self.ouro.user.id}/{id}{file_extension}"

                log.info(f"Uploading file to {path_on_storage} in the {bucket} bucket")
                request = self.ouro.supabase.storage.from_(bucket).upload(
                    file=f,
                    path=path_on_storage,
                    file_options={"content-type": mime_type},
                )
                file_data = request.json()

            file_id = file_data["Id"]
            meta_info = self._handle_response(
                self.client.get(f"/files/{file_id}/metadata")
            )

            metadata = _build_file_metadata(
                file_id, f"{id}{file_extension}", bucket, path_on_storage,
                mime_type, meta_info["metadata"],
            )

            file = {
                **file,
                "metadata": metadata,
                "preview": meta_info["preview"],
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
