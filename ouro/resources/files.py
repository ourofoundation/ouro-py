import json
import logging
import mimetypes
import os
import uuid
from typing import List, Optional

from ouro._resource import SyncAPIResource
from ouro.models import File

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Files"]


class Files(SyncAPIResource):
    def create(
        self,
        name: str,
        visibility: str,
        file_path: Optional[str] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[str] = None,
        **kwargs,
    ) -> File:
        """
        Create a File
        """

        log.debug(f"Creating a file")

        if not file_path:
            # We're making a file stub to be updated later
            file = {
                "id": str(uuid.uuid4()),
                "name": name,
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                "description": description,
                "asset_type": "file",
                "state": "in-progress",
                **kwargs,
            }
        else:
            # Update file with Supabase
            with open(file_path, "rb") as f:
                # Get file extension and MIME type
                mime_type = mimetypes.guess_type(file_path)[0]
                file_extension = os.path.splitext(file_path)[1]
                id = str(uuid.uuid4())

                bucket = "public-files" if visibility == "public" else "files"
                path_on_storage = f"{self.ouro.user.id}/{id}{file_extension}"

                log.info(f"Uploading file to {path_on_storage} in the {bucket} bucket")
                request = self.ouro.supabase.storage.from_(bucket).upload(
                    file=f,
                    path=path_on_storage,
                    file_options={"content-type": mime_type},
                )
                request.raise_for_status()
                file = request.json()

            # Not sure why it's cased like this
            file_id = file["Id"]
            # Get file details server-side
            request = self.client.get(
                f"/files/{file_id}/metadata",
            )
            request.raise_for_status()
            response = request.json()

            metadata = response["data"]["metadata"]
            metadata = {
                "id": file_id,
                "name": f"{id}{file_extension}",
                "bucket": bucket,
                "path": path_on_storage,
                "type": mime_type,
                "mimeType": mime_type,
                **metadata,
            }
            preview = response["data"]["preview"]

            file = {
                "id": file_id,  # this doesn't need to be the same as the file object id
                "name": name,
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                "description": description,
                "metadata": metadata,
                "preview": preview,
                "asset_type": "file",
            }

        # Filter out None values in the file body
        file = {k: v for k, v in file.items() if v is not None}

        request = self.client.post(
            "/files/create",
            json={"file": file},
        )
        request.raise_for_status()
        response = request.json()
        log.info(response)
        if response["error"]:
            raise Exception(json.dumps(response["error"]))
        return File(**response["data"])

    def retrieve(self, id: str) -> File:
        """
        Retrieve a File by its ID
        """
        request = self.client.get(
            f"/files/{id}",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        # Get the file data
        data_request = self.client.get(
            f"/files/{id}/data",
        )
        data_request.raise_for_status()
        data_response = data_request.json()
        # Don't fail if the file is still in progress
        # if data_response["error"]:
        #     raise Exception(data_response["error"])

        # Combine the file asset and file data
        combined = response["data"]
        combined["data"] = data_response["data"]
        return File(**combined)

    def update(
        self,
        id: str,
        file_path: Optional[str] = None,
        **kwargs,
    ) -> File:
        """
        Create a File
        """

        log.debug(f"Updating a file")

        # Build the file body
        file = {
            "id": str(id),
            **kwargs,
        }
        # Load existing file data
        existing = self.retrieve(id)

        if file_path:
            # Update file with Supabase
            with open(file_path, "rb") as f:
                # Get file extension and MIME type
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
                request.raise_for_status()
                file_data = request.json()

            # Not sure why it's cased like this
            file_id = file_data["Id"]
            # Get file details server-side
            request = self.client.get(
                f"/files/{file_id}/metadata",
            )
            request.raise_for_status()
            response = request.json()

            metadata = response["data"]["metadata"]
            metadata = {
                "id": file_id,
                "name": f"{id}{file_extension}",
                "bucket": bucket,
                "path": path_on_storage,
                "type": mime_type,
                **metadata,
            }
            preview = response["data"]["preview"]

            file = {
                **file,
                "metadata": metadata,
                "preview": preview,
                "asset_type": "file",
            }

        # Filter out None values in the file body
        file = {k: v for k, v in file.items() if v is not None}

        request = self.client.put(
            f"/files/{id}",
            json={"file": file},
        )
        request.raise_for_status()
        response = request.json()
        log.info(response)
        if response["error"]:
            raise Exception(json.dumps(response["error"]))
        return File(**response["data"])
