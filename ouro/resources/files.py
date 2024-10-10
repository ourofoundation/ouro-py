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
        file_path: str,
        name: str,
        visibility: str,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[str] = None,
    ) -> File:
        """
        Create a File
        """

        # Refresh supabase client
        self.ouro.supabase.auth.refresh_session()

        log.debug(f"Creating a file")
        # Update file with Supabase
        with open(file_path, "rb") as f:
            # Get file extension and MIME type
            mime_type = mimetypes.guess_type(file_path)[0]
            file_extension = os.path.splitext(file_path)[1]
            id = str(uuid.uuid4())

            bucket = "public-files" if visibility == "public" else "files"
            path_on_storage = f"{self.ouro.user.id}/{id}{file_extension}"

            print(f"Uploading file to {path_on_storage} in the {bucket} bucket")
            request = self.ouro.supabase.storage.from_(bucket).upload(
                file=f,
                path=path_on_storage,
                file_options={"content-type": mime_type},
            )
            request.raise_for_status()
            file = request.json()
            print(file)

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
            "name": f"{id}{file_extension}",
            "bucket": bucket,
            "path": path_on_storage,
            "type": mime_type,
            "mimeType": mime_type,
            **metadata,
        }
        preview = response["data"]["preview"]

        body = {
            "id": file_id,
            "name": name,
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            "description": description,
            "metadata": metadata,
            "preview": preview,
            "asset_type": "file",
        }

        # Filter out None values
        body = {k: v for k, v in body.items() if v is not None}

        request = self.client.post(
            "/datasets/create/from-file",
            json=body,
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
            f"/datasets/{id}",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        # Get the file data
        data_request = self.client.get(
            f"/datasets/{id}/data",
        )
        data_request.raise_for_status()
        data_response = data_request.json()
        if data_response["error"]:
            raise Exception(data_response["error"])

        # Combine the file asset and file data
        combined = response["data"]
        combined["data"] = data_response["data"]

        return File(**combined)
