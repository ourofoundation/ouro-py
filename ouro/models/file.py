import json
from typing import TYPE_CHECKING, Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field

from .asset import Asset

if TYPE_CHECKING:
    from ouro import Ouro


class FileData(BaseModel):
    url: str


class FileMetadata(BaseModel):
    name: str
    path: str
    size: int
    type: str
    bucket: Literal["public-files", "files"]
    id: Optional[UUID] = None
    fullPath: Optional[str] = None


class InProgressFileMetadata(BaseModel):
    type: str


class File(Asset):
    metadata: Union[FileMetadata, InProgressFileMetadata] = Field(
        union_mode="left_to_right",
        # discriminator="state",
    )
    data: Optional[FileData]
    _ouro: Optional["Ouro"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ouro = kwargs.get("_ouro")

    def share(
        self, user_id: UUID | str, role: Literal["read", "write", "admin"] = "read"
    ) -> None:
        """Share this file with another user. You must be an admin of the file to share.

        Args:
            user_id: The UUID of the user to share with
            role: The role to grant the user (read, write, admin)
        """
        if not self._ouro:
            raise RuntimeError("File object not connected to Ouro client")
        self._ouro.files.share(str(self.id), user_id, role)

    def read_data(self) -> FileData:
        """Get the file data.

        Returns:
            FileData with url property for this file
        """
        if not self._ouro:
            raise RuntimeError("File object not connected to Ouro client")
        request = self._ouro.client.get(
            f"/files/{self.id}/data",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(json.dumps(response["error"]))

        self.data = FileData(**response["data"])
        return self.data
