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
    metadata: Optional[Union[FileMetadata, InProgressFileMetadata]] = Field(
        default=None,
        union_mode="left_to_right",
    )
    data: Optional[FileData] = None
    _ouro: Optional["Ouro"] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ouro = kwargs.get("_ouro")

    def _require_client(self) -> "Ouro":
        if not self._ouro:
            raise RuntimeError("File object not connected to Ouro client")
        return self._ouro

    def share(
        self,
        user_id: Union[UUID, str],
        role: Literal["read", "write", "admin"] = "read",
    ) -> None:
        """Share this file with another user.

        Args:
            user_id: The UUID of the user to share with.
            role: The role to grant the user (read, write, admin).
        """
        ouro = self._require_client()
        ouro.files.share(str(self.id), user_id, role)

    def read_data(self) -> FileData:
        """Get the file data (signed URL).

        Returns:
            FileData with url property for this file.
        """
        ouro = self._require_client()
        from ouro._resource import SyncAPIResource

        resource = SyncAPIResource(ouro)
        result = resource._handle_response(ouro.client.get(f"/files/{self.id}/data"))

        self.data = FileData(**result)
        return self.data
