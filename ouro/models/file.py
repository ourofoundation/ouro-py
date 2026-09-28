from typing import List, Literal, Optional, Union
from uuid import UUID

from pydantic import Field

from ._base import OuroModel
from .asset import Asset


class FileData(OuroModel):
    url: str


class ZipArchiveMetadata(OuroModel):
    entry_count: int
    file_count: int
    directory_count: int
    total_uncompressed_size: int
    total_compressed_size: int
    preview_truncated: bool


class FileMetadata(OuroModel):
    name: str
    path: str
    size: int
    type: str
    bucket: Literal["public-files", "files"]
    id: Optional[UUID] = None
    full_path: Optional[str] = Field(default=None, alias="fullPath")
    extension: Optional[str] = None
    mime_type: Optional[str] = Field(default=None, alias="mimeType")
    archive: Optional[ZipArchiveMetadata] = None


class InProgressFileMetadata(OuroModel):
    type: str


class File(Asset):
    metadata: Optional[Union[FileMetadata, InProgressFileMetadata]] = Field(
        default=None,
        union_mode="left_to_right",
    )
    # CSV uploads store the first parsed rows here (same shape as Dataset.preview).
    preview: Optional[List[dict]] = None
    data: Optional[FileData] = None

    def share(
        self,
        user_id: Union[UUID, str],
        role: Literal["read", "write", "admin"] = "read",
    ) -> None:
        """Share this file with another user."""
        self._require_client().assets.share(str(self.id), user_id, role)

    def read_data(self) -> FileData:
        """Fetch a signed URL for this file's bytes and cache it on ``data``."""
        self.data = self._require_client().files.read_data(str(self.id))
        return self.data
