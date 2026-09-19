from datetime import datetime
from typing import Annotated, Any, Dict, Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

AssetRenditionKind = Literal["translation", "speech"]
AssetRenditionStatus = Literal["queued", "in-progress", "timed-out", "success", "error"]
AssetRenditionCacheScope = Literal["none", "user", "shared"]


class RenditionContent(BaseModel):
    """Structured TipTap content and its plain-text rendition."""

    data: Dict[str, Any] = Field(alias="json")
    text: str


class AssetRendition(BaseModel):
    """Common persisted record for a rendition of a post or comment."""

    model_config = ConfigDict(extra="allow")

    id: UUID
    asset_id: UUID
    route_id: UUID
    provider_id: UUID
    action_id: Optional[UUID] = None
    kind: AssetRenditionKind
    status: AssetRenditionStatus
    source_content_hash: Optional[str] = None
    configuration_hash: Optional[str] = None
    cache_scope: Optional[AssetRenditionCacheScope] = None
    cache_version: Optional[str] = None
    error: Optional[Any] = None
    created_at: datetime
    last_updated: datetime


class TranslationAssetRendition(AssetRendition):
    kind: Literal["translation"] = "translation"
    source_language: Optional[str] = None
    target_language: str
    content: Optional[RenditionContent] = None


class SpeechAudio(BaseModel):
    """Stored audio metadata plus an optional permission-checked playback URL."""

    model_config = ConfigDict(extra="allow")

    storage_path: Optional[str] = None
    url: Optional[str] = None
    content_type: Optional[str] = None
    duration_seconds: Optional[float] = None
    byte_size: Optional[int] = None


class SpeechAssetRendition(AssetRendition):
    kind: Literal["speech"] = "speech"
    language: str
    voice: Optional[str] = None
    audio: Optional[SpeechAudio] = None
    metadata: Optional[Dict[str, Any]] = None


AssetRenditionRecord = Annotated[
    Union[TranslationAssetRendition, SpeechAssetRendition],
    Field(discriminator="kind"),
]


__all__ = [
    "AssetRenditionKind",
    "AssetRenditionStatus",
    "AssetRenditionCacheScope",
    "RenditionContent",
    "AssetRendition",
    "TranslationAssetRendition",
    "SpeechAudio",
    "SpeechAssetRendition",
    "AssetRenditionRecord",
]
