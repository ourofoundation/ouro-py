from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union
from uuid import UUID

from pydantic import Field, model_validator

from ._base import OuroModel


class RichText(OuroModel):
    """TipTap document (``data``) paired with its plain-text rendering.

    Plain strings from older records parse as ``RichText(text=...)``.
    """

    text: str = ""
    data: Optional[Dict[str, Any]] = Field(default=None, alias="json")

    @model_validator(mode="before")
    @classmethod
    def _from_plain_text(cls, value: Any) -> Any:
        return {"text": value} if isinstance(value, str) else value


class LanguageToolProviderPreference(OuroModel):
    """Provider route and output-affecting options."""

    route_id: Optional[UUID] = None
    options: Dict[str, Any] = Field(default_factory=dict)


class SpeechProviderPreference(LanguageToolProviderPreference):
    voice: Optional[str] = None


class LanguageToolsPreferences(OuroModel):
    """Preferred language and route-provider configuration."""

    language: str
    translation: LanguageToolProviderPreference
    speech: SpeechProviderPreference
    transcription: Optional[LanguageToolProviderPreference] = None


class Preferences(OuroModel):
    """User preferences record returned by the preferences API."""

    id: UUID
    user_id: UUID
    notifications: Optional[Dict[str, Any]] = None
    webhooks: Optional[List[Dict[str, Any]]] = None
    onboarding: Optional[Dict[str, Any]] = None
    language_tools: Optional[LanguageToolsPreferences] = None


class UserProfile(OuroModel):
    """The public face of a user, as embedded in other objects."""

    user_id: UUID
    username: Optional[str] = None
    name: Optional[str] = None
    avatar_path: Optional[str] = None
    bio: Optional[str] = None
    actor_type: Optional[str] = None

    @property
    def is_agent(self) -> bool:
        return self.actor_type == "agent"


class User(UserProfile):
    """A full user profile from ``users.me()``, ``users.get()``, or search."""

    plan_type: Optional[str] = None
    last_active: Optional[datetime] = None
    urls: Optional[List[Any]] = None
    followers: Optional[int] = None
    following: Optional[int] = None
    level: Optional[int] = None
    total_xp: Optional[int] = Field(default=None, alias="totalXp")
    is_self: Optional[bool] = Field(default=None, alias="isSelf")
    is_following: Optional[bool] = Field(default=None, alias="isFollowing")
    is_followed: Optional[bool] = Field(default=None, alias="isFollowed")
    badges: Optional[List[Dict[str, Any]]] = None
    metrics: Optional[Dict[str, Any]] = None


class PlanLimits(OuroModel):
    max_storage_bytes: int = Field(alias="maxStorageBytes")
    max_assets: int = Field(alias="maxAssets")
    max_datasets: int = Field(alias="maxDatasets")
    can_create_private_assets: bool = Field(alias="canCreatePrivateAssets")
    can_monetize: bool = Field(alias="canMonetize")


class PlanUsage(OuroModel):
    used: int
    limit: int


class PlanInfo(OuroModel):
    """The authenticated user's plan, its limits, and current usage."""

    plan_type: str = Field(alias="planType")
    limits: PlanLimits
    usage: Dict[str, PlanUsage] = Field(default_factory=dict)


class OrganizationProfile(OuroModel):
    id: UUID
    name: str
    avatar_path: Optional[str] = None
    mission: Optional[str] = None


class TeamProfile(OuroModel):
    id: Optional[UUID] = None
    org_id: Optional[UUID] = None
    name: Optional[str] = None


class Citation(OuroModel):
    """Structured bibliographic record for a related scholarly work."""

    doi: Optional[str] = None
    title: Optional[str] = None
    authors: Optional[List[str]] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    url: Optional[str] = None
    bibtex: Optional[str] = None
    source: Optional[str] = None


class Attribution(OuroModel):
    """Provenance and citation for an asset (assets.attribution column).

    Distinct from type-specific ``metadata`` (file storage, service config, …).
    ``doi`` is reserved for this asset's minted DOI; related-work DOIs live on
    ``citation`` / ``doi_url``.
    """

    originality: Optional[Literal["original", "derivative", "third-party"]] = "original"
    external_url: Optional[str] = None
    github_url: Optional[str] = None
    paper_url: Optional[str] = None
    doi_url: Optional[str] = None
    citation: Optional[Citation] = None
    relation_type: Optional[
        Literal[
            "IsSupplementTo",
            "IsDerivedFrom",
            "References",
            "IsVariantFormOf",
            "IsIdenticalTo",
        ]
    ] = None
    doi: Optional[str] = None


class Asset(OuroModel):
    id: UUID
    user_id: UUID
    user: Optional[UserProfile] = None
    org_id: UUID
    team_id: UUID
    parent_id: Optional[UUID] = None
    organization: Optional[OrganizationProfile] = None
    team: Optional[TeamProfile] = None
    visibility: str
    asset_type: str
    created_at: datetime
    last_updated: datetime
    name: Optional[str] = None
    description: Optional[RichText] = None
    license_id: Optional[str] = None
    metadata: Optional[dict] = None
    attribution: Optional[Attribution] = None
    monetization: Optional[str] = None
    price: Optional[float] = None
    # The seller's primary currency; price / unit_cost mirror its price
    price_currency: Optional[str] = None
    # Dual pricing: an independent price per currency (dollars / sats).
    # None = not sold in that currency.
    price_usd: Optional[float] = None
    price_sats: Optional[int] = None
    unit_cost_usd: Optional[float] = None
    unit_cost_sats: Optional[float] = None
    # A TipTap doc for posts; the first rows for datasets and CSV files.
    preview: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None
    cost_accounting: Optional[str] = None
    cost_unit: Optional[str] = None
    unit_cost: Optional[float] = None
    # Runtime pricing: the most seconds one run can be billed
    max_billable_seconds: Optional[int] = None
    state: Literal["queued", "in-progress", "success", "error"] = "success"
    source: Literal["web", "api"] = "web"
    slug: Optional[str] = None
    url: Optional[str] = None


class AssetRef(OuroModel):
    """A lightweight summary of an asset, as embedded in other objects."""

    id: UUID
    name: Optional[str] = None
    asset_type: Optional[str] = None
    org_id: Optional[UUID] = None
    team_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    user: Optional[UserProfile] = None
    visibility: Optional[str] = None
    description: Optional[RichText] = None
    created_at: Optional[datetime] = None
    slug: Optional[str] = None
    url: Optional[str] = None


class DeleteResult(OuroModel):
    """What a delete removed — or would remove, when ``dry_run`` is true."""

    id: UUID
    name: Optional[str] = None
    asset_type: str
    deleted_children: List[AssetRef] = Field(default_factory=list)
    dry_run: bool = False


class Download(OuroModel):
    """A downloaded asset saved to local disk."""

    id: UUID
    path: str
    filename: str
    content_type: Optional[str] = None
    size: int


class Permission(OuroModel):
    """A direct grant of a role on an asset."""

    id: UUID
    asset_id: UUID
    asset_type: Optional[str] = None
    user_id: Optional[UUID] = None
    user: Optional[UserProfile] = None
    org_id: Optional[UUID] = None
    role: str
    visibility: Optional[str] = None
    granter_id: Optional[UUID] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class Tag(OuroModel):
    id: UUID
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    parent_id: Optional[UUID] = None
    asset_types: Optional[List[str]] = None
    rank: Optional[int] = None


class AssetTag(OuroModel):
    """A tag applied to an asset, manually or by the auto-tagger."""

    id: UUID
    asset_id: UUID
    tag_id: UUID
    tag: Tag
    source: Optional[str] = None
    confidence: Optional[float] = None
    user_id: Optional[UUID] = None
    created_at: Optional[datetime] = None


class Connection(OuroModel):
    """A graph edge between two assets (reference, component, action, …)."""

    id: UUID
    type: str
    source_id: UUID
    target_id: UUID
    source_asset_type: Optional[str] = None
    target_asset_type: Optional[str] = None
    source: Optional[AssetRef] = None
    target: Optional[AssetRef] = None
    action_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class AssetCounts(OuroModel):
    views: int = 0
    comments: int = 0
    reactions: int = 0
    downloads: int = 0
    earnings_total: int = 0


class Engagement(AssetCounts):
    """Engagement split into external (non-owner) and bot-filtered signals."""

    quality_views: int = 0
    external_comments: int = 0
    external_reactions: int = 0
    uses: int = 0


class AssetImpact(Engagement):
    asset_id: UUID
    asset_type: Optional[str] = None
    name: Optional[str] = None
    owner_user_id: Optional[UUID] = None
    popularity_7d: float = 0
    median_dwell_ms: Optional[float] = None
    quest_ids: List[UUID] = Field(default_factory=list)


class ImpactTotals(Engagement):
    assets: int = 0


class UserImpact(OuroModel):
    user_id: UUID
    since: Optional[datetime] = None
    aggregate: ImpactTotals
    assets: List[AssetImpact] = Field(default_factory=list)
