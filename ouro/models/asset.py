from datetime import datetime
from typing import List, Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel
from typing_extensions import TypedDict


class UserProfile(BaseModel):
    user_id: UUID
    username: Optional[str] = None
    avatar_path: Optional[str] = None
    bio: Optional[str] = None
    actor_type: Optional[str] = None
    is_agent: bool = False

    def __init__(self, **data):
        if "is_agent" not in data and data.get("actor_type") is not None:
            data["is_agent"] = data.get("actor_type") == "agent"
        super().__init__(**data)


class OrganizationProfile(BaseModel):
    id: UUID
    name: str
    avatar_path: Optional[str] = None
    mission: Optional[str] = None


class TeamProfile(BaseModel):
    id: Optional[UUID] = None
    org_id: Optional[UUID] = None
    name: Optional[str] = None


class DescriptionDict(TypedDict, total=False):
    """Shape of a structured description as returned by the API."""
    json: dict
    text: str


class Citation(BaseModel):
    """Structured bibliographic record for a related scholarly work."""

    doi: Optional[str] = None
    title: Optional[str] = None
    authors: Optional[List[str]] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    url: Optional[str] = None
    bibtex: Optional[str] = None
    source: Optional[str] = None


class Attribution(BaseModel):
    """Provenance and citation for an asset (assets.attribution column).

    Distinct from type-specific ``metadata`` (file storage, service config, …).
    ``doi`` is reserved for this asset's minted DOI; related-work DOIs live on
    ``citation`` / ``doi_url``.
    """

    originality: Optional[Literal["original", "derivative", "third-party"]] = None
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


class Asset(BaseModel):
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
    description: Optional[Union[str, DescriptionDict]] = None
    license_id: Optional[str] = None
    metadata: Optional[dict] = None
    attribution: Optional[Attribution] = None
    monetization: Optional[str] = None
    price: Optional[float] = None
    price_currency: Optional[str] = None
    preview: Optional[dict] = None
    cost_accounting: Optional[str] = None
    cost_unit: Optional[str] = None
    unit_cost: Optional[float] = None
    state: Literal["queued", "in-progress", "success", "error"] = "success"
    source: Literal["web", "api"] = "web"
    slug: Optional[str] = None
    url: Optional[str] = None
