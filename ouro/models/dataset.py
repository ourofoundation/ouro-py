from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import AliasChoices, Field

from ._base import OuroModel, Page
from .asset import Asset

__all__ = [
    "Dataset",
    "DatasetColumn",
    "DatasetMetadata",
    "DatasetRef",
    "DatasetRows",
    "DatasetStats",
    "DatasetView",
    "ResolvedRef",
    "RowIngest",
]


class DatasetRef(OuroModel):
    """Declares a column as a reference to Ouro objects.

    The backend adds a foreign key to the table named by ``kind``
    (public.assets or public.actions); ``asset_type`` narrows asset references.
    """

    kind: Literal["asset", "action"] = "asset"
    asset_type: Optional[str] = None


class DatasetMetadata(OuroModel):
    table_name: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")
    columns: List[str] = Field(default_factory=list)
    table_size: Optional[int] = None
    refs: Optional[Dict[str, DatasetRef]] = None


class RowIngest(OuroModel):
    """How many rows a write inserted, and how many it skipped (bad refs)."""

    inserted: int = 0
    skipped: int = 0
    mode: Optional[str] = None


class Dataset(Asset):
    metadata: Optional[DatasetMetadata] = None
    preview: Optional[List[Dict[str, Any]]] = None
    # Set on the result of create/update calls that wrote rows.
    row_ingest: Optional[RowIngest] = None
    ingest_warning: Optional[Dict[str, Any]] = None


class DatasetColumn(OuroModel):
    """One column of a dataset's table. Names are lowercase snake_case."""

    name: str = Field(validation_alias=AliasChoices("name", "column_name"))
    type: str = Field(validation_alias=AliasChoices("type", "data_type"))
    is_nullable: Optional[bool] = None
    semantic_type: Optional[Literal["reference", "enum"]] = None
    ref_kind: Optional[Literal["asset", "action"]] = None
    asset_type: Optional[str] = None
    enum_values: Optional[List[str]] = None


class DatasetStats(OuroModel):
    count: int
    is_estimate: bool = False
    column_count: int = Field(alias="columnCount")
    columns: List[str] = Field(default_factory=list)
    table_name: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")
    size_bytes: Optional[int] = None
    table_size: Optional[int] = None


class DatasetView(OuroModel):
    """A saved ``(sql_query, config)`` pair that renders a chart."""

    id: UUID
    dataset_id: UUID
    name: str
    description: Optional[str] = None
    sql_query: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    engine_type: Optional[str] = None
    created_by: Optional[UUID] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class ResolvedRef(OuroModel):
    """The Ouro object a reference column value points to.

    Asset references carry ``asset_type``; action references carry
    ``status`` and the ``route_id`` / ``route_name`` that ran them.
    """

    kind: Literal["asset", "action"]
    id: UUID
    name: Optional[str] = None
    web_url: Optional[str] = None
    asset_type: Optional[str] = None
    status: Optional[str] = None
    route_id: Optional[UUID] = None
    route_name: Optional[str] = None
    created_at: Optional[datetime] = None


class DatasetRows(Page[Dict[str, Any]]):
    """A page of raw dataset rows.

    ``resolved_refs`` maps column -> referenced id -> :class:`ResolvedRef`
    when requested with ``resolve_refs=True``; ids the caller cannot see are
    omitted.
    """

    resolved_refs: Dict[str, Dict[str, ResolvedRef]] = Field(default_factory=dict)
