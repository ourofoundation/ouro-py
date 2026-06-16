from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from typing import Any, Dict, List, Literal, Optional, Union

import numpy as np
import pandas as pd
from ouro._resource import SyncAPIResource, _coerce_description, _strip_none
from ouro.models import Dataset

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Datasets"]

BATCH_INSERT_WARNING_THRESHOLD = 10_000
DatasetRowsInput = Union[pd.DataFrame, list[dict], dict]
DatasetUploadMode = Literal["append", "overwrite", "upsert"]
DatasetEnumInput = Mapping[str, Union[Sequence[str], Mapping[str, Any]]]

# Placeholder identifier for the CREATE TABLE we generate client-side. The
# backend rewrites the table name to one derived from the dataset's UUID, and
# uses the request's `name` field (not the parsed SQL name) as the dataset's
# display-name candidate, so this identifier is purely a syntactic placeholder.
_PLACEHOLDER_TABLE = "dataset"


def _attach_ingest(dataset: Dataset, body: Any) -> Dataset:
    """Stash row-ingest stats and any partial-success warning from a write
    response onto the returned model.

    Reference columns are FK-enforced, so an ingest can land some rows and skip
    others (bad/missing ref ids). The backend reports this as ``row_ingest``
    ({inserted, skipped}) plus a structured ``warning`` listing the offending
    ids. The Dataset model rejects unknown fields, so expose them out-of-band
    (the same approach Action uses for ``_ouro``).
    """
    if not isinstance(body, Mapping):
        return dataset
    row_ingest = body.get("row_ingest")
    if row_ingest is None:
        # The /data endpoint reports stats under `data`; create-from-schema
        # reports them under `row_ingest`.
        data = body.get("data")
        if isinstance(data, Mapping) and "inserted" in data:
            row_ingest = {
                k: data[k] for k in ("inserted", "skipped", "mode") if k in data
            }
    if row_ingest is not None:
        object.__setattr__(dataset, "row_ingest", row_ingest)
    warning = body.get("warning")
    if warning is not None:
        object.__setattr__(dataset, "ingest_warning", warning)
    return dataset


class Datasets(SyncAPIResource):
    def list(
        self,
        query: str = "",
        limit: int = 20,
        offset: int = 0,
        scope: Optional[str] = None,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        sort: Optional[str] = None,
        time_window: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dataset]:
        """List datasets, optionally filtered by search query and scope.

        Args:
            sort: "relevant" | "recent" | "popular" | "updated"
            time_window: For sort="popular": "day" | "week" | "month" | "all".
                         Default: "month".
        """
        results = self.ouro.assets.search(
            query=query,
            asset_type="dataset",
            limit=limit,
            offset=offset,
            scope=scope,
            org_id=org_id,
            team_id=team_id,
            sort=sort,
            time_window=time_window,
            **kwargs,
        )
        return [Dataset(**item) for item in results]

    def _coerce_dataframe(
        self,
        data: Optional[DatasetRowsInput],
        *,
        parameter_name: str = "data",
    ) -> Optional[pd.DataFrame]:
        """Normalize common row inputs into a DataFrame."""
        if data is None:
            return None
        if isinstance(data, pd.DataFrame):
            return data.copy()
        if isinstance(data, Mapping):
            return pd.DataFrame([dict(data)])
        if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
            rows = list(data)
            if any(not isinstance(row, Mapping) for row in rows):
                raise ValueError(
                    f"{parameter_name} must be a DataFrame, dict, or list of dict rows."
                )
            return pd.DataFrame([dict(row) for row in rows])
        raise ValueError(
            f"{parameter_name} must be a DataFrame, dict, or list of dict rows."
        )

    def _dataset_has_rows(self, id: str) -> bool:
        """Return whether the dataset table has rows, falling back to upload on errors."""
        try:
            stats = self.stats(id)
            return int(stats.get("count") or 0) > 0
        except Exception as exc:
            log.debug("Could not verify dataset row count for %s: %s", id, exc)
            return False

    def _require_dataset_payload(self, payload: Any, *, operation: str) -> Mapping:
        if isinstance(payload, Mapping):
            return payload
        raise ValueError(
            f"Dataset {operation} response did not include a dataset payload."
        )

    @staticmethod
    def _normalize_refs(
        refs: Optional[Mapping[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Coerce a column->reference declaration into the wire shape.

        Each value selects a reference kind and (for assets) an optional target
        type. Accepted forms, keyed by column name:

        - ``"action"`` / ``"asset"`` — kind only.
        - ``"file"`` (any other string) — shorthand for an asset reference with
          that target type, i.e. ``{"kind": "asset", "asset_type": "file"}``.
        - ``{"kind": "action"}`` or ``{"kind": "asset", "asset_type": "file"}``.
        - ``{"asset_type": "file"}`` / ``None`` — kind defaults to ``"asset"``.
        """
        normalized: Dict[str, Dict[str, Any]] = {}
        for column, hint in (refs or {}).items():
            if hint is None:
                normalized[str(column)] = {"kind": "asset"}
            elif isinstance(hint, str):
                if hint in ("asset", "action"):
                    normalized[str(column)] = {"kind": hint}
                else:
                    normalized[str(column)] = {"kind": "asset", "asset_type": hint}
            elif isinstance(hint, Mapping):
                kind = hint.get("kind", "asset")
                if kind not in ("asset", "action"):
                    raise ValueError("refs kind must be 'asset' or 'action'.")
                entry: Dict[str, Any] = {"kind": kind}
                if kind == "asset" and hint.get("asset_type") is not None:
                    entry["asset_type"] = hint["asset_type"]
                normalized[str(column)] = entry
            else:
                raise ValueError(
                    "refs values must be a kind string ('asset'/'action'), an "
                    "asset target type string, or a mapping like "
                    "{'kind': 'asset', 'asset_type': 'file'}."
                )
        return normalized

    @staticmethod
    def _normalize_enum_columns(
        enum_columns: Optional[DatasetEnumInput],
    ) -> Dict[str, Dict[str, list[str]]]:
        """Coerce a column->enum declaration into the wire shape.

        Accepts either ``{"status": ["todo", "done"]}`` (shorthand) or
        ``{"status": {"values": ["todo", "done"]}}``.
        """
        normalized: Dict[str, Dict[str, list[str]]] = {}
        for column, declaration in (enum_columns or {}).items():
            if isinstance(declaration, Mapping):
                raw_values = declaration.get("values")
            else:
                raw_values = declaration

            if not isinstance(raw_values, Sequence) or isinstance(
                raw_values, (str, bytes, bytearray)
            ):
                raise ValueError(
                    "enum_columns values must be a list of strings or a "
                    "mapping like {'values': ['todo', 'done']}."
                )

            values: list[str] = []
            seen: set[str] = set()
            for raw in raw_values:
                if not isinstance(raw, str):
                    raise ValueError("enum_columns values must contain only strings.")
                value = raw.strip()
                if not value:
                    raise ValueError("enum_columns values must not contain empty strings.")
                if value not in seen:
                    seen.add(value)
                    values.append(value)

            if not values:
                raise ValueError("enum_columns values must not be empty.")
            normalized[str(column)] = {"values": values}
        return normalized

    def create(
        self,
        name: str,
        visibility: str,
        data: Optional[DatasetRowsInput] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[Union[str, "Content"]] = None,
        refs: Optional[Mapping[str, Any]] = None,
        enum_columns: Optional[DatasetEnumInput] = None,
        **kwargs,
    ) -> Dataset:
        """Create a new Dataset from tabular rows.

        Args:
            refs: Columns that hold Ouro object ids, keyed by column name. Each
                backend-promoted column gets a real Postgres foreign key (ON
                DELETE SET NULL) to the table for its kind: ``"asset"`` ->
                public.assets(id), ``"action"`` -> public.actions(id). Values
                may be a kind string (``"asset"``/``"action"``), an asset target
                type (``"file"`` -> asset ref of that type), or a mapping like
                ``{"kind": "asset", "asset_type": "file"}`` /
                ``{"kind": "action"}``.
            enum_columns: Columns with a closed set of string values. Values
                may be ``{"values": ["todo", "done"]}`` or the shorthand
                ``["todo", "done"]``. The backend stores a CHECK constraint
                and surfaces the values in dataset schema reads.
        """
        df = self._coerce_dataframe(data, parameter_name="data")
        if df is None:
            raise ValueError("data is required for dataset creation.")
        if df.empty or len(df.columns) == 0:
            raise ValueError("data must contain at least one row and one column.")

        index_name = df.index.name
        if index_name:
            df.reset_index(inplace=True)

        normalized_refs = self._normalize_refs(refs)
        normalized_enum_columns = self._normalize_enum_columns(enum_columns)
        ref_columns = list(normalized_refs)
        enum_columns_list = list(normalized_enum_columns)
        missing = [c for c in ref_columns + enum_columns_list if c not in df.columns]
        if missing:
            raise ValueError(
                f"declared dataset columns not present in data: {', '.join(missing)}"
            )

        # The backend rewrites the table name to one derived from the dataset's
        # UUID and uses the request's `name` field as the display-name
        # candidate, so the identifier embedded here is just a syntactic
        # placeholder to make the CREATE TABLE parseable.
        create_table_sql = pd.io.sql.get_schema(
            df,
            name=_PLACEHOLDER_TABLE,
            schema="datasets",
        )

        create_table_sql = create_table_sql.replace(
            "TIMESTAMP", "TIMESTAMP WITH TIME ZONE"
        )
        create_table_sql = create_table_sql.replace(
            "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"
        )

        log.debug(f"Creating a dataset:\n{create_table_sql}")

        preview = self._serialize_dataframe(df.head(12))
        insert_data = self._serialize_dataframe(df)
        metadata: Dict[str, Any] = {
            "schema": "datasets",
            "columns": df.columns.tolist(),
        }
        if normalized_refs:
            metadata["refs"] = normalized_refs
        if normalized_enum_columns:
            metadata["enum_columns"] = normalized_enum_columns

        body = _strip_none({
            "name": name,
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            "description": _coerce_description(description),
            "schema": create_table_sql,
            **kwargs,
            "source": "api",
            "asset_type": "dataset",
            "preview": preview,
            "rows": insert_data,
            "refs": normalized_refs or None,
            "enum_columns": normalized_enum_columns or None,
            "metadata": metadata,
        })

        request = self.client.post(
            "/datasets/create/from-schema",
            json={"dataset": body},
        )
        response_body = self._handle_response(request, raw=True) or {}
        response_data = (
            response_body.get("data") if isinstance(response_body, Mapping) else None
        )
        create_ingested_rows = (
            isinstance(response_body, Mapping) and "row_ingest" in response_body
        )

        created = Dataset(**self._require_dataset_payload(response_data, operation="create"))
        _attach_ingest(created, response_body)
        if len(insert_data) > BATCH_INSERT_WARNING_THRESHOLD:
            log.warning(
                f"Inserting {len(insert_data)} rows at once into {created.id}. "
                "Consider batching for very large datasets."
            )

        # Newer backends ingest rows atomically in the create request. Older
        # backends ignore ``rows`` and create only schema/preview, so keep the
        # explicit upload fallback until all deployments have the create support.
        if (
            insert_data
            and not create_ingested_rows
            and not self._dataset_has_rows(str(created.id))
        ):
            upload_req = self.client.post(
                f"/datasets/{created.id}/data",
                json={"rows": insert_data, "mode": "append"},
            )
            _attach_ingest(created, self._handle_response(upload_req, raw=True) or {})

        log.info(f"Inserted {len(insert_data)} rows into dataset {created.id}")
        return created

    def retrieve(self, id: str) -> Dataset:
        """Retrieve a dataset by its id."""
        request = self.client.get(f"/datasets/{id}")
        return Dataset(**self._handle_response(request))

    def stats(self, id: str) -> dict:
        """Retrieve a dataset's stats (row count, column count, etc.)."""
        request = self.client.get(f"/datasets/{id}/stats")
        return self._handle_response(request)

    def permissions(self, id: str) -> List[dict]:
        """Retrieve a dataset's permissions."""
        request = self.client.get(f"/datasets/{id}/permissions")
        return self._handle_response(request)

    def schema(self, id: str) -> List[dict]:
        """Retrieve a dataset's column schema."""
        request = self.client.get(f"/datasets/{id}/schema")
        return self._handle_response(request)

    def query(
        self,
        id: str,
        sql: Optional[str] = None,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
        with_pagination: bool = False,
        resolve_refs: bool = False,
    ) -> Union[pd.DataFrame, Dict[str, Any]]:
        """Query a dataset's data by its id.

        Three modes:

        1. **Full table** (default): fetches every row via the backend's
           paginated data endpoint and returns a single
           :class:`pandas.DataFrame`. Fine for notebooks and small tables;
           slow on large ones.
        2. **Paginated** (``limit`` set): fetch a single server-side page
           (``GET /datasets/{id}/data?limit=&offset=``). Combine with
           ``with_pagination=True`` to walk pages. Agents and MCP tools should
           use this path.
        3. **SQL** (``sql`` set): runs a read-only PostgreSQL query against
           the dataset's table. Use ``{{table}}`` as a placeholder for the
           fully-qualified table name. Read-only is enforced server-side and
           queries time out after 10 seconds. Standard PostgreSQL syntax —
           ``SELECT``, ``JOIN``, ``GROUP BY``, window functions, …
           ``limit``/``offset``/``with_pagination`` are not supported in this
           mode; include ``LIMIT``/``OFFSET`` directly in the SQL.

        Args:
            id: Dataset UUID.
            sql: Optional SQL query. Use ``{{table}}`` for the dataset table.
            limit: Single-page row count for the paginated mode.
            offset: Zero-based row offset for the paginated mode.
            with_pagination: If True (requires ``limit``), return
                ``{"data": DataFrame, "pagination": {"hasMore": bool, ...}}``.

        Returns:
            A DataFrame, or — when ``with_pagination=True`` — a dict with
            ``data`` (DataFrame) and ``pagination`` keys.

        Examples:
            >>> ouro.datasets.query(id)                           # all rows
            >>> ouro.datasets.query(id, limit=100, offset=0)      # first page
            >>> ouro.datasets.query(id, "SELECT count(*) FROM {{table}}")
            >>> ouro.datasets.query(
            ...     id,
            ...     "SELECT species, AVG(weight) AS avg "
            ...     "FROM {{table}} GROUP BY species ORDER BY avg DESC",
            ... )
        """
        if not id:
            raise ValueError("Dataset id is required")

        if sql is not None:
            if not sql.strip():
                raise ValueError("sql query is required when sql is provided.")
            if limit is not None or offset != 0 or with_pagination:
                raise ValueError(
                    "limit/offset/with_pagination are not compatible with "
                    "sql; include LIMIT/OFFSET in the SQL query instead."
                )
            if resolve_refs:
                raise ValueError(
                    "resolve_refs is only supported for the paginated "
                    "(non-sql) query path."
                )
            request = self.client.post(
                f"/datasets/{id}/query-custom",
                json={"query": sql},
            )
            rows = self._handle_response(request) or []
            return pd.DataFrame(rows)

        if with_pagination and limit is None:
            raise ValueError("with_pagination=True requires a limit.")
        if limit is not None and limit <= 0:
            raise ValueError("limit must be a positive integer.")
        if offset < 0:
            raise ValueError("offset must be non-negative.")

        resolved_refs: Optional[Dict[str, Any]] = None
        if limit is None:
            rows = self._fetch_all_rows(id)
            pagination: Dict[str, Any] = {"hasMore": False, "offset": 0, "limit": len(rows)}
        else:
            params: Dict[str, Any] = {"limit": limit, "offset": offset}
            if resolve_refs:
                params["resolve_refs"] = "true"
            request = self.client.get(
                f"/datasets/{id}/data",
                params=params,
            )
            payload = self._handle_response(request, raw=True) or {}
            rows = payload.get("data") or []
            pagination = payload.get("pagination") or {"hasMore": False}
            resolved_refs = payload.get("resolved_refs")

        df = self._coerce_schema_dtypes(pd.DataFrame(rows), id)

        # resolve_refs returns a sidecar map (column -> uuid -> resolved
        # reference), so force a dict return when requested to carry it
        # alongside the raw rows.
        if with_pagination or resolve_refs:
            result: Dict[str, Any] = {"data": df, "pagination": pagination}
            if resolve_refs:
                result["resolved_refs"] = resolved_refs or {}
            return result
        return df

    def _coerce_schema_dtypes(self, df: pd.DataFrame, id: str) -> pd.DataFrame:
        """Apply timestamp/date dtype coercion to a query result."""
        if df.empty:
            return df
        schema = self.schema(id)
        for definition in schema:
            column_name = definition["column_name"]
            if column_name not in df.columns:
                continue
            if (
                "timestamp" in definition["data_type"]
                or "date" in definition["data_type"]
            ):
                df[column_name] = pd.to_datetime(df[column_name])
                # Strips timezone info and converts to date; make configurable
                # if callers need full datetime precision.
                df[column_name] = df[column_name].dt.tz_localize(None)
                df[column_name] = df[column_name].dt.date
        return df

    def update(
        self,
        id: str,
        name: Optional[str] = None,
        visibility: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        preview: Optional[List[dict]] = None,
        data: Optional[DatasetRowsInput] = None,
        data_mode: DatasetUploadMode = "append",
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        refs: Optional[Mapping[str, Any]] = None,
        enum_columns: Optional[DatasetEnumInput] = None,
        **kwargs,
    ) -> Dataset:
        """Update a dataset by its id.

        When `data` is provided, `data_mode` controls ingest semantics:
        - "append": add new rows
        - "overwrite": replace existing rows
        - "upsert": merge by id conflict target

        Args:
            refs: Promote existing columns to references by adding a real FK (ON
                DELETE SET NULL) to public.assets(id) ("asset" kind) or
                public.actions(id) ("action" kind). Every value in each column
                must already be a valid id of that kind or NULL. Values may be a
                kind string, an asset target type, or a mapping like
                ``{"kind": "asset", "asset_type": "file"}`` /
                ``{"kind": "action"}``.
            enum_columns: Promote existing columns to enum columns by adding
                a CHECK constraint and schema metadata.
        """
        if data_mode not in {"append", "overwrite", "upsert"}:
            raise ValueError("data_mode must be one of: append, overwrite, upsert.")

        normalized_refs = self._normalize_refs(refs)
        normalized_enum_columns = self._normalize_enum_columns(enum_columns)
        metadata = kwargs.pop("metadata", None)
        if normalized_refs:
            metadata = {**(metadata or {}), "refs": normalized_refs}
        if normalized_enum_columns:
            metadata = {**(metadata or {}), "enum_columns": normalized_enum_columns}

        body = _strip_none({
            "name": name,
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            "description": _coerce_description(description),
            "preview": preview,
            "refs": normalized_refs or None,
            "enum_columns": normalized_enum_columns or None,
            "metadata": metadata,
            **kwargs,
        })

        request = self.client.put(
            f"/datasets/{id}",
            json={"dataset": body},
        )
        response_data = self._handle_response(request)

        df = self._coerce_dataframe(data, parameter_name="data")
        if df is not None and (df.empty or len(df.columns) == 0):
            raise ValueError("data must contain at least one row and one column.")

        upload_body: Any = None
        if df is not None:
            insert_data = self._serialize_dataframe(df)
            upload_req = self.client.post(
                f"/datasets/{id}/data",
                json={"rows": insert_data, "mode": data_mode},
            )
            upload_body = self._handle_response(upload_req, raw=True) or {}
            log.info(
                f"Ingested {len(insert_data)} rows into dataset {id} using mode={data_mode}"
            )

        updated = Dataset(
            **self._require_dataset_payload(response_data, operation="update")
        )
        return _attach_ingest(updated, upload_body)

    def list_views(self, id: str) -> List[dict]:
        """List saved views (visualizations) for a dataset."""
        request = self.client.get(f"/datasets/{id}/visualizations")
        return self._handle_response(request) or []

    def create_view(
        self,
        id: str,
        name: str,
        description: Optional[str] = None,
        sql_query: Optional[str] = None,
        engine_type: str = "auto",
        config: Optional[dict] = None,
        prompt: Optional[str] = None,
    ) -> dict:
        """Create a saved view (visualization) for a dataset."""
        body = _strip_none(
            {
                "name": name,
                "description": description,
                "sql_query": sql_query,
                "engine_type": engine_type,
                "config": config,
                "prompt": prompt,
            }
        )
        request = self.client.post(f"/datasets/{id}/visualizations", json=body)
        return self._handle_response(request)

    def update_view(
        self,
        id: str,
        view_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        sql_query: Optional[str] = None,
        engine_type: Optional[str] = None,
        config: Optional[dict] = None,
        prompt: Optional[str] = None,
    ) -> dict:
        """Update a saved view (visualization) for a dataset."""
        body = _strip_none(
            {
                "name": name,
                "description": description,
                "sql_query": sql_query,
                "engine_type": engine_type,
                "config": config,
                "prompt": prompt,
            }
        )
        request = self.client.put(f"/datasets/{id}/visualizations/{view_id}", json=body)
        return self._handle_response(request)

    def delete_view(self, id: str, view_id: str) -> None:
        """Delete a saved view (visualization) from a dataset."""
        request = self.client.delete(f"/datasets/{id}/visualizations/{view_id}")
        self._handle_response(request)

    def _fetch_all_rows(self, dataset_id: str, page_size: int = 1000) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            request = self.client.get(
                f"/datasets/{dataset_id}/data",
                params={"limit": page_size, "offset": offset},
            )
            payload = self._handle_response(request, raw=True) or {}
            page_rows = payload.get("data") or []
            pagination = payload.get("pagination") or {}
            rows.extend(page_rows)
            has_more = bool(pagination.get("hasMore"))
            if not has_more or len(page_rows) == 0:
                break
            offset += len(page_rows)
        return rows

    def delete(self, id: str) -> None:
        """Delete a dataset by its id."""
        request = self.client.delete(f"/datasets/{id}")
        self._handle_response(request)

    def _serialize_dataframe(self, data: pd.DataFrame) -> List[dict]:
        """Make a DataFrame serializable for JSON insertion."""

        def serialize_value(val: Any):
            if pd.isna(val) or val == "":
                return None
            elif isinstance(val, (date, datetime, time)):
                return val.isoformat()
            elif isinstance(val, (np.integer, np.floating)):
                return val.item()
            elif isinstance(val, (list, dict)):
                return json.dumps(val)
            return str(val)

        clean = data.copy()
        clean = clean.map(serialize_value)
        clean = clean.to_dict(orient="records")

        return clean
