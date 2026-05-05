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

# Placeholder identifier for the CREATE TABLE we generate client-side. The
# backend rewrites the table name to one derived from the dataset's UUID, and
# uses the request's `name` field (not the parsed SQL name) as the dataset's
# display-name candidate, so this identifier is purely a syntactic placeholder.
_PLACEHOLDER_TABLE = "dataset"


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

    def create(
        self,
        name: str,
        visibility: str,
        data: Optional[DatasetRowsInput] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[Union[str, "Content"]] = None,
        **kwargs,
    ) -> Dataset:
        """Create a new Dataset from tabular rows."""
        df = self._coerce_dataframe(data, parameter_name="data")
        if df is None:
            raise ValueError("data is required for dataset creation.")
        if df.empty or len(df.columns) == 0:
            raise ValueError("data must contain at least one row and one column.")

        index_name = df.index.name
        if index_name:
            df.reset_index(inplace=True)

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
        metadata = {
            "schema": "datasets",
            "columns": df.columns.tolist(),
        }

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
            self._handle_response(upload_req, raw=True)

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

        if limit is None:
            rows = self._fetch_all_rows(id)
            pagination: Dict[str, Any] = {"hasMore": False, "offset": 0, "limit": len(rows)}
        else:
            request = self.client.get(
                f"/datasets/{id}/data",
                params={"limit": limit, "offset": offset},
            )
            payload = self._handle_response(request, raw=True) or {}
            rows = payload.get("data") or []
            pagination = payload.get("pagination") or {"hasMore": False}

        df = self._coerce_schema_dtypes(pd.DataFrame(rows), id)

        if with_pagination:
            return {"data": df, "pagination": pagination}
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
        **kwargs,
    ) -> Dataset:
        """Update a dataset by its id.

        When `data` is provided, `data_mode` controls ingest semantics:
        - "append": add new rows
        - "overwrite": replace existing rows
        - "upsert": merge by id conflict target
        """
        if data_mode not in {"append", "overwrite", "upsert"}:
            raise ValueError("data_mode must be one of: append, overwrite, upsert.")

        body = _strip_none({
            "name": name,
            "visibility": visibility,
            "monetization": monetization,
            "price": price,
            "description": _coerce_description(description),
            "preview": preview,
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

        if df is not None:
            insert_data = self._serialize_dataframe(df)
            upload_req = self.client.post(
                f"/datasets/{id}/data",
                json={"rows": insert_data, "mode": data_mode},
            )
            self._handle_response(upload_req, raw=True)
            log.info(
                f"Ingested {len(insert_data)} rows into dataset {id} using mode={data_mode}"
            )

        return Dataset(**self._require_dataset_payload(response_data, operation="update"))

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
