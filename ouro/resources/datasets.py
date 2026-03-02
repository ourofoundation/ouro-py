from __future__ import annotations

import json
import logging
import re
import time as timer
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd
from ouro._resource import SyncAPIResource, _coerce_description, _strip_none
from ouro.models import Dataset

from .content import Content

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Datasets"]

BATCH_INSERT_WARNING_THRESHOLD = 10_000
DatasetRowsInput = Union[pd.DataFrame, list[dict], dict]


def to_safe_sql_table_name(name: str) -> str:
    """Convert a name to a safe SQL table name (PostgreSQL 63-char limit).

    Matches the TypeScript implementation in backend/src/lib/elements/datasets.ts
    """
    name = name.strip()
    name = re.sub(r"[\s-]+", "_", name)
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    if not re.match(r"^[a-zA-Z_]", name):
        name = "_" + name
    name = name.lower()
    name = name[:63]
    return name


class Datasets(SyncAPIResource):
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

        table_name = to_safe_sql_table_name(name)

        index_name = df.index.name
        if index_name:
            df.reset_index(inplace=True)

        create_table_sql = pd.io.sql.get_schema(
            df,
            name=table_name,
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
        metadata = {
            "table_name": table_name,
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
            "metadata": metadata,
        })

        request = self.client.post(
            "/datasets/create/from-schema",
            json={"dataset": body},
        )
        response_data = self._handle_response(request)

        created = Dataset(**response_data)
        insert_data = self._serialize_dataframe(df)

        created_table_name = created.metadata["table_name"]

        if len(insert_data) > BATCH_INSERT_WARNING_THRESHOLD:
            log.warning(
                f"Inserting {len(insert_data)} rows at once into {created_table_name}. "
                "Consider batching for very large datasets."
            )

        try:
            self.supabase.postgrest.schema("datasets")
            insert = self.supabase.table(created_table_name).insert(insert_data).execute()

            if len(insert.data) > 0:
                log.info(f"Inserted {len(insert.data)} rows into {created_table_name}")

            return created
        finally:
            self.supabase.postgrest.schema("public")

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

    def query(self, id: str) -> pd.DataFrame:
        """Query a dataset's data by its id."""
        if not id:
            raise ValueError("Dataset id is required")

        request = self.client.get(f"/datasets/{id}/data")
        data = self._handle_response(request)

        df = pd.DataFrame(data)
        schema = self.schema(id)
        for definition in schema:
            column_name = definition["column_name"]
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

    def load(self, table_name: str) -> pd.DataFrame:
        """Load a Dataset's data by its table name.

        Good for large datasets. Checks the row count and loads in batches
        if necessary.
        """
        start = timer.time()

        self.supabase.postgrest.schema("datasets")

        row_count = self.supabase.table(table_name).select("*", count="exact").execute()
        row_count = row_count.count

        log.info(f"Loading {row_count} rows from datasets.{table_name}...")
        if row_count > 1_000_000:
            data = []
            for i in range(0, row_count, 1_000_000):
                log.debug(f"Loading rows {i} to {i+1_000_000}")
                res = (
                    self.supabase.table(table_name)
                    .select("*")
                    .range(i, i + 1_000_000)
                    .execute()
                )
                data.extend(res.data)
        else:
            res = self.supabase.table(table_name).select("*").limit(1_000_000).execute()
            data = res.data

        end = timer.time()
        log.info(f"Finished loading data in {round(end - start, 2)} seconds.")

        self.supabase.postgrest.schema("public")

        return pd.DataFrame(data)

    def update(
        self,
        id: str,
        name: Optional[str] = None,
        visibility: Optional[str] = None,
        description: Optional[Union[str, "Content"]] = None,
        preview: Optional[List[dict]] = None,
        data: Optional[DatasetRowsInput] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        **kwargs,
    ) -> Dataset:
        """Update a dataset by its id."""
        try:
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
                table_name = self.retrieve(id).metadata["table_name"]
                insert_data = self._serialize_dataframe(df)
                self.supabase.postgrest.schema("datasets")
                insert = self.supabase.table(table_name).insert(insert_data).execute()
                if len(insert.data) > 0:
                    log.info(f"Inserted {len(insert.data)} rows into {table_name}")

            return Dataset(**response_data)
        finally:
            self.supabase.postgrest.schema("public")

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
