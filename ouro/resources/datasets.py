import json
import logging
import time as timer
from datetime import date, datetime, time
from typing import Any, List, Optional

import numpy as np
import pandas as pd
from ouro._resource import SyncAPIResource
from ouro.models import Dataset

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Datasets"]


class Datasets(SyncAPIResource):
    def create(
        self,
        name: str,
        visibility: str,
        data: Optional[pd.DataFrame] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
        description: Optional[str] = None,
        **kwargs,
    ):
        try:
            df = data.copy()
            # Get a sql safe table name from the name
            table_name = name.replace(" ", "_").lower()

            # Reset the index if it exists to use as the primary key
            index_name = df.index.name
            if index_name:
                df.reset_index(inplace=True)

            create_table_sql = pd.io.sql.get_schema(
                df,
                name=table_name,
                schema="datasets",
                # TODO: Add support for primary keys
                # keys=index_name
            )

            create_table_sql = create_table_sql.replace(
                "TIMESTAMP", "TIMESTAMP WITH TIME ZONE"
            )
            create_table_sql = create_table_sql.replace(
                "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"
            )

            log.debug(f"Creating a dataset:\n{create_table_sql}")

            body = {
                "name": name,
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                "description": description,
                "schema": create_table_sql,
                **kwargs,
            }

            # Filter out None values
            body = {k: v for k, v in body.items() if v is not None}

            request = self.client.post(
                "/datasets/create/from-schema",
                json={"dataset": body},
            )
            request.raise_for_status()
            response = request.json()

            log.info(response)

            if response["error"]:
                raise Exception(response["error"])

            # Good response, but no data to insert right now
            if data is None:
                return Dataset(**response["data"])

            # Good response, we can now insert the data
            created = Dataset(**response["data"])
            table_name = created.metadata["table_name"]

            insert_data = self._serialize_dataframe(df)

            # Insert the data into the table
            # TODO: May need to batch insert
            self.supabase.postgrest.schema("datasets")
            insert = self.supabase.table(table_name).insert(insert_data).execute()
            self.supabase.postgrest.schema("public")

            if len(insert.data) > 0:
                log.info(f"Inserted {len(insert.data)} rows into {table_name}")

            # Update the dataset with a data preview
            update = self.update(created.id, preview=insert_data[0:7])

            return created
        except Exception as e:
            log.error(e)
            raise e
        finally:
            self.supabase.postgrest.schema("public")

    def retrieve(self, id: str):
        """
        Retrieve a Dataset by its id
        """
        request = self.client.get(
            f"/datasets/{id}",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return Dataset(**response["data"])

    def query(self, id: str) -> pd.DataFrame:
        """
        Query a Dataset's data by its id
        """

        if not id:
            raise ValueError("Dataset id is required")

        request = self.client.get(
            f"/datasets/{id}/data",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])

        # Load the data as a DataFrame
        df = pd.DataFrame(response["data"])
        # Get the dataset's schema
        schema = self.schema(id)
        # Parse datatypes according to the schema
        for definition in schema:
            column_name = definition["column_name"]
            if (
                "timestamp" in definition["data_type"]
                or "date" in definition["data_type"]
            ):
                # Convert to pd.Timestamp
                df[column_name] = pd.to_datetime(df[column_name])
                # TODO: make this configurable
                # Remove any timezone information
                df[column_name] = df[column_name].dt.tz_localize(None)
                df[column_name] = df[column_name].dt.date
        return df

    def load(self, table_name: str) -> pd.DataFrame:
        """
        Load a Dataset's data by its table name. Good for large datasets.
        Method checks the row count and loads the data in batches if it's too big.
        """
        start = timer.time()

        # Set the schema to datasets
        self.supabase.postgrest.schema("datasets")

        row_count = self.supabase.table(table_name).select("*", count="exact").execute()
        row_count = row_count.count

        log.info(f"Loading {row_count} rows from datasets.{table_name}...")
        # Batch load the data if it's too big
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

        # Reset the schema
        self.supabase.postgrest.schema("public")

        return pd.DataFrame(data)

    def schema(self, id: str):
        """
        Retrieve a Dataset's schema
        """
        request = self.client.get(
            f"/datasets/{id}/schema",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return response["data"]

    def update(
        self,
        id: str,
        name: Optional[str] = None,
        visibility: Optional[str] = None,
        description: Optional[str] = None,
        preview: Optional[List[dict]] = None,
        data: Optional[pd.DataFrame] = None,
        monetization: Optional[str] = None,
        price: Optional[float] = None,
    ):
        """
        Update a Dataset's data by its id
        """
        try:
            body = {
                "name": name,
                "visibility": visibility,
                "monetization": monetization,
                "price": price,
                "description": description,
                "preview": preview,
            }
            # Filter out None values
            body = {k: v for k, v in body.items() if v is not None}
            request = self.client.put(
                f"/datasets/{id}",
                json={"dataset": body},
            )
            request.raise_for_status()
            response = request.json()
            if response["error"]:
                raise Exception(response["error"])
            # Make the data update if it's provided
            if data is not None:
                table_name = self.retrieve(id).metadata["table_name"]
                insert_data = self._serialize_dataframe(data)
                # Set the schema to datasets
                self.supabase.postgrest.schema("datasets")
                insert = self.supabase.table(table_name).insert(insert_data).execute()
                if len(insert.data) > 0:
                    log.info(f"Inserted {len(insert.data)} rows into {table_name}")
            return Dataset(**response["data"])
        except Exception as e:
            log.error(e)
            raise e
        finally:
            self.supabase.postgrest.schema("public")

    def _serialize_dataframe(self, data: pd.DataFrame) -> List[dict]:
        """
        Make a DataFrame serializable by converting NaN values to None,
        formatting datetime columns to strings, and converting empty strings to None.
        """

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

        # Apply the serialization function to all elements
        clean = clean.map(serialize_value)

        # Convert to list of dicts
        clean = clean.to_dict(orient="records")

        return clean
