import logging
import time
from typing import List, Optional

import pandas as pd
from ouro._resource import SyncAPIResource

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
    ):
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
            return response["data"]

        # Good response, we can now insert the data
        created = response["data"]
        table_name = created["metadata"]["table_name"]

        insert_data = self._serialize_dataframe(df)
        # Insert the data into the table
        # TODO: May need to batch insert
        insert = self.database.table(table_name).insert(insert_data).execute()
        if len(insert.data) > 0:
            log.info(f"Inserted {len(insert.data)} rows into {table_name}")

        # Update the dataset with a data preview
        update = self.update(created["id"], preview=insert_data[0:7])

        return response["data"]

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
        return response["data"]

    def query(self, id: str) -> pd.DataFrame:
        """
        Query a Dataset's data by its id
        """
        request = self.client.get(
            f"/datasets/{id}/data",
        )
        request.raise_for_status()
        response = request.json()
        if response["error"]:
            raise Exception(response["error"])
        return pd.DataFrame(response["data"])

    def load(self, table_name: str):
        """
        Load a Dataset's data by its table name. Good for large datasets.
        Method checks the row count and loads the data in batches if it's too big.
        """
        start = time.time()

        row_count = self.database.table(table_name).select("*", count="exact").execute()
        row_count = row_count.count

        log.info(f"Loading {row_count} rows from datasets.{table_name}...")
        # Batch load the data if it's too big
        if row_count > 1_000_000:
            data = []
            for i in range(0, row_count, 1_000_000):
                log.debug(f"Loading rows {i} to {i+1_000_000}")
                res = (
                    self.database.table(table_name)
                    .select("*")
                    .range(i, i + 1_000_000)
                    .execute()
                )
                data.extend(res.data)
        else:
            res = self.database.table(table_name).select("*").limit(1_000_000).execute()
            data = res.data

        end = time.time()
        log.info(f"Finished loading data in {round(end - start, 2)} seconds.")

        self.data = data
        return data

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
            table_name = self.retrieve(id)["metadata"]["table_name"]
            insert_data = self._serialize_dataframe(data)

            insert = self.database.table(table_name).insert(insert_data).execute()
            if len(insert.data) > 0:
                log.info(f"Inserted {len(insert.data)} rows into {table_name}")

        return response["data"]

    def _serialize_dataframe(self, data: pd.DataFrame) -> List[dict]:
        """
        Make a DataFrame serializable by converting NaN values to None,
        formatting datetime columns to strings, and converting empty strings to None.
        """
        clean = data.copy()

        # Fill NaN values with None
        clean = clean.where(pd.notnull(clean), None)
        clean = clean.map(lambda x: None if pd.isna(x) or x == "" else x)

        # Convert datetime columns to strings
        for column in clean.columns:
            if clean[column].dtype == "datetime64[ns]":
                clean[column] = clean[column].dt.strftime("%Y-%m-%d")

        clean = clean.to_dict(orient="records")
        # Ensure that we're not inserting any NaN values by converting them to None
        clean = [
            {k: v if not pd.isna(v) else None for k, v in row.items()} for row in clean
        ]

        return clean
