from supabase import Client
import time
import logging
import httpx
import os
import pandas as pd
import numpy as np


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# console_handler = logging.StreamHandler()
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)
# console_handler.setFormatter(formatter)
# logger.addHandler(console_handler)


class Earth:
    def __init__(self, config):
        self.config = config

    def create_dataset(self, dataset: dict, data: pd.DataFrame):
        df = data.copy()
        # Get a sql safe table name from the name
        table_name = dataset["name"].replace(" ", "_").lower()

        # Reset the index if it exists to use as the primary key
        index_name = df.index.name
        if index_name:
            df.reset_index(inplace=True)

        create_table_sql = pd.io.sql.get_schema(
            df, name=table_name, schema="datasets", keys=index_name
        )

        create_table_sql = create_table_sql.replace(
            "TIMESTAMP", "TIMESTAMP WITH TIME ZONE"
        )
        create_table_sql = create_table_sql.replace(
            "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"
        )

        logger.info(f"{create_table_sql}")

        request = httpx.post(
            f"{os.environ.get('OURO_BACKEND_URL')}/elements/earth/create/from-schema",
            headers={
                "Authorization": f"{self.config.token}",
                "Content-Type": "application/json",
            },
            json={"dataset": {**dataset, "schema": create_table_sql}},
        )
        request.raise_for_status()
        response = request.json()

        logger.info(response)

        if response["error"]:
            logger.error(response["error"])
            raise Exception(response["error"])

        # If we've created the dataset, we can now insert the data
        # if response["data"] and not response["error"]:
        created = response["data"]
        table_name = created["metadata"]["table_name"]

        # Format the DataFrame to be inserted
        # Manually format any dates to be ISO 8601

        # insert_data = data.to_json(orient="records")
        # insert_data = pd.read_json(insert_data)

        for column in df.columns:
            if df[column].dtype == "datetime64[ns]":
                df[column] = df[column].dt.strftime("%Y-%m-%d")

        # Fill NaN values with None
        df = df.where(pd.notnull(df), None)
        df = df.map(lambda x: None if pd.isna(x) or x == "" else x)

        insert_data = df.to_dict(orient="records")

        # Ensure that we're not inserting any NaN values by converting them to None
        insert_data = [
            {k: v if not pd.isna(v) else None for k, v in row.items()}
            for row in insert_data
        ]

        insert = self.config.client.table(table_name).insert(insert_data).execute()
        if len(insert.data) > 0:
            logger.info(f"Inserted {len(insert.data)} rows into {table_name}")

    def get_dataset(self, dataset_id: str):
        dataset = (
            self.public_client.table("datasets")
            .select("*")
            .eq("id", dataset_id)
            .limit(1)
            .single()
            .execute()
        ).data
        return dataset

    def get_dataset_from_name(self, name: str):
        dataset = (
            self.public_client.table("datasets")
            .select("*")
            .eq("name", name)
            .limit(1)
            .single()
            .execute()
        ).data
        return dataset

    def get_dataset_schema(self, dataset_id: str):
        dataset = (
            self.public_client.table("datasets")
            .select("*")
            .eq("id", dataset_id)
            .limit(1)
            .single()
            .execute()
        ).data
        # Get the schema with an RPC call to the database
        schema = (
            self.public_client.rpc(
                "get_table_schema",
                {"table_schema_name": "datasets", "table_name": dataset["table_name"]},
            ).execute()
        ).data
        return schema

    def load_dataset(self, table_name: str, schema: str = "datasets"):
        start = time.time()

        row_count = self.client.table(table_name).select("*", count="exact").execute()
        row_count = row_count.count

        logger.info(f"Loading {row_count} rows from {schema}.{table_name}...")
        # Batch load the data if it's too big
        if row_count > 1_000_000:
            data = []
            for i in range(0, row_count, 1_000_000):
                logger.debug(f"Loading rows {i} to {i+1_000_000}")
                res = (
                    self.client.table(table_name)
                    .select("*")
                    .range(i, i + 1_000_000)
                    .execute()
                )
                data.extend(res.data)
        else:
            res = self.client.table(table_name).select("*").limit(1_000_000).execute()
            data = res.data

        end = time.time()
        logger.info(f"Finished loading data in {round(end - start, 2)} seconds.")

        self.data = data
        return data
