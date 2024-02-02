from supabase import Client
import time
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class Earth:
    def __init__(self, client: Client, public_client: Client):
        self.client = client
        self.public_client = public_client

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
