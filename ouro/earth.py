from supabase import Client


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
