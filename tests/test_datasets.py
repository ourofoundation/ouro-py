from __future__ import annotations

import unittest

import pandas as pd

from ouro.resources.datasets import Datasets


DATASET_RESPONSE = {
    "id": "019df875-7957-7888-888f-f8140ff62564",
    "user_id": "847f4445-78ee-41b1-913b-5bd155c71b13",
    "org_id": "00000000-0000-0000-0000-000000000000",
    "team_id": "01956d7e-7f02-7715-9a89-1f847181e199",
    "visibility": "private",
    "asset_type": "dataset",
    "created_at": "2026-05-05T14:05:41.591000+00:00",
    "last_updated": "2026-05-05T14:05:41.591000+00:00",
    "name": "sample",
    "metadata": {
        "table_name": "sample",
        "columns": ["sample", "value"],
    },
    "preview": [{"sample": "alpha", "value": 1}],
}

DEFAULT_CREATE_DATA = object()


class _FakeResponse:
    def __init__(self, body: dict, status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code
        self.is_success = status_code < 400

    def json(self):
        return self._body


class _FakeClient:
    def __init__(
        self,
        stats_count: int,
        create_data=DEFAULT_CREATE_DATA,
        create_extra: dict | None = None,
    ) -> None:
        self.stats_count = stats_count
        self.create_data = (
            DATASET_RESPONSE if create_data is DEFAULT_CREATE_DATA else create_data
        )
        self.create_extra = create_extra or {}
        self.requests: list[dict] = []

    def post(self, path: str, json=None):
        self.requests.append({"method": "POST", "path": path, "json": json})
        if path == "/datasets/create/from-schema":
            return _FakeResponse(
                {"data": self.create_data, "error": None, **self.create_extra}
            )
        return _FakeResponse({"data": {"inserted": len(json.get("rows", []))}, "error": None})

    def put(self, path: str, json=None):
        self.requests.append({"method": "PUT", "path": path, "json": json})
        return _FakeResponse({"data": self.create_data, "error": None})

    def get(self, path: str, params=None):
        self.requests.append({"method": "GET", "path": path, "params": params})
        if path.endswith("/stats"):
            return _FakeResponse({"data": {"count": self.stats_count}, "error": None})
        return _FakeResponse({"data": [], "error": None})


class _SqlFakeClient:
    """Fake client for sql() tests — returns canned rows for query-custom."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.requests: list[dict] = []

    def post(self, path: str, json=None):
        self.requests.append({"method": "POST", "path": path, "json": json})
        return _FakeResponse({"data": self.rows, "error": None})

    def get(self, path: str, params=None):
        self.requests.append({"method": "GET", "path": path, "params": params})
        return _FakeResponse({"data": [], "error": None})


class _SqlFakeOuro:
    def __init__(self, rows: list[dict]) -> None:
        self.client = _SqlFakeClient(rows)
        self.websocket = None

    def _make_status_error(self, message: str, *, response, body):
        return RuntimeError(message)


class _FakeOuro:
    def __init__(
        self,
        stats_count: int,
        create_data=DEFAULT_CREATE_DATA,
        create_extra: dict | None = None,
    ) -> None:
        self.client = _FakeClient(
            stats_count,
            create_data=create_data,
            create_extra=create_extra,
        )
        self.websocket = None

    def _make_status_error(self, message: str, *, response, body):
        return RuntimeError(message)


class TestDatasetsCreate(unittest.TestCase):
    def test_create_sends_rows_in_schema_request_and_skips_fallback_when_backend_ingests(self) -> None:
        ouro = _FakeOuro(
            stats_count=0,
            create_extra={"row_ingest": {"inserted": 2, "skipped": 0}},
        )
        datasets = Datasets(ouro)

        datasets.create(
            name="sample",
            visibility="private",
            org_id="00000000-0000-0000-0000-000000000000",
            team_id="01956d7e-7f02-7715-9a89-1f847181e199",
            data=pd.DataFrame(
                [
                    {"sample": "alpha", "value": 1},
                    {"sample": "beta", "value": 2},
                ]
            ),
        )

        create_request = ouro.client.requests[0]
        self.assertEqual(create_request["path"], "/datasets/create/from-schema")
        self.assertEqual(
            create_request["json"]["dataset"]["rows"],
            [{"sample": "alpha", "value": "1"}, {"sample": "beta", "value": "2"}],
        )
        self.assertFalse(
            any(request["path"].endswith("/data") for request in ouro.client.requests)
        )
        self.assertFalse(
            any(request["path"].endswith("/stats") for request in ouro.client.requests)
        )

    def test_create_uploads_rows_when_backend_did_not_insert_them(self) -> None:
        ouro = _FakeOuro(stats_count=0)
        datasets = Datasets(ouro)

        datasets.create(
            name="sample",
            visibility="private",
            data=[{"sample": "alpha", "value": 1}],
        )

        data_uploads = [
            request for request in ouro.client.requests if request["path"].endswith("/data")
        ]
        self.assertEqual(len(data_uploads), 1)
        self.assertEqual(
            data_uploads[0]["json"],
            {"rows": [{"sample": "alpha", "value": "1"}], "mode": "append"},
        )

    def test_create_raises_clear_error_when_response_has_no_dataset(self) -> None:
        ouro = _FakeOuro(stats_count=0, create_data=None)
        datasets = Datasets(ouro)

        with self.assertRaisesRegex(
            ValueError, "Dataset create response did not include a dataset payload"
        ):
            datasets.create(
                name="sample",
                visibility="private",
                data=[{"sample": "alpha", "value": 1}],
            )


class TestDatasetAssetRefs(unittest.TestCase):
    def test_create_sends_asset_refs_without_client_side_fk_ddl(self) -> None:
        ouro = _FakeOuro(
            stats_count=0,
            create_extra={"row_ingest": {"inserted": 1, "skipped": 0}},
        )
        datasets = Datasets(ouro)

        datasets.create(
            name="refs",
            visibility="private",
            data=[{"file_id": "019df875-7957-7888-888f-f8140ff62564", "score": 1}],
            asset_refs={"file_id": "file"},
        )

        body = ouro.client.requests[0]["json"]["dataset"]
        self.assertNotIn("REFERENCES public.assets", body["schema"])
        self.assertEqual(body["asset_refs"], {"file_id": {"asset_type": "file"}})
        self.assertEqual(
            body["metadata"]["asset_refs"], {"file_id": {"asset_type": "file"}}
        )

    def test_create_treats_asset_refs_keys_as_promoted_columns(self) -> None:
        ouro = _FakeOuro(
            stats_count=0,
            create_extra={"row_ingest": {"inserted": 1, "skipped": 0}},
        )
        datasets = Datasets(ouro)

        datasets.create(
            name="refs",
            visibility="private",
            data=[{"file_id": "019df875-7957-7888-888f-f8140ff62564"}],
            asset_refs={"file_id": {"asset_type": "file"}},
        )

        body = ouro.client.requests[0]["json"]["dataset"]
        self.assertEqual(body["asset_refs"], {"file_id": {"asset_type": "file"}})

    def test_create_raises_for_unknown_asset_ref_column(self) -> None:
        datasets = Datasets(_FakeOuro(stats_count=0))
        with self.assertRaisesRegex(ValueError, "not present in data"):
            datasets.create(
                name="refs",
                visibility="private",
                data=[{"file_id": "019df875-7957-7888-888f-f8140ff62564"}],
                asset_refs={"missing_col": {"asset_type": "file"}},
            )

    def test_update_passes_asset_refs(self) -> None:
        ouro = _FakeOuro(stats_count=0)
        datasets = Datasets(ouro)

        datasets.update(
            "019df875-7957-7888-888f-f8140ff62564",
            asset_refs={"file_id": "file"},
        )

        put_request = next(
            r for r in ouro.client.requests if r["method"] == "PUT"
        )
        body = put_request["json"]["dataset"]
        self.assertEqual(body["asset_refs"], {"file_id": {"asset_type": "file"}})
        self.assertEqual(
            body["metadata"]["asset_refs"], {"file_id": {"asset_type": "file"}}
        )


class TestDatasetEnumColumns(unittest.TestCase):
    def test_create_sends_enum_columns_without_client_side_check_ddl(self) -> None:
        ouro = _FakeOuro(
            stats_count=0,
            create_extra={"row_ingest": {"inserted": 2, "skipped": 0}},
        )
        datasets = Datasets(ouro)

        datasets.create(
            name="statuses",
            visibility="private",
            data=[{"status": "todo"}, {"status": "done"}],
            enum_columns={"status": ["todo", "done"]},
        )

        body = ouro.client.requests[0]["json"]["dataset"]
        self.assertNotIn("CHECK", body["schema"])
        self.assertEqual(body["enum_columns"], {"status": {"values": ["todo", "done"]}})
        self.assertEqual(
            body["metadata"]["enum_columns"],
            {"status": {"values": ["todo", "done"]}},
        )

    def test_create_raises_for_unknown_enum_column(self) -> None:
        datasets = Datasets(_FakeOuro(stats_count=0))
        with self.assertRaisesRegex(ValueError, "not present in data"):
            datasets.create(
                name="statuses",
                visibility="private",
                data=[{"status": "todo"}],
                enum_columns={"missing": ["todo", "done"]},
            )

    def test_update_passes_enum_columns(self) -> None:
        ouro = _FakeOuro(stats_count=0)
        datasets = Datasets(ouro)

        datasets.update(
            "019df875-7957-7888-888f-f8140ff62564",
            enum_columns={"status": {"values": ["todo", "done"]}},
        )

        put_request = next(
            r for r in ouro.client.requests if r["method"] == "PUT"
        )
        body = put_request["json"]["dataset"]
        self.assertEqual(body["enum_columns"], {"status": {"values": ["todo", "done"]}})
        self.assertEqual(
            body["metadata"]["enum_columns"],
            {"status": {"values": ["todo", "done"]}},
        )


class _ResolveFakeClient(_FakeClient):
    def get(self, path: str, params=None):
        self.requests.append({"method": "GET", "path": path, "params": params})
        if path.endswith("/stats"):
            return _FakeResponse({"data": {"count": self.stats_count}, "error": None})
        if path.endswith("/data"):
            body = {
                "data": [{"file_id": "019df875-7957-7888-888f-f8140ff62564"}],
                "pagination": {"hasMore": False},
                "error": None,
            }
            if params and params.get("resolve_asset_refs"):
                body["resolved_asset_refs"] = {
                    "file_id": {
                        "019df875-7957-7888-888f-f8140ff62564": {
                            "asset_id": "019df875-7957-7888-888f-f8140ff62564",
                            "asset_type": "file",
                            "name": "sample.cif",
                            "web_url": "https://ouro.foundation/files/a/sample-cif",
                        }
                    }
                }
            return _FakeResponse(body)
        return _FakeResponse({"data": [], "error": None})


class _ResolveFakeOuro(_FakeOuro):
    def __init__(self) -> None:
        super().__init__(stats_count=1)
        self.client = _ResolveFakeClient(stats_count=1)


class TestDatasetQueryResolveAssetRefs(unittest.TestCase):
    def test_query_resolve_asset_refs_passes_param_and_returns_sidecar(self) -> None:
        ouro = _ResolveFakeOuro()
        datasets = Datasets(ouro)

        result = datasets.query(
            "019df875-7957-7888-888f-f8140ff62564",
            limit=100,
            resolve_asset_refs=True,
        )

        data_request = next(
            r for r in ouro.client.requests if r["path"].endswith("/data")
        )
        self.assertEqual(data_request["params"]["resolve_asset_refs"], "true")
        self.assertIn("resolved_asset_refs", result)
        self.assertEqual(
            result["resolved_asset_refs"]["file_id"][
                "019df875-7957-7888-888f-f8140ff62564"
            ]["name"],
            "sample.cif",
        )

    def test_query_resolve_asset_refs_rejected_with_sql(self) -> None:
        datasets = Datasets(_SqlFakeOuro([]))
        with self.assertRaisesRegex(ValueError, "only supported for the paginated"):
            datasets.query(
                "019df875-7957-7888-888f-f8140ff62564",
                "SELECT * FROM {{table}}",
                resolve_asset_refs=True,
            )


class TestDatasetsQuerySql(unittest.TestCase):
    def test_query_with_sql_posts_to_query_custom_and_returns_dataframe(self) -> None:
        rows = [{"species": "gentoo", "n": 124}, {"species": "adelie", "n": 152}]
        ouro = _SqlFakeOuro(rows)
        datasets = Datasets(ouro)

        df = datasets.query(
            "019df875-7957-7888-888f-f8140ff62564",
            "SELECT species, count(*) AS n FROM {{table}} GROUP BY species",
        )

        self.assertEqual(len(ouro.client.requests), 1)
        request = ouro.client.requests[0]
        self.assertEqual(
            request["path"],
            "/datasets/019df875-7957-7888-888f-f8140ff62564/query-custom",
        )
        self.assertEqual(
            request["json"],
            {"query": "SELECT species, count(*) AS n FROM {{table}} GROUP BY species"},
        )
        self.assertEqual(list(df.columns), ["species", "n"])
        self.assertEqual(len(df), 2)

    def test_query_with_sql_rejects_pagination_args(self) -> None:
        datasets = Datasets(_SqlFakeOuro([]))
        with self.assertRaisesRegex(ValueError, "not compatible with sql"):
            datasets.query(
                "019df875-7957-7888-888f-f8140ff62564",
                "SELECT * FROM {{table}}",
                limit=10,
            )

    def test_query_rejects_empty_sql_string(self) -> None:
        datasets = Datasets(_SqlFakeOuro([]))
        with self.assertRaisesRegex(ValueError, "sql query is required"):
            datasets.query("019df875-7957-7888-888f-f8140ff62564", "   ")


if __name__ == "__main__":
    unittest.main()
