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


class _FakeResponse:
    def __init__(self, body: dict, status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code
        self.is_success = status_code < 400

    def json(self):
        return self._body


class _FakeClient:
    def __init__(self, stats_count: int) -> None:
        self.stats_count = stats_count
        self.requests: list[dict] = []

    def post(self, path: str, json=None):
        self.requests.append({"method": "POST", "path": path, "json": json})
        if path == "/datasets/create/from-schema":
            return _FakeResponse({"data": DATASET_RESPONSE, "error": None})
        return _FakeResponse({"data": {"inserted": len(json.get("rows", []))}, "error": None})

    def get(self, path: str, params=None):
        self.requests.append({"method": "GET", "path": path, "params": params})
        if path.endswith("/stats"):
            return _FakeResponse({"data": {"count": self.stats_count}, "error": None})
        return _FakeResponse({"data": [], "error": None})


class _FakeOuro:
    def __init__(self, stats_count: int) -> None:
        self.client = _FakeClient(stats_count)
        self.websocket = None

    def _make_status_error(self, message: str, *, response, body):
        return RuntimeError(message)


class TestDatasetsCreate(unittest.TestCase):
    def test_create_sends_rows_in_schema_request_and_skips_fallback_when_inserted(self) -> None:
        ouro = _FakeOuro(stats_count=2)
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


if __name__ == "__main__":
    unittest.main()
