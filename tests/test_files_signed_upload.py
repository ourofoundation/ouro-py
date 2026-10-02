"""File bytes that arrive through a signed upload URL instead of this client."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from ouro.resources.assets import Assets
from ouro.resources.files import Files
from ouro.resources.services import Services, _service_metadata

UPLOAD_ID = "files/user-1/0199-abc.csv"
FILE_ID = "25fa1d37-cd9c-4b16-990e-31231aea9a35"


def _response(data):
    response = MagicMock()
    response.json.return_value = {"data": data, "error": None}
    response.status_code = 200
    response.is_error = False
    return response


def _file(metadata: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": FILE_ID,
        "user_id": "00000000-0000-0000-0000-000000000099",
        "org_id": "00000000-0000-0000-0000-000000000000",
        "team_id": "00000000-0000-0000-0000-000000000000",
        "name": "points",
        "asset_type": "file",
        "visibility": "private",
        "created_at": now,
        "last_updated": now,
        "metadata": metadata,
    }


COMPLETED = {
    "id": FILE_ID,
    "bucket": "files",
    "path": "user-1/0199-abc.csv",
    "size": 8,
    "mime_type": "text/csv",
    "file_name": "points.csv",
}
METADATA = {
    "id": FILE_ID,
    "name": "0199-abc.csv",
    "bucket": "files",
    "path": "user-1/0199-abc.csv",
    "type": "text/csv",
    "extension": "csv",
    "size": 8,
}


def _files(*responses) -> tuple[Files, MagicMock]:
    ouro = MagicMock()
    ouro.organization = None
    ouro.client.post.side_effect = [r for method, r in responses if method == "post"]
    ouro.client.get.side_effect = [r for method, r in responses if method == "get"]
    ouro.client.put.side_effect = [r for method, r in responses if method == "put"]
    return Files(ouro), ouro.client


class TestSignedUpload(unittest.TestCase):
    def test_create_upload_url_sends_only_what_is_set(self) -> None:
        files, client = _files(("post", _response({"upload_id": UPLOAD_ID})))
        self.assertEqual(files.create_upload_url("points.csv")["upload_id"], UPLOAD_ID)
        client.post.assert_called_once_with(
            "/files/upload-url", json={"file_name": "points.csv", "visibility": "private"}
        )

    def test_create_completes_the_upload_instead_of_sending_bytes(self) -> None:
        files, client = _files(
            ("post", _response(COMPLETED)),
            ("get", _response({"metadata": {}})),
            ("post", _response(_file(METADATA))),
        )
        files.create(name="points", visibility="private", upload_id=UPLOAD_ID, file_name="points.csv")

        (complete, create) = client.post.call_args_list
        self.assertEqual(complete.args[0], "/files/upload/complete")
        self.assertEqual(complete.kwargs["json"], {"upload_id": UPLOAD_ID, "file_name": "points.csv"})
        self.assertEqual(create.args[0], "/files/create")
        sent = create.kwargs["json"]["file"]
        self.assertEqual(sent["id"], FILE_ID)
        self.assertEqual(sent["metadata"]["path"], "user-1/0199-abc.csv")
        self.assertEqual(sent["metadata"]["size"], 8)
        self.assertEqual(sent["metadata"]["type"], "text/csv")

    def test_update_hands_the_uploaded_object_to_the_file(self) -> None:
        files, client = _files(("post", _response(COMPLETED)), ("put", _response(_file(METADATA))))
        files.update(FILE_ID, upload_id=UPLOAD_ID, name="renamed")

        path, = client.put.call_args.args
        self.assertEqual(path, f"/files/{FILE_ID}")
        sent = client.put.call_args.kwargs["json"]["file"]
        self.assertEqual(sent["name"], "renamed")
        self.assertEqual(
            sent["metadata"],
            {
                "bucket": "files",
                "path": "user-1/0199-abc.csv",
                "size": 8,
                "type": "text/csv",
                "name": "points.csv",
            },
        )

    def test_read_upload_fetches_the_bytes_then_discards(self) -> None:
        files, client = _files(("post", _response({"download_url": "https://storage/signed"})))
        client.request.return_value = _response({"discarded": True})
        fetched = MagicMock(content=b"# Title\n")
        with patch("ouro.resources.files.httpx.get", return_value=fetched) as get:
            self.assertEqual(files.read_upload(UPLOAD_ID), b"# Title\n")

        client.post.assert_called_once_with(
            "/files/upload/complete", json={"upload_id": UPLOAD_ID, "download": True}
        )
        self.assertEqual(get.call_args.args[0], "https://storage/signed")
        client.request.assert_called_once_with(
            "DELETE", "/files/upload", json={"upload_id": UPLOAD_ID}
        )

    def test_read_upload_can_keep_the_upload(self) -> None:
        files, client = _files(("post", _response({"download_url": "https://storage/signed"})))
        with patch("ouro.resources.files.httpx.get", return_value=MagicMock(content=b"x")):
            files.read_upload(UPLOAD_ID, discard=False)
        client.request.assert_not_called()

    def test_exactly_one_source_of_bytes(self) -> None:
        files, client = _files()
        with self.assertRaises(ValueError):
            files.create(name="x", visibility="private")
        with self.assertRaises(ValueError):
            files.create(name="x", visibility="private", upload_id=UPLOAD_ID, file_path="a.csv")
        with self.assertRaises(ValueError):
            files.update(FILE_ID, upload_id=UPLOAD_ID, file_content=b"x", file_name="a.csv")
        client.post.assert_not_called()


class TestDownloadUrl(unittest.TestCase):
    def test_create_download_url_sends_only_what_is_set(self) -> None:
        ouro = MagicMock()
        assets = Assets(ouro)
        assets._handle_response = MagicMock(return_value={"download_url": "https://x"})

        self.assertEqual(assets.create_download_url(FILE_ID), {"download_url": "https://x"})
        ouro.client.post.assert_called_with(f"/assets/{FILE_ID}/download-url", json={})

        assets.create_download_url(FILE_ID, format="html")
        ouro.client.post.assert_called_with(
            f"/assets/{FILE_ID}/download-url", json={"format": "html"}
        )


class TestServiceSpecUpload(unittest.TestCase):
    def test_spec_upload_id_travels_in_metadata(self) -> None:
        self.assertEqual(
            _service_metadata(base_url="https://api.example.com", spec_upload_id=UPLOAD_ID),
            {"base_url": "https://api.example.com", "spec_upload_id": UPLOAD_ID},
        )

    def test_an_uploaded_spec_uses_the_spec_endpoints(self) -> None:
        ouro = MagicMock()
        ouro.organization = None
        services = Services(ouro)
        services._parse = MagicMock()
        services._handle_response = MagicMock()

        services.create(name="api", base_url="https://api.example.com", spec_upload_id=UPLOAD_ID)
        self.assertEqual(ouro.client.post.call_args.args[0], "/services/create/from-file")
        sent = ouro.client.post.call_args.kwargs["json"]["service"]
        self.assertEqual(sent["metadata"]["spec_upload_id"], UPLOAD_ID)

        services.update(FILE_ID, name="api", spec_upload_id=UPLOAD_ID)
        self.assertEqual(ouro.client.put.call_args.args[0], f"/services/{FILE_ID}/update/from-file")


if __name__ == "__main__":
    unittest.main()
