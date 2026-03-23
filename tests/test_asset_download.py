from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ouro.resources.assets import Assets, _extract_download_filename, _resolve_download_path


class _FakeResponse:
    def __init__(self, chunks: list[bytes], headers: dict[str, str], status_code: int = 200) -> None:
        self._chunks = chunks
        self.headers = headers
        self.status_code = status_code
        self.is_error = status_code >= 400

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def iter_bytes(self):
        for chunk in self._chunks:
            yield chunk

    def json(self):
        return {"error": "boom"}


class _FakeRawClient:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def stream(self, method: str, path: str, json=None):
        self.last_request = {
            "method": method,
            "path": path,
            "json": json,
        }
        return self._response


class _FakeOuro:
    def __init__(self, response: _FakeResponse) -> None:
        self.client = None
        self.websocket = None
        self._raw_client = _FakeRawClient(response)
        self._validated = False

    def ensure_valid_token(self) -> None:
        self._validated = True

    def _make_status_error(self, err_msg: str, *, body, response):
        return RuntimeError(err_msg)


class TestAssetDownloadHelpers(unittest.TestCase):
    def test_extract_download_filename_prefers_filename_star(self) -> None:
        disposition = "attachment; filename*=UTF-8''my%20dataset.csv; filename=fallback.csv"
        self.assertEqual(_extract_download_filename(disposition, "default.bin"), "my dataset.csv")

    def test_resolve_download_path_uses_directory_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = _resolve_download_path(tmpdir, "asset.csv")
            self.assertEqual(target, Path(tmpdir) / "asset.csv")


class TestAssetsDownload(unittest.TestCase):
    def test_download_writes_streamed_bytes_and_returns_metadata(self) -> None:
        response = _FakeResponse(
            chunks=[b"hello ", b"world"],
            headers={
                "content-disposition": 'attachment; filename="report.csv"',
                "content-type": "text/csv",
            },
        )
        ouro = _FakeOuro(response)
        assets = Assets(ouro)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = assets.download("asset-123", output_path=tmpdir)

            output_path = Path(result["path"])
            self.assertTrue(ouro._validated)
            self.assertEqual(output_path.read_bytes(), b"hello world")
            self.assertEqual(result["filename"], "report.csv")
            self.assertEqual(result["content_type"], "text/csv")
            self.assertEqual(result["bytes"], 11)
            self.assertEqual(ouro._raw_client.last_request["method"], "POST")
            self.assertEqual(ouro._raw_client.last_request["path"], "/assets/asset-123/download")


if __name__ == "__main__":
    unittest.main()
