"""A tiny HTTP service the local Ouro backend can proxy route calls to.

It serves its own OpenAPI spec so ``ouro.services.create(spec_url=...)`` can
register every route, and records each request so tests can assert on exactly
what Ouro forwarded (body, resolved input assets, ``ouro-*`` headers).
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import httpx

ASYNC_DELAY_S = 1.0


def _json_body(schema_props: dict) -> dict:
    return {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"type": "object", "properties": schema_props}
            }
        },
    }


def _openapi(title: str) -> dict:
    ok = {"200": {"description": "OK"}}
    return {
        "openapi": "3.1.0",
        "info": {"title": title, "version": "1.0.0"},
        "paths": {
            "/echo": {
                "post": {
                    "summary": "Echo",
                    "operationId": "echo",
                    "requestBody": _json_body({"text": {"type": "string"}}),
                    "responses": ok,
                }
            },
            "/slow-echo": {
                "post": {
                    "summary": "Slow echo",
                    "operationId": "slow_echo",
                    "x-ouro-execution-mode": "async",
                    "requestBody": _json_body({"text": {"type": "string"}}),
                    "responses": {"202": {"description": "Accepted"}},
                }
            },
            "/slow-fail": {
                "post": {
                    "summary": "Slow fail",
                    "operationId": "slow_fail",
                    "x-ouro-execution-mode": "async",
                    "requestBody": _json_body({}),
                    "responses": {"202": {"description": "Accepted"}},
                }
            },
            "/fail": {
                "post": {
                    "summary": "Fail",
                    "operationId": "fail",
                    "requestBody": _json_body({}),
                    "responses": ok,
                }
            },
            "/items/{item_id}": {
                "get": {
                    "summary": "Get item",
                    "operationId": "get_item",
                    "parameters": [
                        {"name": "item_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "q", "in": "query", "required": False, "schema": {"type": "string"}},
                    ],
                    "responses": ok,
                }
            },
            "/inspect-file": {
                "post": {
                    "summary": "Inspect file",
                    "operationId": "inspect_file",
                    "x-ouro-input-assets": {"structure": {"asset_type": "file"}},
                    "requestBody": _json_body({"structure": {"type": "object"}}),
                    "responses": ok,
                }
            },
            "/make-report": {
                "post": {
                    "summary": "Make report",
                    "operationId": "make_report",
                    "x-ouro-output-assets": {"report": {"asset_type": "post"}},
                    "requestBody": _json_body({"title": {"type": "string"}}),
                    "responses": ok,
                }
            },
        },
    }


class MockService:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.requests: list[dict] = []
        self._server: ThreadingHTTPServer | None = None

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/{self.prefix}"

    @property
    def spec_url(self) -> str:
        return f"{self.base_url}/openapi.json"

    def last(self, path: str) -> dict:
        matches = [r for r in self.requests if r["path"] == path]
        assert matches, f"mock service never received {path}"
        return matches[-1]

    def start(self) -> None:
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status: int, payload) -> None:
                raw = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def _route(self, method: str) -> None:
                url = urlparse(self.path)
                path = url.path.removeprefix(f"/{service.prefix}") or "/"
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
                record = {
                    "method": method,
                    "path": path,
                    "query": {k: v[0] for k, v in parse_qs(url.query).items()},
                    "body": body,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                }
                service.requests.append(record)
                self._send(*service.handle(record))

            def do_GET(self):
                self._route("GET")

            def do_POST(self):
                self._route("POST")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()

    def handle(self, request: dict) -> tuple[int, object]:
        path, body, headers = request["path"], request["body"], request["headers"]
        if path == "/openapi.json":
            return 200, _openapi(f"{self.prefix} mock")
        if path == "/echo":
            return 200, {"echo": body.get("text"), "action_id": headers.get("ouro-action-id")}
        if path == "/fail":
            return 500, {"detail": "mock service exploded"}
        if path.startswith("/items/"):
            return 200, {"item_id": path.rsplit("/", 1)[1], "q": request["query"].get("q")}
        if path == "/inspect-file":
            structure = body.get("structure") or {}
            return 200, {"keys": sorted(body), "structure_keys": sorted(structure)}
        if path == "/make-report":
            title = body.get("title") or "Report"
            return 200, {
                "summary": f"made {title}",
                "report": {
                    "name": title,
                    "content": {
                        "text": f"# {title}\n\nGenerated by the mock service.",
                        "json": {
                            "type": "doc",
                            "content": [
                                {"type": "paragraph", "content": [{"type": "text", "text": "Generated by the mock service."}]}
                            ],
                        },
                    },
                },
            }
        if path in ("/slow-echo", "/slow-fail"):
            outcome = (
                {"status": "success", "response": {"echo": body.get("text")}}
                if path == "/slow-echo"
                else {"status": "error", "response": {"error": {"message": "async mock failure"}}}
            )
            threading.Thread(
                target=self._complete_later,
                args=(headers["ouro-webhook-url"], headers["ouro-webhook-token"], outcome),
                daemon=True,
            ).start()
            return 202, {"status": "accepted"}
        return 404, {"detail": f"no mock route for {path}"}

    @staticmethod
    def _complete_later(url: str, token: str, outcome: dict) -> None:
        time.sleep(ASYNC_DELAY_S)
        httpx.post(url, json=outcome, headers={"ouro-webhook-token": token}, timeout=10)
