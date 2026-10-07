import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

import duct
from duct._connection import DuctError

seen: dict[str, Any] = {}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"])).decode()
        seen.update(path=self.path, body=body, auth=self.headers.get("Authorization"))
        if "boom" in body:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"bad query")
            return
        payload = {"meta": [{"name": "x", "type": "Int32"}], "data": [{"x": 1}, {"x": 2}]}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture
def server() -> Iterator[str]:
    srv = HTTPServer(("127.0.0.1", 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()
    srv.server_close()


def test_env_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUCT_URL", "http://h:1/")
    monkeypatch.setenv("DUCT_TOKEN", "t")
    con = duct.connect()
    assert con.url == "http://h:1"
    assert con._token == "t"  # pyright: ignore[reportPrivateUsage]


def test_args_override_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUCT_URL", "http://env")
    assert duct.connect("http://arg").url == "http://arg"


def test_missing_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUCT_URL", raising=False)
    with pytest.raises(ValueError, match="DUCT_URL"):
        duct.connect()


def test_http_query(server: str) -> None:
    with duct.connect(server, token="secret") as con:
        res = con.sql("SELECT x FROM t;")
    assert res.columns == ["x"]
    assert res.rows == [{"x": 1}, {"x": 2}]
    assert seen["path"] == "/query"
    assert seen["body"] == "SELECT x FROM t\nFORMAT JSON"
    assert seen["auth"] == "Bearer secret"


def test_http_error(server: str) -> None:
    with pytest.raises(DuctError, match="HTTP 400: bad query"):
        duct.connect(server).sql("boom")


def test_closed(server: str) -> None:
    con = duct.connect(server)
    con.close()
    with pytest.raises(DuctError, match="closed"):
        con.sql("SELECT 1")


def test_optional_extras_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    monkeypatch.setitem(sys.modules, "polars", None)
    with pytest.raises(ImportError, match=r"duct-data\[polars\]"):
        duct.Result(["x"], []).to_polars()
