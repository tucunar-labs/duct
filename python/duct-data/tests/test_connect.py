# pyright: basic
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

import duct
from duct._connection import DuctError, HTTPConnection

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
        self.send_response(200)
        if body.endswith("FORMAT ArrowStream"):
            sink = pa.BufferOutputStream()
            tbl = pa.table({"x": [1, 2], "s": ["a", None]})
            with ipc.new_stream(sink, tbl.schema) as w:
                w.write_table(tbl)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            self.end_headers()
            self.wfile.write(sink.getvalue().to_pybytes())
            return
        payload = {"meta": [{"name": "x", "type": "Int32"}], "data": [{"x": 1}, {"x": 2}]}
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
    assert isinstance(con, HTTPConnection)
    assert con.url == "http://h:1"
    assert con._token == "t"  # pyright: ignore[reportPrivateUsage]


def test_args_override_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUCT_URL", "http://env")
    assert duct.connect("http://arg").url == "http://arg"


def test_missing_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUCT_URL", raising=False)
    with pytest.raises(ValueError, match="DUCT_URL"):
        duct.connect()


def test_bad_scheme() -> None:
    with pytest.raises(ValueError, match="scheme"):
        duct.connect("ftp://x")


def test_http_json_fallback(server: str) -> None:
    with duct.connect(server, token="secret", format="json") as con:
        res = con.sql("SELECT x FROM t;")
    assert res.columns == ["x"]
    assert res.rows == [{"x": 1}, {"x": 2}]
    assert seen["path"] == "/query"
    assert seen["body"] == "SELECT x FROM t\nFORMAT JSON"
    assert seen["auth"] == "Bearer secret"


def test_http_arrow_stream(server: str) -> None:
    with duct.connect(server, token="secret") as con:  # pyarrow is installed: auto = arrow
        res = con.sql("SELECT x FROM t;")
    assert seen["body"] == "SELECT x FROM t\nFORMAT ArrowStream"
    assert res.columns == ["x", "s"]
    assert len(res) == 2
    assert res.to_arrow().column("x").to_pylist() == [1, 2]
    assert res.rows == [{"x": 1, "s": "a"}, {"x": 2, "s": None}]
    assert res.to_pandas()["x"].tolist() == [1, 2]
    assert res.to_polars()["s"].to_list() == ["a", None]


def test_json_result_converts(server: str) -> None:
    res = duct.connect(server, format="json").sql("SELECT 1")
    assert res.to_arrow().to_pylist() == [{"x": 1}, {"x": 2}]
    assert res.to_polars()["x"].to_list() == [1, 2]


def test_http_error(server: str) -> None:
    with pytest.raises(DuctError, match="HTTP 400: bad query"):
        duct.connect(server).sql("boom")


def test_unreachable() -> None:
    with pytest.raises(DuctError, match="cannot reach"):
        duct.connect("http://127.0.0.1:1", format="json").sql("SELECT 1")


def test_closed(server: str) -> None:
    con = duct.connect(server)
    con.close()
    with pytest.raises(DuctError, match="closed"):
        con.sql("SELECT 1")


def test_postgres_transport_selected_and_fails_clearly() -> None:
    with pytest.raises(DuctError, match="cannot connect"):
        duct.connect("postgresql://u:p@127.0.0.1:1/duct")


def test_optional_extras_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    monkeypatch.setitem(sys.modules, "polars", None)
    with pytest.raises(ImportError, match=r"duct-data\[polars\]"):
        duct.Result(["x"], []).to_polars()
    monkeypatch.setitem(sys.modules, "psycopg", None)
    with pytest.raises(ImportError, match=r"duct-data\[pg\]"):
        duct.connect("postgres://u@h/d")
