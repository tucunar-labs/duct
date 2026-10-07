# pyright: basic
import struct
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

SRC = """
import duct

@duct.function
def inc(x: int) -> int:
    print("noise on stdout must not break the protocol")
    return x + 1

@duct.function
def shout(s: str) -> str:
    return s.upper()

@duct.function
def boom(x: int) -> int:
    raise ValueError("nope")

@duct.function(vectorized=True, types=["DOUBLE", "DOUBLE"], returns="DOUBLE")
def hyp(a, b):
    import pyarrow.compute as pc
    return pc.sqrt(pc.add(pc.multiply(a, a), pc.multiply(b, b)))

@duct.function(vectorized=True, types=["BIGINT"], returns="BIGINT")
def short(a):
    return a.slice(0, 1)
"""


class Worker:
    def __init__(self, path: Path) -> None:
        self.p = subprocess.Popen(
            [sys.executable, "-m", "duct.udf.worker", "--load", str(path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert self.p.stdout is not None and self.p.stdin is not None
        self.ready = self._read()

    def _read(self) -> bytes:
        assert self.p.stdout is not None
        (n,) = struct.unpack(">I", self.p.stdout.read(4))
        return self.p.stdout.read(n)

    def call(self, name: str, table: Any) -> tuple[int, Any]:
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as w:
            w.write_table(table)
        nb = name.encode()
        payload = struct.pack(">H", len(nb)) + nb + sink.getvalue().to_pybytes()
        assert self.p.stdin is not None
        self.p.stdin.write(struct.pack(">I", len(payload)) + payload)
        self.p.stdin.flush()
        body = self._read()
        if body[0] != 0:
            return body[0], body[1:].decode()
        return 0, ipc.open_stream(pa.py_buffer(body[1:])).read_all().column("result").to_pylist()

    def close(self) -> int:
        assert self.p.stdin is not None
        self.p.stdin.write(struct.pack(">I", 0))
        self.p.stdin.flush()
        return self.p.wait(timeout=10)


@pytest.fixture
def worker(tmp_path: Path) -> Iterator[Worker]:
    f = tmp_path / "udfs.py"
    f.write_text(SRC)
    w = Worker(f)
    yield w
    if w.p.poll() is None:
        w.p.kill()


def test_ready_frame_describes_functions(worker: Worker) -> None:
    import json

    d = json.loads(worker.ready[1:])
    names = {f["name"]: f for f in d["functions"]}
    assert names["inc"]["duckdb_args"] == ["BIGINT"]
    assert names["hyp"]["vectorized"] and names["hyp"]["duckdb_returns"] == "DOUBLE"


def test_scalar_with_nulls(worker: Worker) -> None:
    assert worker.call("inc", pa.table({"c0": pa.array([1, None, 3], pa.int64())})) == (
        0,
        [2, None, 4],
    )
    assert worker.call("shout", pa.table({"c0": ["a", "b"]})) == (0, ["A", "B"])


def test_vectorized(worker: Worker) -> None:
    t = pa.table({"c0": [3.0, 5.0], "c1": [4.0, 12.0]})
    assert worker.call("hyp", t) == (0, [5.0, 13.0])


def test_errors_keep_worker_alive(worker: Worker) -> None:
    status, msg = worker.call("boom", pa.table({"c0": pa.array([1], pa.int64())}))
    assert status == 1 and "ValueError: nope" in msg and "row 0" in msg
    status, msg = worker.call("short", pa.table({"c0": pa.array([1, 2], pa.int64())}))
    assert status == 1 and "1 values for 2 rows" in msg
    status, msg = worker.call("missing", pa.table({"c0": [1]}))
    assert status == 1 and "unknown function" in msg
    assert worker.call("inc", pa.table({"c0": pa.array([1], pa.int64())})) == (0, [2])
    assert worker.close() == 0
