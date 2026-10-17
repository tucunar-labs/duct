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


TYPES_SRC = """
import datetime
from decimal import Decimal
import duct

@duct.function
def next_day(d: datetime.date) -> datetime.date:
    return d + datetime.timedelta(days=1)

@duct.function
def plus_hour(t: datetime.datetime) -> datetime.datetime:
    return t + datetime.timedelta(hours=1)

@duct.function(types=["DECIMAL(10,2)"], returns="DECIMAL(12,2)")
def double_price(p):
    return p * 2

@duct.function
def doubled(xs: list[int]) -> list[int]:
    return [x * 2 for x in xs]

@duct.function(types=["STRUCT(a BIGINT, b VARCHAR)"], returns="STRUCT(n BIGINT, s VARCHAR[])")
def reshape(r):
    return {"n": r["a"] + 1, "s": [r["b"], r["b"].upper()]}

@duct.function(vectorized=True, types=["DECIMAL(10,2)", "DATE[]"], returns="VARCHAR")
def vdesc(p, ds):
    import pyarrow as pa
    return pa.array([f"{a}:{len(b)}" for a, b in zip(p.to_pylist(), ds.to_pylist(), strict=True)])
"""


def test_complex_types(tmp_path: Path) -> None:
    import datetime
    import json
    from decimal import Decimal

    (tmp_path / "t.py").write_text(TYPES_SRC)
    w = Worker(tmp_path)
    try:
        specs = {f["name"]: f for f in json.loads(w.ready[1:])["functions"]}
        assert specs["next_day"]["duckdb_returns"] == "DATE"
        assert specs["double_price"]["duckdb_returns"] == "DECIMAL(12,2)"
        assert specs["plus_hour"]["duckdb_args"] == ["TIMESTAMP"]
        assert specs["doubled"]["duckdb_returns"] == "BIGINT[]"
        assert specs["reshape"]["duckdb_returns"] == "STRUCT(n BIGINT, s VARCHAR[])"

        st, res = w.call("next_day", pa.table({"c0": pa.array([datetime.date(2026, 1, 31), None])}))
        assert st == 0 and res == [datetime.date(2026, 2, 1), None]
        ts = datetime.datetime(2026, 1, 1, 23, 30, 0, 5)
        st, res = w.call("plus_hour", pa.table({"c0": pa.array([ts], type=pa.timestamp("us"))}))
        assert st == 0 and res == [ts + datetime.timedelta(hours=1)]
        st, res = w.call(
            "double_price",
            pa.table({"c0": pa.array([Decimal("12.34")], type=pa.decimal128(10, 2))}),
        )
        assert st == 0
        assert res == [Decimal("24.68")]
        st, res = w.call("doubled", pa.table({"c0": pa.array([[1, 2], [], None, [3]])}))
        assert st == 0 and res == [[2, 4], [], None, [6]]
        struct = pa.array(
            [{"a": 1, "b": "x"}], type=pa.struct([("a", pa.int64()), ("b", pa.string())])
        )
        st, res = w.call("reshape", pa.table({"c0": struct}))
        assert st == 0 and res == [{"n": 2, "s": ["x", "X"]}]
        st, res = w.call(
            "vdesc",
            pa.table(
                {
                    "c0": pa.array([Decimal("1.50")], type=pa.decimal128(10, 2)),
                    "c1": pa.array([[datetime.date(2026, 1, 1)] * 3], type=pa.list_(pa.date32())),
                }
            ),
        )
        assert st == 0 and res == ["1.50:3"]
    finally:
        w.close()


def test_type_grammar() -> None:
    from duct.udf.types import arrow_type, duckdb_type, parse_type

    assert duckdb_type("decimal(10, 2)") == "DECIMAL(10,2)"
    assert duckdb_type("numeric") == "DECIMAL(18,3)"
    assert duckdb_type('struct(a int, "b c" text[])') == 'STRUCT(a INTEGER, "b c" VARCHAR[])'
    assert duckdb_type("list[list[float]]") == "DOUBLE[][]"
    assert duckdb_type("datetime.date | None") == "DATE"
    assert duckdb_type("decimal.Decimal") is None
    assert duckdb_type("DECIMAL(40,2)") is None
    assert arrow_type("TIMESTAMP") == pa.timestamp("us")
    assert arrow_type("DECIMAL(10,2)[]") == pa.list_(pa.decimal128(10, 2))
    with pytest.raises(ValueError, match="unsupported"):
        parse_type("JSON")


TABLE_SRC = """
import pyarrow as pa
import duct

@duct.table_function(columns={"n": "BIGINT", "sq": "BIGINT"})
def squares(upto: int):
    for lo in range(0, upto, 4):
        hi = min(lo + 4, upto)
        yield pa.record_batch({"n": list(range(lo, hi)), "sq": [i * i for i in range(lo, hi)]})

@duct.table_function(columns=[("d", "DATE"), ("tags", "VARCHAR[]")], types=["DATE", "INTEGER"])
def days(start, n):
    import datetime
    return [{"d": start + datetime.timedelta(days=i), "tags": ["a"] * i} for i in range(n)]

@duct.table_function(columns={"x": "DOUBLE"})
def cols(k: int):
    return {"x": [1, 2, 3][:k]}

@duct.table_function(columns={"x": "DOUBLE"})
def empty():
    return []
"""


def _call_table(w: Worker, name: str, table: Any) -> Any:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as wr:
        wr.write_table(table)
    nb = name.encode()
    payload = struct.pack(">H", len(nb)) + nb + sink.getvalue().to_pybytes()
    assert w.p.stdin is not None
    w.p.stdin.write(struct.pack(">I", len(payload)) + payload)
    w.p.stdin.flush()
    body = w._read()  # pyright: ignore[reportPrivateUsage]
    assert body[0] == 0, body[1:]
    return ipc.open_stream(pa.py_buffer(body[1:])).read_all()


def test_table_functions(tmp_path: Path) -> None:
    import datetime
    import json

    (tmp_path / "t.py").write_text(TABLE_SRC)
    w = Worker(tmp_path)
    try:
        specs = {f["name"]: f for f in json.loads(w.ready[1:])["functions"]}
        assert specs["squares"]["kind"] == "table"
        assert specs["squares"]["columns"] == [["n", "BIGINT"], ["sq", "BIGINT"]]
        assert specs["days"]["duckdb_args"] == ["DATE", "INTEGER"]
        assert specs["days"]["columns"][1] == ["tags", "VARCHAR[]"]

        t = _call_table(w, "squares", pa.table({"c0": pa.array([10], pa.int64())}))
        assert t.column("n").to_pylist() == list(range(10))
        assert t.column("sq").to_pylist() == [i * i for i in range(10)]
        t = _call_table(
            w,
            "days",
            pa.table(
                {"c0": pa.array([datetime.date(2026, 1, 30)]), "c1": pa.array([2], pa.int32())}
            ),
        )
        assert t.to_pylist() == [
            {"d": datetime.date(2026, 1, 30), "tags": []},
            {"d": datetime.date(2026, 1, 31), "tags": ["a"]},
        ]
        t = _call_table(w, "cols", pa.table({"c0": pa.array([2], pa.int64())}))
        assert t.column("x").to_pylist() == [1.0, 2.0]
        t = _call_table(w, "empty", pa.table({}))
        assert t.num_rows == 0 and t.schema.names == ["x"]
    finally:
        w.close()
