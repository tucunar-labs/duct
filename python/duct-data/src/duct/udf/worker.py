"""Long-lived UDF worker, driven by the Go core over stdin/stdout.

Wire format (all integers big endian), one request answered by one response:

* request frame:  ``u32 length`` + ``u16 name_length`` + ``name`` (UTF-8) + Arrow IPC stream
  (schema + one record batch holding one column per argument, named ``c0``, ``c1``...)
* response frame: ``u32 length`` + ``u8 status`` + body. Status 0: Arrow IPC stream with one
  column ``result``; status 1: UTF-8 error message. A ``@table_function`` gets one row of
  arguments and answers with an Arrow IPC stream of its declared columns (any number of rows).

On start the worker sends one ready frame (status 0, JSON body with the registry snapshot).
stdout is reserved for the protocol: user ``print`` output is redirected to stderr.
Requires ``pyarrow`` (``duct-data[arrow]``). A zero length frame, or EOF, ends the worker.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import struct
import sys
import traceback
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, BinaryIO, cast

from duct import registry
from duct.registry.__main__ import _load as load_path  # pyright: ignore[reportPrivateUsage]
from duct.udf.types import arrow_type

pa: Any = importlib.import_module("pyarrow")
ipc: Any = importlib.import_module("pyarrow.ipc")

MAX_FRAME = 1 << 30


def _read_exact(f: BinaryIO, n: int) -> bytes | None:
    buf = bytearray()
    while len(buf) < n:
        chunk = f.read(n - len(buf))
        if not chunk:
            return None if not buf else bytes(buf)  # truncated frame: caller decides
        buf += chunk
    return bytes(buf)


def read_frame(f: BinaryIO) -> bytes | None:
    """Read one frame; ``None`` on EOF or the zero-length shutdown frame."""
    head = _read_exact(f, 4)
    if head is None or len(head) < 4:
        return None
    (n,) = struct.unpack(">I", head)
    if n == 0:
        return None
    if n > MAX_FRAME:
        raise ValueError(f"frame too large: {n}")
    body = _read_exact(f, n)
    if body is None or len(body) < n:
        raise EOFError("truncated frame")
    return body


def write_frame(f: BinaryIO, payload: bytes) -> None:
    f.write(struct.pack(">I", len(payload)) + payload)
    f.flush()


def _ipc_bytes(table: Any) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return sink.getvalue().to_pybytes()


def _as_table(res: Any, schema: Any) -> Any:
    """Coerce what a table function returned into a ``pyarrow.Table`` of ``schema``."""
    if isinstance(res, pa.Table):
        parts: list[Any] = [res]
    elif isinstance(res, pa.RecordBatch):
        parts = [pa.Table.from_batches([res])]
    elif isinstance(res, dict):
        parts = [pa.table(res)]
    elif isinstance(res, list) and res and isinstance(res[0], dict):
        parts = [pa.Table.from_pylist(res)]
    else:  # an iterable (generator) of batches, tables, dicts or row lists
        parts = [_as_table(r, schema) for r in cast(Iterable[Any], res)]
    parts = [p.select(schema.names) if p.num_columns else p for p in parts]
    parts = [p.cast(schema) for p in parts if p.num_columns]
    return pa.concat_tables(parts) if parts else schema.empty_table()


def make_table_runner(spec: registry.FunctionSpec) -> Callable[[Any], Any]:
    """Return ``args table (one row) -> pyarrow.Table`` for a ``@table_function``."""
    schema = pa.schema([(n, arrow_type(t)) for n, t in spec.columns])
    fn = spec.fn

    def run_table(table: Any) -> Any:
        args = [table.column(i)[0].as_py() for i in range(table.num_columns)]
        return _as_table(fn(*args), schema)

    return run_table


def make_runner(spec: registry.FunctionSpec) -> Callable[[Any], Any]:
    """Return ``table -> pyarrow.Array`` for one registered scalar function."""
    if spec.duckdb_returns is None:
        raise ValueError(f"function {spec.name}: return type unresolved (use returns=)")
    out_type = arrow_type(spec.duckdb_returns)
    fn = spec.fn

    if spec.vectorized:

        def run_vec(table: Any) -> Any:
            res = fn(*[table.column(i) for i in range(table.num_columns)])
            arr = res if isinstance(res, pa.ChunkedArray | pa.Array) else pa.array(res)
            if isinstance(arr, pa.ChunkedArray):
                arr = arr.combine_chunks()
            if arr.type != out_type:
                arr = arr.cast(out_type)
            return arr

        return run_vec

    def run_scalar(table: Any) -> Any:
        cols = [table.column(i).to_pylist() for i in range(table.num_columns)]
        out: list[Any] = []
        for i, row in enumerate(zip(*cols, strict=True) if cols else [()] * table.num_rows):
            if any(v is None for v in row):  # NULL in, NULL out (DuckDB's default)
                out.append(None)
                continue
            try:
                out.append(fn(*row))
            except Exception as e:
                raise RuntimeError(f"row {i}: {type(e).__name__}: {e}") from e
        return pa.array(out, type=out_type)

    return run_scalar


def _warm_up() -> None:
    """Exercise the IPC round trip once before reporting ready.

    pyarrow initializes parts of IPC and its memory pool lazily; on a slow
    machine that first use costs hundreds of milliseconds and would be charged
    to the first real batch (and its per-batch timeout). User functions are
    never called here.
    """
    sample = pa.table({"i": pa.array([1], type=pa.int64()), "s": pa.array(["x"])})
    ipc.open_stream(pa.py_buffer(_ipc_bytes(sample))).read_all()
    _ipc_bytes(pa.table({"result": pa.array([None], type=pa.int64())}))


def serve(stdin: BinaryIO, proto_out: BinaryIO, load: list[Path]) -> int:
    for p in load:
        load_path(p)
    runners: dict[str, Callable[[Any], Any]] = {}
    tables: set[str] = set()
    for spec in registry.functions():
        runners[spec.name] = make_table_runner(spec) if spec.kind == "table" else make_runner(spec)
        if spec.kind == "table":
            tables.add(spec.name)
    _warm_up()
    write_frame(proto_out, b"\x00" + json.dumps(registry.to_dict()).encode())
    while True:
        frame = read_frame(stdin)
        if frame is None:
            return 0
        (nlen,) = struct.unpack(">H", frame[:2])
        name = frame[2 : 2 + nlen].decode()
        try:
            run = runners[name]
            table = ipc.open_stream(pa.py_buffer(frame[2 + nlen :])).read_all()
            result = run(table)
            if name in tables:
                body = b"\x00" + _ipc_bytes(result)
            else:
                if len(result) != table.num_rows:
                    raise ValueError(f"returned {len(result)} values for {table.num_rows} rows")
                body = b"\x00" + _ipc_bytes(pa.table({"result": result}))
        except KeyError:
            body = b"\x01" + f"unknown function {name!r}".encode()
        except Exception as e:  # report to the caller; the worker stays alive
            traceback.print_exc(file=sys.stderr)
            body = b"\x01" + f"{name}: {type(e).__name__}: {e}".encode()
        write_frame(proto_out, body)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m duct.udf.worker")
    ap.add_argument("--load", action="append", default=[], type=Path)
    args = ap.parse_args(argv)
    # Keep the real stdout for the protocol; anything the user prints goes to stderr.
    proto_out = os.fdopen(os.dup(1), "wb")
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    return serve(sys.stdin.buffer, proto_out, list(args.load))


if __name__ == "__main__":
    raise SystemExit(main())
