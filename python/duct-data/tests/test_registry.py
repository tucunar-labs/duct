import datetime
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import duct
from duct import registry


def test_function_bare_and_options() -> None:
    @duct.function
    def double(x: int) -> int:
        return x * 2

    @duct.function(name="plus", vectorized=True)
    def add(a: float, b: float) -> float:
        return a + b

    specs = {s.name: s for s in registry.functions()}
    assert specs["double"].input_types == ["int"]
    assert specs["double"].return_type == "int"
    assert not specs["double"].vectorized
    assert specs["plus"].vectorized
    assert specs["plus"].input_types == ["float", "float"]
    assert double(2) == 4


def test_table_registers_expectations() -> None:
    exp = duct.Expectation("id_ok", "id IS NOT NULL", "drop_row")

    @duct.table(name="silver.events", expectations=[exp])
    def events() -> None: ...

    (spec,) = registry.tables()
    assert spec.name == "silver.events"
    assert spec.expectations == [exp]


def test_duplicate_rejected() -> None:
    @duct.function
    def f() -> None: ...

    with pytest.raises(ValueError, match="duplicate"):

        @duct.function(name="f")
        def g() -> None: ...


def test_to_dict_is_json() -> None:
    @duct.function
    def f(x: int) -> int:
        return x

    d = json.loads(json.dumps(registry.to_dict()))
    assert d["schema_version"] == 3
    assert d["functions"][0]["name"] == "f"
    assert "fn" not in d["functions"][0]


def test_cli_json(tmp_path: Path) -> None:
    (tmp_path / "udfs.py").write_text(
        "import duct\n\n@duct.function\ndef inc(x: int) -> int:\n    return x + 1\n"
    )
    out = subprocess.run(
        [sys.executable, "-m", "duct.registry", "--json", "--load", str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(out.stdout)["functions"][0]["name"] == "inc"
    assert out.stderr == ""


def test_duckdb_types_resolved() -> None:
    @duct.function
    def f(a: int, b: float, c: str, d: bool, e: float | None) -> str:
        return ""

    @duct.function(vectorized=True, types=["INT", "double"], returns="BIGINT")
    def g(a: Any, b: Any) -> Any:
        return a

    @duct.function
    def h(x: dict[str, int]) -> None: ...

    @duct.function
    def k(a: list[int], b: datetime.date, c: datetime.datetime | None) -> list[str]:
        return []

    specs = {s.name: s for s in registry.functions()}
    assert specs["f"].duckdb_args == ["BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN", "DOUBLE"]
    assert specs["f"].duckdb_returns == "VARCHAR"
    assert specs["g"].duckdb_args == ["INTEGER", "DOUBLE"]
    assert specs["g"].duckdb_returns == "BIGINT"
    assert specs["h"].duckdb_args == [None]
    assert specs["h"].duckdb_returns is None
    assert specs["k"].duckdb_args == ["BIGINT[]", "DATE", "TIMESTAMP"]
    assert specs["k"].duckdb_returns == "VARCHAR[]"


def test_types_length_checked() -> None:
    with pytest.raises(ValueError, match="types="):

        @duct.function(types=["DOUBLE"])
        def f(a: float, b: float) -> float:
            return a
