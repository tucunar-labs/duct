# pyright: basic
import sys
import types
from typing import Any

import pyarrow as pa
import pytest

import duct.mlflow
from duct import registry


class FakeModel:
    def predict(self, df: Any) -> Any:
        return (df["tenure"] * 2 + df["spend"]).to_numpy()


def test_score_registers_vectorized_udf(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded: list[str] = []
    pyfunc = types.ModuleType("mlflow.pyfunc")
    pyfunc.load_model = lambda uri: loaded.append(uri) or FakeModel()  # type: ignore[attr-defined]
    pkg = types.ModuleType("mlflow")
    pkg.pyfunc = pyfunc  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "mlflow", pkg)
    monkeypatch.setitem(sys.modules, "mlflow.pyfunc", pyfunc)

    duct.mlflow.score(
        "models:/churn/1",
        name="churn",
        inputs=["DOUBLE", "DOUBLE"],
        columns=["tenure", "spend"],
    )
    (spec,) = registry.functions()
    assert spec.vectorized
    assert spec.duckdb_args == ["DOUBLE", "DOUBLE"]
    assert spec.duckdb_returns == "DOUBLE"
    out = spec.fn(pa.array([1.0, 2.0]), pa.array([10.0, 20.0]))
    assert out.to_pylist() == [12.0, 24.0]
    spec.fn(pa.array([1.0]), pa.array([1.0]))
    assert loaded == ["models:/churn/1"]  # loaded once


def test_score_validates_columns() -> None:
    with pytest.raises(ValueError, match="same length"):
        duct.mlflow.score("m", name="x", inputs=["DOUBLE"], columns=["a", "b"])
