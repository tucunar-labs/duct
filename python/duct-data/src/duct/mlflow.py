"""Batch scoring of an MLflow model from SQL (ADR 0010).

In a ``functions/*.py`` file::

    import duct.mlflow

    duct.mlflow.score(
        "models:/churn/Production",
        name="churn_score",
        inputs=["DOUBLE", "DOUBLE"],
        returns="DOUBLE",
    )

then ``SELECT churn_score(tenure, spend) FROM gold.customers`` in ``duct query``,
``duct run`` pipelines or jobs. The model is loaded once per worker process, on first use,
with ``mlflow.pyfunc.load_model`` and called on one batch of rows at a time (Arrow in, Arrow
out). Needs ``mlflow`` in the Python environment (``duct enable mlflow`` documents how) and
``MLFLOW_TRACKING_URI`` pointing at ``duct mlflow``'s server (``duct`` sets it for you).
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from typing import Any

from duct import registry


def score(
    model_uri: str,
    *,
    name: str,
    inputs: Sequence[str],
    returns: str = "DOUBLE",
    columns: Sequence[str] | None = None,
) -> Callable[..., Any]:
    """Register ``name(...)`` as a vectorized UDF that scores rows with ``model_uri``.

    ``inputs`` are the DuckDB types of the arguments; ``columns`` the feature names the model
    expects (default ``c0``, ``c1``...). ``returns`` is the DuckDB type of the prediction.
    Models that return several columns must be wrapped by the user: the UDF returns one value
    per row (the first column of a DataFrame, or the array itself).
    """
    names = list(columns) if columns is not None else [f"c{i}" for i in range(len(inputs))]
    if len(names) != len(inputs):
        raise ValueError("columns and inputs must have the same length")
    model: list[Any] = []  # loaded lazily, once per process

    def run(*arrays: Any) -> Any:
        if not model:
            pyfunc = importlib.import_module("mlflow.pyfunc")
            model.append(pyfunc.load_model(model_uri))
        pa = importlib.import_module("pyarrow")
        table = pa.table(dict(zip(names, arrays, strict=True)))
        pred = model[0].predict(table.to_pandas())
        if hasattr(pred, "iloc") and getattr(pred, "ndim", 1) == 2:
            pred = pred.iloc[:, 0]
        return pa.array(pred)

    run.__name__ = name
    registry.function(name=name, vectorized=True, types=list(inputs), returns=returns)(run)
    return run
