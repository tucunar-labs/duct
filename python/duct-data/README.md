# duct-data

Python SDK for [Duct](https://github.com/) (import name: `duct`). It talks to
`duct serve` over HTTP (Arrow) or the Postgres wire and never holds lake credentials.

```python
import duct

# http(s):// -> FORMAT ArrowStream (pyarrow) or JSON; postgres(ql):// -> psycopg (password = token)
with duct.connect("http://localhost:8123", token="...") as con:  # or DUCT_URL / DUCT_TOKEN
    res = con.sql("SELECT 1 AS x")
    res.rows, res.to_arrow(), res.to_pandas(), res.to_polars()


@duct.function  # functions/*.py; scalar: int -> BIGINT, float -> DOUBLE, str -> VARCHAR, bool -> BOOLEAN,
# datetime.date -> DATE, datetime.datetime -> TIMESTAMP, list[int] -> BIGINT[]
def double(x: int) -> int:
    return x * 2


# vectorized: Arrow arrays in and out
@duct.function(vectorized=True, types=["DOUBLE", "DOUBLE"], returns="DOUBLE")
def hypot(a, b): ...


# DECIMAL(p,s) and STRUCT have no annotation: Decimal / dict in, types=/returns= say which
@duct.function(types=["DECIMAL(10,2)"], returns="STRUCT(net DECIMAL(12,2), tax DECIMAL(12,2))")
def split_tax(price): ...


# table function: SELECT * FROM squares(10); the schema is declared, rows come back as Arrow
@duct.table_function(columns={"n": "BIGINT", "sq": "BIGINT"})
def squares(upto: int):
    yield pa.record_batch({"n": list(range(upto)), "sq": [i * i for i in range(upto)]})


@duct.table(name="silver.clean_events", expectations=[duct.Expectation("id_ok", "id IS NOT NULL")])
def clean_events(): ...
```

Extras: `duct-data[arrow]`, `[pandas]`, `[polars]`, `[pg]`, `[marimo]`.

List registered objects (used by the Go core): `python -m duct.registry --json --load functions/`.
UDF worker (used by the Go core): `python -m duct.udf.worker --load functions/`.
Notebooks behind Duct auth: `python -m duct.notebook.asgi` (started by `duct serve`'s notebook protocol).
Batch scoring: `duct.mlflow.score("models:/m/1", name=..., inputs=[...])` inside `functions/*.py`.

License: Apache-2.0.
