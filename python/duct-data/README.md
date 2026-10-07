# duct-data

Python SDK for [Duct](https://github.com/) (import name: `duct`). It talks to
`duct serve` over HTTP (Postgres wire planned) and never holds lake credentials.

```python
import duct

with duct.connect("http://localhost:8080", token="...") as con:  # or DUCT_URL / DUCT_TOKEN
    rows = con.sql("SELECT 1 AS x").rows


@duct.function
def double(x: int) -> int:
    return x * 2


@duct.table(name="silver.clean_events", expectations=[duct.Expectation("id_ok", "id IS NOT NULL")])
def clean_events(): ...
```

Extras: `duct-data[arrow]`, `[pandas]`, `[polars]`, `[pg]`, `[marimo]`.

List registered objects (used by the Go core): `python -m duct.registry --json --load functions/`.

License: Apache-2.0.
