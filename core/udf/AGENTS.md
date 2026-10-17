# core/udf

- **Purpose:** loads the project's `functions/` (ADR 0005) and registers each function in an engine
  that implements `contract.ScalarRegistrar` (and `TableRegistrar` for table functions): `engine-duckdb`.
  No cgo, no dependency on a module.
- **Types:** one grammar for Python, executables and engine (`contract.ParseSQLType`, canonical
  spelling in `String()`): BOOLEAN..DOUBLE, VARCHAR, DATE, TIMESTAMP (us, no zone), DECIMAL(p,s), `T[]`,
  `STRUCT(a T, ...)`. Python twin: `duct/udf/types.py`; change both together.
- **Table functions:** `@duct.table_function(columns=...)`; constant args, whole result as Arrow batches
  (`pyPool.tableCaller`), CLI/jobs only (`duct serve` guard denies table functions for everyone).
  No aggregate UDFs: duckdb-go has no binding for `duckdb_create_aggregate_function` (use
  `my_fn(list(x))` with a list argument).
- **Reload (`Manager`):** DuckDB cannot unregister a function (registering the same signature replaces it, another one adds an overload), so each registered
  function is a stub (`slot`) that forwards to an atomically swapped `impl`. `Reload` discovers first
  (error = nothing changes), registers new names, swaps bodies, marks vanished names as removed (they
  keep failing until restart), refuses signature changes, and replaces the `pyPool` (`Close` lets
  in-flight batches finish; callers that hit a closed pool retry on the new one). `SetGuard` is called
  with the full name list before registering new names. `cmd/duct/functions_serve.go` polls `functions/`.
- **Python:** `functions/*.py` with `@duct.function`. Discovery = `python -m duct.registry --json
  --load functions/` (schema 2, SQL types in `duckdb_args`/`duckdb_returns`); execution = pool of
  `python -m duct.udf.worker` processes (lazy, `Options.Workers`, default 2) speaking length-framed
  Arrow IPC over stdin/stdout. A function error leaves the worker alive; I/O errors and timeouts
  (default 60 s per batch, counted once the worker is ready; cold start has its own `StartTimeout`, 2 min) kill it and the pool starts another.
- **Executables:** `functions/<name>.toml` (`command` as argv, `args`, `returns`, `format` tsv|json,
  `timeout`): one process per batch, one input line per row, exactly one output line per row.
- **Environment:** children get `SafeEnv` (allow-list: PATH, HOME, VIRTUAL_ENV, UV_*, MLFLOW_TRACKING_URI...),
  never `DUCT_*` or `AWS_*`. Tests: `TestPythonFunctions` checks it.
- **Interpreter:** `Options.Python` (argv prefix); `cmd/duct` fills it from `project.PythonCommand()`.
- **Guard:** the names returned by `Attach` go to `governance.HandlerOptions.AdminOnlyFunctions`.
- **Tests:** `udf_test.go` (fake registrar; Python tests need `python/.venv` or `DUCT_TEST_PYTHON`, else skip);
  `BenchmarkPythonBatch` measures the round trip. End to end in `cmd/duct/functions_test.go`.
- **Status:** experimental.
- **Not supported:** aggregate functions, table-function named args, TIMESTAMP WITH TIME ZONE, MAP/UNION/ENUM.
