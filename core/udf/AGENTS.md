# core/udf

- **Purpose:** loads the project's `functions/` (ADR 0005) and registers each function in an engine
  that implements `contract.ScalarRegistrar` (`engine-duckdb`). No cgo, no dependency on a module.
- **Python:** `functions/*.py` with `@duct.function`. Discovery = `python -m duct.registry --json
  --load functions/` (schema 2, SQL types in `duckdb_args`/`duckdb_returns`); execution = pool of
  `python -m duct.udf.worker` processes (lazy, `Options.Workers`, default 2) speaking length-framed
  Arrow IPC over stdin/stdout. A function error leaves the worker alive; I/O errors and timeouts
  (default 60 s per batch) kill it and the pool starts another.
- **Executables:** `functions/<name>.toml` (`command` as argv, `args`, `returns`, `format` tsv|json,
  `timeout`): one process per batch, one input line per row, exactly one output line per row.
- **Environment:** children get `SafeEnv` (allow-list: PATH, HOME, VIRTUAL_ENV, UV_*, MLFLOW_TRACKING_URI...),
  never `DUCT_*` or `AWS_*`. Tests: `TestPythonFunctions` checks it.
- **Interpreter:** `Options.Python` (argv prefix); `cmd/duct` fills it from `project.PythonCommand()`.
- **Guard:** the names returned by `Attach` go to `governance.HandlerOptions.AdminOnlyFunctions`.
- **Tests:** `udf_test.go` (fake registrar; Python tests need `python/.venv` or `DUCT_TEST_PYTHON`, else skip);
  `BenchmarkPythonBatch` measures the round trip. End to end in `cmd/duct/functions_test.go`.
- **Status:** experimental.
