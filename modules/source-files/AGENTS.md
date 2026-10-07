# source-files

- **Purpose:** ingest CSV / Parquet / JSON (NDJSON or arrays) from local paths or globs
  into `bronze.<table>` using DuckDB's native readers (ADR 0003).
- **Contract:** `core/contract/v1` `KindSource` + the optional `SQLSource` interface;
  registers as `source/files`. It does not import or call the engine: `Plan` returns
  `SELECT * FROM read_csv|read_parquet|read_json(...)` and the core stages, evolves the
  schema and writes (see `core/ingest`).
- **Options** (`options:` in `ingest/<name>.yaml`):
  - `path` (required): string or list; relative paths resolve against the project root;
    globs (`data/*.csv`) work. `://` URLs (s3, http) are rejected until httpfs and secrets
    land (M4).
  - `format`: `csv` | `parquet` | `json`; inferred from the extension (`.ndjson`/`.jsonl` = json).
  - `reader`: map of extra reader parameters passed verbatim (`header: true`, `delim: ";"`, ...).
    `union_by_name = true` is always on so files with different columns merge.
- **Incremental:** with `mode: incremental` and `watermark: col`, adds
  `WHERE "col" > '<last>'` (DuckDB casts the literal to the column type).
- **Tests:** `files_test.go` (plan SQL, validation); end-to-end CSV -> bronze -> query in
  `cmd/duct/ingest_test.go`.
- **Status:** experimental.
