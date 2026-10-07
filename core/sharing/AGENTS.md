# core/sharing

Data sharing (ADR 0014). No cgo, no module imports; talks to the lake through `contract.Engine`
(Duct tables, `lake.current_snapshot()`) and a governed `contract.QueryHandler` (data).

- `store.go`: shares, tables, recipients, grants (migration 7). A recipient = role `share_<n>` +
  user `share_<n>` + tokens (governance). Policies are governance row filters/masks on that role.
- `service.go`: `Ensure` materializes a compacted Parquet copy per (share, recipient, table) when
  the lake snapshot moved *and* the fingerprint of the role view changed; `GC`, `Refresh`, `Purge`.
- `storage*.go`: `LocalStorage` (HMAC-signed expiring URLs, served by modules/sharing-server) and
  `S3Storage` (presigned GET). The location must be OUTSIDE the lake data path (`duct gc` deletes
  unknown files there); `Overlaps` enforces it.
- `parquet.go`: Arrow to Parquet (pqarrow) and Spark schema string; unsupported types are cast.
- `profile.go`: the `.share` file.
