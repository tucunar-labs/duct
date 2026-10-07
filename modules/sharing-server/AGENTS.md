# sharing-server

- **Purpose:** OpenSharing / Delta Sharing REST server of `duct serve` (ADR 0014, phase 2).
  Registers as `protocol/sharing`. Lists the shares granted to the bearer token's recipient and
  hands out short-lived URLs of a Parquet copy already filtered and masked for it.
- **Contract:** `core/contract/v1` `Protocol` (+ `Addresser`) and `core/sharing.EngineUser`
  (`duct serve` calls `UseEngine` before `Serve`: the model and the state of the copies live in
  the Duct tables). Data is read only through a governed `governance.Handler` (own instance,
  30 min timeout) as the recipient's role `share_<name>`; the model, copies and URLs are
  `core/sharing`. This module is only the HTTP layer, auth and audit.
- **Config** (`[serve.sharing]`): `listen` (default `127.0.0.1:8124`; non-loopback needs
  `tls_cert` + `tls_key`), `public_url`, `url_ttl` (15m), `grace` (1h), `rows_per_file`,
  `refresh_interval` (off), `storage` (default `<project>/shares` or `<lake prefix>.shares`;
  must not overlap the lake), `s3_public_endpoint`. Filled by `duct serve`: `signing_key_file`
  (`.duct/sharing.key`), `s3`, `data_path`. Starts only if `[serve.sharing]` exists or with
  `--protocols sharing`.
- **API:** under `/delta-sharing`: `/shares`, `/shares/{s}[/schemas[/{sch}/tables]|/all-tables]`,
  `…/tables/{t}/version|metadata|query`, `…/changes` (501), `/_files/<key>?exp&sig` (local
  storage only). `/healthz`. Errors are `{"errorCode","message"}`.
- **Auth:** `Authorization: Bearer <token>` from `duct recipient create|token`; 401 invalid,
  revoked or expired; 403 token of a non-recipient; 404 for what is not granted.
- **Gotchas:** an error after the first NDJSON byte aborts the connection. A first request after
  a lake change pays the materialization (singleflight per copy). Unsupported column types are
  cast to text in the copy.
- **Tests:** `server_test.go` (real DuckDB engine, protocol, expiry, tampering, audit,
  revocation); end to end with the official client in `cmd/duct/share_test.go` and RustFS in
  `test/integration/share_s3_test.go`.
- **Status:** experimental.
