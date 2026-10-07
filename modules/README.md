# modules/

Each directory is a self-contained, pluggable module (ADR 0015).

Rules:

1. Import only `core/contract/...` (plus `core/registry` to register from `init()`).
   Never import another module or `cmd/`. Enforced by `depguard` in `.golangci.yml`.
2. Have an `AGENTS.md`: what it does, which contract it implements, whether it is
   frozen/experimental.
3. Pass the conformance suite for its contract (`core/contract/v1/contracttest`) and
   keep its own tests and golden files inside the directory.
4. Register in `init()` via `registry.Register`; `cmd/duct/modules.go` selects which
   modules are linked with a blank import.
5. A mature module can be extracted into its own `go.mod`/repo without rewrites.

See `channel-webhook/` for the reference example.
