# mcp

- **Purpose:** MCP `Protocol` of `duct serve` (Genie v1, ADR 0010/0011): the semantic layer as
  tools for an LLM client over the Streamable HTTP transport.
- **Contract:** `core/contract/v1` `Protocol` (+ optional `Addresser`); registers as
  `protocol/mcp`. Data only through the `QueryHandler`, as the authenticated identity (role views,
  row filters, masks, audit). Uses `core/semantic` to compile requests; the handler's optional
  `Catalog` tells which views the role owns (a role without grant on a dataset gets an error and
  does not see its metrics).
- **SDK:** `github.com/modelcontextprotocol/go-sdk` v1.8.0 (official). Bearer auth with
  `auth.RequireBearerToken`; the verifier is the `AuthProvider` (Duct tokens), identity travels in
  `TokenInfo`. DNS-rebinding protection of the SDK stays on.
- **Config** (`[serve.mcp]`): `listen` (default `127.0.0.1:8765`, non-loopback requires TLS),
  `tls_cert` + `tls_key`, `semantic_dir` (`duct serve` sets `<project>/semantic`), `sql = true`
  to add the read-only `sql` tool (same guard as the other protocols).
- **Tools:** `list_metrics`, `describe(name?)`, `query_metrics(dims, measures, filters, limit, order)`,
  optional `sql(query)`. Errors come back as tool errors (`isError`), so the model can correct itself.
- **Audit:** `query_metrics` and `sql` are audited by the handler (protocol `mcp`, real SQL);
  `list_metrics`, `describe` and requests refused before reaching the handler are audited here as
  `mcp <tool> ...`.
- **Gotchas:** the model is loaded at start (restart to pick up changes); strings from Arrow must
  be cloned before the stream is released.
- **Tests:** `mcp_test.go`; end-to-end with the SDK client in `cmd/duct/semantic_test.go`.
- **Status:** experimental.
