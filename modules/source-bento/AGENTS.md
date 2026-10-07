# source-bento

- **Purpose:** ingest REST/SOAP/GraphQL/WebSocket/gRPC (any Bento input) into
  `bronze.<table>` by embedding Bento as a library (ADR 0003).
- **Contract:** `core/contract/v1` `Source` (batches); registers as `source/bento`. The core
  loads the batches (`contract.Transactor`) so this module never sees the engine.
- **Library:** `github.com/warpstreamlabs/bento` v1.x (MIT; the import path has no `/v4`),
  `public/service` + only `public/components/pure` and `public/components/io`
  (http_client, file, stdin, generate, bloblang, ...). Do not blank-import
  `public/components/all`: it adds cloud SDKs, Kafka, etc. Add component sets deliberately.
- **Options:** `input` (required; a Bento input as YAML map or string, e.g.
  `http_client: {url: ..., verb: GET}`), `processors` (optional list of Bento processors:
  pagination with `generate` + `http`, `unarchive`, `mapping`...). Duct supplies the output.
  Env vars in Bento YAML (`${TOKEN}`) are resolved by Bento.
- **Rows:** each message is JSON. Object -> one row; array -> one row per element; scalar ->
  column `value`. All messages are collected until the input ends (finite inputs only; a
  streaming input never ends: for streaming, run Bento with a Kafka output and let `duct stream`
  land the topic, ADR 0003), then emitted as Arrow batches of
  10 000 rows with one schema inferred over all rows: int64, float64, bool, string
  (nested values and mixed types as JSON text). Whole result is held in memory.
- **Trap:** a bare `http_client` input polls the URL forever (Bento semantics) and never
  ends. For a one-shot or paginated REST read use `generate` (`count: N`, `interval: ""`) +
  the `http` processor, as in the tests.
- **Tests:** `bento_test.go` with `httptest` (single + paginated).
- **Status:** experimental.
