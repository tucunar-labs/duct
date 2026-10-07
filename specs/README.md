# specs/

JSON Schemas for Duct's own YAML formats (pipelines, dashboards, alerts, ...).

- `ingest.schema.json`: `ingest/<name>.yaml` (M2). Enforced in Go by `core/ingest`
  (`Parse` rejects unknown keys); `core/ingest` tests keep both in step.
- `jobs.schema.json`: `jobs/<name>.yaml` (orchestrator, ADR 0005). Enforced in Go by
  `core/orchestrator` (`Parse` rejects unknown keys); its tests keep both in step.
- `stream.schema.json`: `streams/<name>.yaml` (Kafka -> Bronze, ADR 0003). Enforced in Go by
  `core/stream` (`Parse` rejects unknown keys); `core/stream` tests keep both in step.
- `dashboard.schema.json`: `dashboards/<name>.yaml` (ADR 0008/0012). Enforced in Go by
  `core/dashboards` (`Parse` rejects unknown keys); its tests keep both in step.
- `admin.openapi.yaml`: OpenAPI of the admin API (`/admin/v1`, ADR 0013). Implemented in `core/admin`;
  `cmd/duct/admin_test.go` exercises every route group.
