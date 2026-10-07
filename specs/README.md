# specs/

JSON Schemas for Duct's own YAML formats (pipelines, dashboards, alerts, ...).

- `ingest.schema.json`: `ingest/<name>.yaml` (M2). Enforced in Go by `core/ingest`
  (`Parse` rejects unknown keys); `core/ingest` tests keep both in step.
- `jobs.schema.json`: `jobs/<name>.yaml` (orchestrator, ADR 0005). Enforced in Go by
  `core/orchestrator` (`Parse` rejects unknown keys); its tests keep both in step.
- `stream.schema.json`: `streams/<name>.yaml` (Kafka -> Bronze, ADR 0003). Enforced in Go by
  `core/stream` (`Parse` rejects unknown keys); `core/stream` tests keep both in step.
