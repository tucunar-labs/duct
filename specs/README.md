# specs/

JSON Schemas for Duct's own YAML formats (pipelines, dashboards, alerts, ...).

- `ingest.schema.json`: `ingest/<name>.yaml` (M2). Enforced in Go by `core/ingest`
  (`Parse` rejects unknown keys); `core/ingest` tests keep both in step.
