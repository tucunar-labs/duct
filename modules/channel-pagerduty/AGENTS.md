# channel-pagerduty

- **Purpose:** AlertChannel for the PagerDuty Events API v2 (ADR 0009): a firing alert sends
  `trigger`, its resolution sends `resolve`, both with `dedup_key = <rule>:<key>` (max 255
  chars) so PagerDuty keeps one incident per alert.
- **Contract:** `core/contract/v1` `AlertChannel`; registers as `alert.channel/pagerduty`.
- **Config** (`[alerts.channels.<name>]` with `type = "pagerduty"`): `routing_key_env` (name of
  an env var with the integration key; the key itself never goes in duct.toml), `source`
  (default `duct`), `url` (default `https://events.pagerduty.com/v2/enqueue`), `timeout`.
- **Payload:** `summary` = alert message, `severity` from `Labels["severity"]`
  (critical/error/warning/info, default warning), `component` = rule, `custom_details` =
  labels (the row that fired). `resolve` carries only routing key and dedup key.
- **Tests:** conformance suite plus `httptest` cases (trigger, resolve, dedup key, errors).
  No test ever calls PagerDuty.
- **Status:** experimental.
