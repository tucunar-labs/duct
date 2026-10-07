# channel-webhook

- **Purpose:** AlertChannel that POSTs each alert as JSON to a URL (Slack, Teams,
  Discord, any HTTP endpoint). See ADR 0009.
- **Contract:** `core/contract/v1` `AlertChannel`; registers as `alert.channel/webhook`.
- **Config:** `url` (required), `timeout` (Go duration, default `10s`), `headers` (map).
- **Tests:** conformance suite `contracttest.TestAlertChannel` plus `httptest` cases.
- **Status:** experimental (payload shape may change until the alerts layer lands).
