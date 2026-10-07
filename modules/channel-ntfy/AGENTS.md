# channel-ntfy

- **Purpose:** AlertChannel that publishes each alert to an [ntfy](https://ntfy.sh) topic: push
  to Android/iOS (iOS uses ntfy.sh as the APNs relay) and email through the server's SMTP. ADR 0009.
- **Contract:** `core/contract/v1` `AlertChannel`; registers as `alert.channel/ntfy`.
- **Config** (`[alerts.channels.<name>]` with `type = "ntfy"`): `topic` (required), `url`
  (default `https://ntfy.sh`; point it at a self-hosted server), `token_env` (name of an env
  var with an access token, sent as `Authorization: Bearer`), `timeout` (default `10s`).
- **Wire format:** JSON publish (`POST <url>` with `{topic,title,message,priority,tags}`), so
  titles and messages can carry any UTF-8. Title `FIRING: <rule> <key>` / `RESOLVED: ...`;
  priority from `Labels["severity"]` (critical 5, error 4, warning 3, info 2; resolved 2);
  tags are emoji shortcodes (`rotating_light`, `warning`, `white_check_mark`...).
- **Tests:** conformance suite plus `httptest` cases (payload, token, severity mapping, errors).
  Never publishes to the public server in tests.
- **Status:** experimental.
