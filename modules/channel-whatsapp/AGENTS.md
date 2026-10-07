# channel-whatsapp

- **Purpose:** AlertChannel over the WhatsApp Business **Cloud API** (Meta), ADR 0009. Sends
  **template** messages (category *utility*, approved in advance), never free text, so alerts
  arrive outside the 24-hour customer window. Credentials are per installation.
  whatsmeow is out (violates Meta's ToS).
- **Contract:** `core/contract/v1` `AlertChannel`; registers as `alert.channel/whatsapp`.
- **Config** (`[alerts.channels.<name>]`, `type = "whatsapp"`): `phone_number_id`, `token_env`
  (env var with a permanent token), `to` (number(s), international, no `+`), `template`,
  `language` (default `es`), `params` (which fields fill the body variables `{{1}}...`; default
  `state, rule, key, message`; also `severity`, `at` or a label name), `api_url`
  (`https://graph.facebook.com`), `api_version` (default `v23.0`: check Meta's current one),
  `timeout`.
- **Request:** `POST {api_url}/{api_version}/{phone_number_id}/messages` with
  `Authorization: Bearer`, body `{messaging_product, to, type: template, template: {name,
  language, components: [{type: body, parameters: [{type: text, text}]}]}}`, one request per
  recipient. Variables are sanitised to what Meta accepts (no newlines/tabs/4 spaces, <= 1024
  chars, never empty).
- **Template to create in Meta Business Manager** (4 body variables, matches the default
  `params`): `Duct: alerta {{1}} - regla {{2}} - clave {{3}} - {{4}}`.
- **Tests:** conformance suite plus `httptest` cases. Nothing in this repo ever calls Meta, and
  there are no credentials: a real send needs a Business account, a verified phone number and
  an approved template.
- **Status:** experimental (untested against the real API).
