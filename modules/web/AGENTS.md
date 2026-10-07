# web

- **Purpose:** the Duct workspace UI (ADR 0012): login, catalog browser, SQL editor with
  results, dashboards (ADR 0008). Mounted by `duct serve` like any other protocol.
- **Contract:** `core/contract/v1` `Protocol`; registers as `protocol/web`. Imports only
  `core/contract`, `core/registry` and `core/dashboards` (pure: YAML parsing and the translation of
  filters to semantic requests; no I/O besides reading the files). All data goes through `QueryHandler.Query` with the
  session `Identity`; credentials go only through `AuthProvider.Authenticate`
  (`Scheme: "password"`). No data or credentials are ever stored in the browser.
- **Config:** `listen` (alias `addr`; default `127.0.0.1:8484`, port 0 = random; bound in Init, reported by `Addr()` as `contract.Addresser`), `secure_cookies` (bool; default: Secure
  flag when HTTPS or `X-Forwarded-Proto: https`), `session_ttl` (absolute timeout, default `12h`), `session_idle_timeout` (default `2h`),
  `session_key` / env `DUCT_WEB_SESSION_KEY` (base64 32 bytes), `session_key_file` (generated once, 0600;
  `duct serve` passes `.duct/web_session.key`), `session_key_previous` / env `DUCT_WEB_SESSION_KEY_PREVIOUS`
  (rotation), `trust_proxy` (right-most `X-Forwarded-For`), `login_max_attempts` (5), `login_ip_max_attempts` (20),
  `login_lockout` (15m, doubles on repeats, max 24h),
  `row_limit` (default 1000), `query_timeout` (default `60s`),
  `dashboards_dir` (folder with `dashboards/*.yaml`; `duct serve` passes `<project>/dashboards`; unset: none).
- **Routes:** `/login` (GET/POST), `/logout` (POST), `/catalog`, `/sql`, `POST /sql/run`
  (Datastar SSE), `/dashboards`, `/dashboards/{name}`, `POST /dashboards/{name}/data` (SSE), `/static/*`.
- **Dashboards:** the YAML is re-read on every request (edit and reload). Charts never run SQL: they call the
  OPTIONAL `contract.SemanticQuerier` of the QueryHandler (`SemanticQuery` as the session identity,
  `LakeSnapshot`); without it (or without a model) every chart shows an error panel. `POST .../data` gets
  the Datastar signals (`f_<filter>`, `cross`, `ev`), rebuilds the state from the YAML (the browser never
  names a field), and streams one `<duct-chart>` patch per chart. Cache (`lru.go`): key = sha256(latest
  DuckLake snapshot, role, request), 256 entries / 32 MB, errors never cached, no snapshot = no cache.
  Cross-filter: the island emits `ductcross` (click: the value; brush: a range of categories), Datastar
  sets signal `ev`, the server toggles it into signal `cross` (source chart -> values of its first dim)
  and applies it to every chart but the source (`cross_filter: false` opts a chart out). Cache hits skip the
  audit log (the first query of each snapshot is audited).
- **Security:** session cookie `HttpOnly`+`SameSite=Strict`; sessions are stateless: the cookie is
  XChaCha20-Poly1305 (identity, id, CSRF secret, timestamps, key id) so they survive restarts; logout
  uses an in-memory denylist (a restart forgets it; stolen cookies live until idle/absolute timeout);
  idle timeout slides by re-sealing the cookie; failed logins are limited per user and per IP (429 +
  `Retry-After`, generic message, minimum latency); POSTs need a CSRF token (login: double-submit cookie; the rest: HMAC of the
  session id, in form field `csrf` or header `X-CSRF-Token`) plus same-origin checks
  (`Origin`, `Sec-Fetch-Site`); strict CSP (`'unsafe-eval'` only because Datastar needs it).
  Output is escaped by templ.
- **Stack and build (no Node):**
  - templ `v0.3.1070` (`ui.templ` -> `ui_templ.go`, committed) and Tailwind standalone
    `4.3.3` (`ui/input.css` -> `static/app.css`, committed). Both run in `go generate`
    (`just generate`); `just check` fails if the committed output is stale. Pinned in
    `mise.toml`. Plain templ components, no templUI (see note in `ui.templ`).
  - Datastar client `v1.0.4`, vendored as `static/datastar.js`
    (sha256 `727844adfc825ee651fb93c544a2a739986f9a21820a94524b35f0cac470cf91`, from
    `raw.githubusercontent.com/starfederation/datastar/v1.0.4/bundles/datastar.js`).
    Go SDK `datastar-go v1.2.2`. Bumping: replace both, update this hash.
  - CodeMirror 6 island: `static/editor.js` is built by `just bundle` (`tools/bundle`,
    esbuild as a Go library) from `tools/bundle/web/editor.js`; the npm tarballs are pinned
    with sha512 integrity in `tools/bundle/web/package-lock.json`. The output is committed.
  - ECharts island: `static/charts.js` (`<duct-chart>` custom element in a shadow root; ECharts 6.1.0 +
    zrender 6.1.0 + tslib 2.3.0 tree-shaken: line, bar, pie, scatter, heatmap, grid, tooltip, legend,
    visualMap, brush, canvas renderer) built by the same `just bundle` from `tools/bundle/web/charts.js`;
    KPI and table need no ECharts. Light/dark themes follow `prefers-color-scheme` with the brand palette.
    CSP is unchanged: the island needs no `eval`, inline script or other origin.
  - Everything under `static/` is `go:embed`ed and served with content-hashed URLs. The brand
    SVGs in `static/brand/` are copies of `assets/brand/` (palette in its README).
- **Tests:** `web_test.go` (httptest with fake QueryHandler/AuthProvider): `session_test.go` (restart, tamper, timeouts, rotation, logout, rate limits); login/CSRF/cookies,
  redirects, catalog, query, truncation, errors, escaping, static, Serve lifecycle; `dashboards_test.go`
  (fake SemanticQuerier: render, filters, cross-filter, cache by snapshot/role, role without access, LRU).
- **Status:** experimental (M5 minimal web + dashboards; no notebooks, admin yet).
