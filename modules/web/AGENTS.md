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
- **Routes:** `/login` (GET/POST), `/logout` (POST), `/catalog`, `/catalog/{schema}/{table}`,
  `/sql`, `GET /catalog.json` (role-visible schemas/tables/columns for drag & drop and autocompletion), `POST /sql/run` (Datastar SSE), `/dashboards`, `/dashboards/{name}`,
  `POST /dashboards/{name}/data` (SSE), `/static/*`.
- **Shell:** server-rendered frame (templ) with a collapsible sidebar (Catalog, SQL editor,
  Dashboards, Jobs/Admin placeholders — Admin only for role `duct_admin`), breadcrumbs, a user
  menu and a light/dark/system toggle. The `static/shell.js` island (a classic script loaded in
  `<head>` so the theme applies before first paint) persists the theme and sidebar in
  `localStorage`, filters the schema trees client-side, drives the tabs and the editor/results
  splitter, and keeps worksheets and the query history in `localStorage` (nothing leaves the
  browser). Tabs are keyboard-navigable (`role="tab"` + arrows); contrast is AA.
- **Catalog:** `/catalog` shows a searchable schema → table → columns tree plus an overview;
  `/catalog/{schema}/{table}` adds tabs Columnas (name, type, nullable), Vista previa (first 100
  rows) and Detalles. Nullability comes from `is_nullable` (the governed `Catalog` stream carries
  it); the row count runs `SELECT count(*)` through `QueryHandler` as the session identity and the
  last snapshot through the OPTIONAL `contract.SemanticQuerier.LakeSnapshot`; whatever governance
  denies shows as "—". "Abrir en editor SQL" deep-links to `/sql?q=…`.
- **SQL editor:** DBeaver-style schema browser on the left (click inserts at the cursor, double-click previews 100 rows, right-click menu, arrows/Enter, Ctrl/Cmd+click multi-selects columns, width and collapse persisted). Rows are HTML5-draggable with a custom payload (`application/json`); the editor's `domEventHandlers` resolve the drop with `posAtCoords`, draw a drop caret and return true so CodeMirror never inserts the raw text. Table on an empty editor/line -> `SELECT cols FROM s.t LIMIT 100`; Alt or inside a statement -> `s.t`. Autocompletion comes from `/catalog.json` through `lang-sql` (schema., table./alias. prefixes, keywords). CodeMirror on
  top, results below with a draggable splitter. `Ctrl/Cmd+Enter` (or Ejecutar) runs the selection
  if there is one, else the whole worksheet: the island keeps the selection in signal `sel` and
  the server prefers it over `sql`. Results have sticky headers, type badges, per-cell/per-row
  copy, CSV export and an error panel with `Lx:Cy` when the engine reports a position. Worksheets
  and history are client-side only.
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
    The shell island (`static/shell.js`, from `tools/bundle/web/shell.js`, no npm deps) is
    built the same way and is tiny; each page loads only the islands it needs (editor on
    `/sql`, charts on dashboards, shell everywhere).
  - ECharts island: `static/charts.js` (`<duct-chart>` custom element in a shadow root; ECharts 6.1.0 +
    zrender 6.1.0 + tslib 2.3.0 tree-shaken: line, bar, pie, scatter, heatmap, grid, tooltip, legend,
    visualMap, brush, canvas renderer) built by the same `just bundle` from `tools/bundle/web/charts.js`;
    KPI and table need no ECharts. Light/dark themes follow `html[data-theme]` (the shell
    toggle) or `prefers-color-scheme` with the brand palette; each chart panel shows a
    skeleton until the island paints (or reports an error).
    CSP is unchanged: the island needs no `eval`, inline script or other origin.
  - Everything under `static/` is `go:embed`ed and served with content-hashed URLs. The brand
    SVGs in `static/brand/` are copies of `assets/brand/` (palette in its README).
- **Tests:** `web_test.go` (httptest with fake QueryHandler/AuthProvider): `session_test.go` (restart, tamper, timeouts, rotation, logout, rate limits); login/CSRF/cookies,
  redirects, catalog, table detail (columns/preview/details, 404, nullable), admin-only nav, shell
  markers, run selection, query, truncation, errors, escaping, `errPos`, static, Serve lifecycle; `dashboards_test.go`
  (fake SemanticQuerier: render, filters, cross-filter, cache by snapshot/role, role without access, LRU).
- **Status:** experimental (M5 web + dashboards; Jobs/Admin are placeholders, no notebooks yet).
