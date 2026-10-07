# 0015. Estructura del repo

Estado: aceptada (2026-10-06)

## Decisión: núcleo + módulos profundos enchufables (estilo Caddy)
```
duct/
├─ AGENTS.md            # mapa, comandos, reglas (corto; Codex corta a 32 KiB)
├─ go.mod               # un solo módulo Go por ahora
├─ mise.toml            # versiones fijas de herramientas
├─ justfile             # `just check` = generar + lint + tests rápidos (<60 s)
├─ lefthook.yml         # pre-commit
├─ core/
│  ├─ contract/         # interfaces y tipos versionados (contract/v1): Source, Sink,
│  │                    # TaskType, AlertChannel, Protocol, Compiler, AuthProvider…
│  └─ registry/         # registro de módulos; activación por config o build tag
├─ modules/<nombre>/    # autocontenido: código, AGENTS.md, tests y golden tests propios
├─ cmd/duct/            # arma el binario con core + módulos elegidos
├─ python/duct-data/    # SDK (uv workspace) → PyPI
├─ specs/               # JSON Schema de los YAML propios
├─ docs/adr/            # estas decisiones
├─ examples/            # proyectos de punta a punta = tests e2e
└─ test/integration/    # testcontainers: Postgres, Kafka, RustFS
```

## Reglas
- Los módulos solo importan `core/contract`, nunca otro módulo (`depguard` en CI).
- Contratos versionados: un cambio incompatible es `v2`, sin romper a nadie.
- Suites de contrato comunes: todo `Source`, `Sink`, `AlertChannel`… debe pasarlas.
- Cada módulo tiene su `AGENTS.md` (qué hace, su contrato, si está congelado).
- Un módulo maduro puede extraerse a su propio `go.mod`/repo sin reescribir.
- duckgres como dependencia (`replace` a un fork solo si hay parches); si sus dependencias
  pesan, vendorear podado en `third_party/`.
- DuckDB con cgo: builds en runners nativos (linux/macOS, amd64/arm64), no zig.
- Python: uv, ruff, pyright. Frontend: templ generate, Tailwind standalone, esbuild vía Go.
- Herramientas de agentes: solo `AGENTS.md` (raíz y anidados); `.claude/` para hooks y skills;
  `.mcp.json` con `duct mcp`; `llms.txt` en docs.

## Descartados
- Un módulo sin plugins (límites solo por convención); multi-módulo con `go.work` (coordinar
  versiones, CI más complejo).
