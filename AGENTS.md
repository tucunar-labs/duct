# AGENTS.md (duct)

Duct: reemplazo FOSS completo de Databricks, local-first. CLI `duct`, un solo binario Go.
Criterio guía: **superficie mínima** y estándares abiertos. Por ahora solo se diseña el
FOSS; Duct Cloud (modelo silo) va después. Estado y versión actual: ver `BACKLOG.md`.

## Stack en una tabla

| Capa | Decisión | ADR |
|---|---|---|
| Lake y catálogo | DuckLake v1.0; catálogo SQLite+WAL → Postgres; datos `./lake` o RustFS | [0001](docs/adr/0001-almacenamiento-y-catalogo.md) |
| Cómputo y lenguajes | Solo DuckDB 2.0 (alpha → estable); núcleo Go, SDK Python | [0002](docs/adr/0002-computo-y-lenguajes.md) |
| Ingesta y streaming | DuckDB+ODBC; Bento embebido; Bento → Kafka → Bronze | [0003](docs/adr/0003-ingesta-y-streaming.md) |
| Transformación y contratos | Pipelines declarativos SQL propios; contratos ODCS con `odcs-go` | [0004](docs/adr/0004-transformacion-y-contratos.md) |
| Orquestación y UDFs | Orquestador propio en `duct serve`; UDFs Python de DuckDB | [0005](docs/adr/0005-orquestacion-y-udfs.md) |
| Notebooks | marimo integrado | [0006](docs/adr/0006-notebooks.md) |
| Gobierno | Todo por `duct serve`; vistas por rol; DuckDB bloqueado | [0007](docs/adr/0007-gobierno.md) |
| Serving y dashboards | Fork de duckgres (Postgres + HTTP); ECharts + capa semántica | [0008](docs/adr/0008-serving-y-dashboards.md) |
| Alertas | `duct alerts` (SQL sobre Kafka); ntfy, webhook, WhatsApp, PagerDuty | [0009](docs/adr/0009-alertas.md) |
| ML e IA | MLflow opcional; Genie = MCP; Flock; LiteLLM | [0010](docs/adr/0010-ml-e-ia.md) |
| Capa semántica | Apache Ossie compilado a vistas DuckDB | [0011](docs/adr/0011-capa-semantica.md) |
| UI | Go + templ + Datastar + templUI + Tailwind; islas JS mínimas | [0012](docs/adr/0012-ui.md) |
| Tenancy | Silo: FOSS de un tenant; Duct Cloud en repo aparte, una instancia por cliente | [0013](docs/adr/0013-tenancy.md) |
| Compartir datos | OpenSharing: por consulta, luego servidor Go | [0014](docs/adr/0014-compartir-datos.md) |
| Estructura del repo | Núcleo + módulos profundos con contratos versionados | [0015](docs/adr/0015-estructura-del-repo.md) |

## Reglas que no se discuten

- **MinIO prohibido.**
- DuckLake es la verdad: todo dato entra por Bronze; Duct no escribe en las tablas del
  catálogo de DuckLake.
- Nada distribuido hasta que un cliente lo pida.
- Los módulos solo importan `core/contract`, nunca otro módulo.
- Ningún componente con licencia BSL/ELv2/AGPL embebido; marcar open-core al evaluar.
- Nada en el FOSS que solo sirva a Duct Cloud; Cloud usa el contrato de operabilidad.

## Cómo trabajar aquí

- Las decisiones viven en `docs/adr/`, una por capa. Si una decisión cambia, se edita su ADR
  (con fecha) y la tabla de arriba.
- El estado (objetivo, hitos, qué falta) vive solo en `BACKLOG.md`; no se crean planes ni
  notas sueltas.
- Verificar con `just check` (requiere `mise install`).
- Antes de recomendar una herramienta, verificar su estado actual (versión, licencia, última
  release).
- Respuestas al usuario: cortas, en español; recomendación, pocas viñetas, una pregunta.

## Pendiente

Ver `BACKLOG.md`. Fuera de este repo: plano de control de Duct Cloud.
