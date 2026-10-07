# AGENTS.md (python)

SDK de Python de Duct (`duct-data`, se importa como `duct`). Solo es el SDK de código
de usuario: UDFs (`@duct.function`), tablas Python (`@duct.table`), tareas `python`,
integración con marimo y MLflow opcional. El núcleo es Go; ver ADR 0002, 0005, 0006, 0010, 0015.

## Reglas
- Sin credenciales del lake: el SDK solo habla con `duct serve` (HTTP o Postgres wire).
  Nunca S3 ni catálogo directo.
- Stdlib primero: dependencias de runtime vacías. Lo pesado (pyarrow, psycopg, pandas,
  polars, marimo) va en extras opcionales y se importa de forma perezosa con error claro.
- Tipado estricto (pyright strict), código y comentarios en inglés.
- El registro (`duct.registry`) es el contrato con Go: `python -m duct.registry --json`.
  Cambiar su formato JSON exige subir `schema_version`.

## Verificar (desde `python/`)
```
uv sync
uv run ruff check
uv run ruff format --check
uv run pyright
uv run pytest
```
