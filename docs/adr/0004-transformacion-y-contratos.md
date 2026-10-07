# 0004. Transformación y contratos de datos

Estado: aceptada (transformación 2026-10-03; contratos 2026-10-06)

## Pipelines
- **Pipelines declarativos propios, al estilo Lakeflow Declarative Pipelines**, sobre DuckDB +
  DuckLake. SQL estándar, sin Jinja ni YAML: `CREATE MATERIALIZED VIEW`,
  `CREATE STREAMING TABLE`, `CONSTRAINT … EXPECT … ON VIOLATION DROP ROW | FAIL UPDATE`,
  decorador `@duct.table`.
- Dependencias y lineage con el parser de DuckDB (`json_serialize_sql`) desde Go.
- Entornos = schema o catálogo DuckLake aparte.
- Fases: v1 refresco completo + DAG + expectations + `duct run`; v2 incremental con
  `table_changes()`; v3 streaming continuo y entornos.

## Contratos de datos: ODCS v3.2
- Formato: **ODCS** (Bitol, LF AI & Data, Apache-2.0), en `contracts/<tabla>.odcs.yaml`.
- Librería: **`github.com/alonsoburon/odcs-go`** (propia, Apache-2.0; tipos, validación,
  lint, diff con cambios breaking). Ofrecerla a Bitol.
- Comandos: `duct contract lint | compile | diff | test`.
- Mapeo: `schema` → DDL + CI bloquea cambios breaking; calidad `critical` → `EXPECT … FAIL
  UPDATE`, `warning` → `DROP ROW` o auditoría; reglas `sql` con `schedule` → tarea + alerta;
  SLA `latency`/`frequency` → alerta de frescura por snapshot; `retention` → `duct gc`;
  `roles` → `GRANT`; `team`/`support` → dueño y canal de alertas.
- Extensiones `DUCT` en `customProperties`: DuckLake como servidor `custom`, filtros por fila
  y máscaras.
- Regla de una sola fuente: Bronze y Silver llevan `EXPECT` en el SQL; **Gold y tablas
  compartidas se rigen por su contrato** (genera expectations, grants y alertas).
- ODPS y ontologías de Ossie: diferidos.

## Implementado (2026-10-07)

Código: `core/contracts` (carga, compilación, evaluación), `core/pipeline/gate.go` (gancho
`Gate` en `duct run`), `cmd/duct/contract.go`. Contrato ODCS v3.2 con `odcs-go` v0.1.0.

- **Archivos**: `contracts/<tabla>.odcs.yaml`, un contrato por tabla (un solo objeto en
  `schema`). La tabla sale del servidor `type: custom` con `customProperties`
  `ducklake_table: schema.tabla` (si falta: `physicalName`, luego `name`). Dos contratos
  para la misma tabla es un error. Un contrato roto detiene `duct run`.
- **Extensiones** (`customProperties`, prefijo `duct.`; una desconocida es error):
  en un rol `duct.row_filter` (predicado) y `duct.mask.<columna>` (expresión); en una regla
  de calidad `duct.on_violation: drop_row`.
- **`duct contract lint [--strict] [--json] [archivo...]`**: validación del esquema ODCS +
  `odcs.Lint` + chequeos Duct: la tabla es destino de un pipeline o existe en el lake, columnas
  y tipos compatibles con el lake (ODCS `logicalType`/`physicalType` frente al tipo DuckDB),
  cada regla compila y su SQL liga (`EXPLAIN`), roles y extensiones válidos, aviso si la tabla
  es Bronze/Silver (regla de una sola fuente) o si hay SLA sin ser destino de pipeline.
- **`duct contract diff [--base <ref>] [--json]`** (por defecto `HEAD`, con `git show`):
  `odcs.Diff` por archivo; contrato nuevo = `added`; contrato borrado = `removed`, que cuenta
  como breaking. Código 1 si hay breaking (pensado para CI).
- **`duct contract test [tabla...] [--json]`**: mide cada regla contra el lake, PASS/FAIL/WARN;
  código 1 si falla el esquema o una regla `critical` (o `error`) o una que no se pueda medir.
- **`duct contract compile [--apply] [tabla...]`**: imprime lo que genera (esquema, checks con
  su SQL, expectations por fila, roles, frescura). `--apply` sincroniza roles.
- **`duct contract freshness [--json]`**: estado por tabla (`fresh|stale|unknown|no_sla`) como
  datos; código 1 si hay `stale`/`unknown`. Función `contracts.FreshnessOf`. Usa el último
  refresco exitoso en `duct_pipeline_runs`; el máximo es `latency` (si no, `frequency`).
  No hay alertas aquí: el orquestador o `duct alerts` consumen el resultado.

**Cumplimiento en `duct run`** (`pipeline.Options.Gate`; todo sobre el resultado en staging,
antes de tocar el lake; un fallo conserva la versión previa y salta lo que depende de ella):

- Esquema: columnas y tipos del resultado frente al contrato. Faltante, de otro tipo **o no
  declarada** = FAIL UPDATE (el contrato es la interfaz; una columna extra no debe filtrarse).
- Reglas implícitas del esquema: `required`/`primaryKey` -> `nullValues = 0`;
  `unique`/`primaryKey` -> `duplicateValues = 0` (PK compuesta incluida). Son `critical`.
- Calidad: métricas de biblioteca `rowCount`, `nullValues`, `missingValues`, `duplicateValues`,
  `invalidValues` (`validValues` y/o `pattern`, ambas deben cumplirse; NULL no es inválido) con
  `unit: percent` opcional, y reglas `sql` (`{table}`/`${table}`, `{column}`; deben devolver
  un número). Umbrales `mustBe`, `mustNotBe`, `mustBe{Greater,Less}...`, `mustBe(Not)Between`.
  Reglas `text` y `custom` no se evalúan (se listan).
- Severidad: `critical` y `error` = FAIL UPDATE. Cualquier otra (o ninguna) = advertencia:
  queda en `duct_pipeline_runs.expectations` como `contract:<regla> [warn]`. Una regla que no se
  puede medir bloquea solo si es `critical`/`error`.
- DROP ROW: solo para `nullValues`/`missingValues`/`invalidValues` de una propiedad, con
  `duct.on_violation: drop_row`; se funde con las `EXPECT` del pipeline como
  `contract:<regla>` y la regla agregada deja de bloquear.
- `roles`: `access: read` = `GRANT SELECT`; otros accesos no se aplican (Duct solo tiene
  SELECT). Se aplican **después de cada refresco exitoso de la tabla en `duct run`** y con
  `duct contract compile --apply`; el contrato es la fuente de verdad de los roles que nombra
  (crea el rol si falta, fija o borra el filtro y deja solo las máscaras declaradas) y no toca
  roles que no nombra. Nunca revoca otros roles.
- Pendiente: `retention` -> `duct gc`, `team`/`support` -> canal de alertas, reglas `sql` con
  `schedule` como tarea del orquestador (el orquestador puede llamar `duct contract test`).

## Descartados
- dbt (Jinja/YAML), SQLMesh (58 deps, bugs con DuckLake, actividad baja), datacontract-cli
  (Python, no soporta DuckDB 2.0).
