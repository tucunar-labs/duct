# 0013. Tenancy

Estado: aceptada (2026-10-04)

## Decisión: modelo silo
- El FOSS es de un solo tenant (sin `workspace_id`).
- Duct Cloud corre el mismo binario por cliente: su `duct serve`, su base de catálogo, su bucket.
- Infra compartida solo donde es seguro: clúster Postgres con una base por cliente, RustFS
  con un bucket por cliente.
- Plano de control aparte (provisión, versiones, monitoreo, respaldos, facturación).
- Código de usuario en sandbox (Podman/gVisor/Firecracker).

## Duct Cloud como repo aparte (2026-10-06)
- Duct Cloud vive en **otro repo** y consume Duct como artefacto publicado (imagen OCI o
  binario), nunca como fork del código. Licencia de Cloud libre de elegir.
- Duct expone un **contrato de operabilidad** genérico y estable:
  - `/healthz` y `/metrics` (Prometheus);
  - configuración por variables de entorno o archivo, sin estado escondido;
  - `duct migrate` versionado (catálogo y tablas propias);
  - `duct backup` y `duct restore`;
  - API admin: usuarios, roles, tokens, cuotas.
- Regla: nada en el FOSS que solo tenga sentido para Cloud. Lo que Cloud necesite entra al
  FOSS como función genérica de operación.
