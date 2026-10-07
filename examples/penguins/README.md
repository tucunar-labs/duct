# Pingüinos de Palmer

344 pingüinos de tres especies (Adelie, Chinstrap, Gentoo) medidos en tres islas del
archipiélago Palmer (Antártida), 2007-2009: largo y profundidad del pico, largo de la
aleta, masa y sexo. Es el sustituto moderno de *iris*. Funciona sin red.

## Origen y licencia

- Datos: Kristen Gorman, Palmer Station LTER (Long Term Ecological Research Network).
  Gorman KB, Williams TD, Fraser WR (2014), *Ecological Sexual Dimorphism and
  Environmental Variability within a Community of Antarctic Penguins*, PLoS ONE 9(3): e90081.
- Paquete R `palmerpenguins` de Allison Horst, Alison Hill y Kristen Gorman, publicado
  bajo **CC0 1.0** (dominio público):
  <https://github.com/allisonhorst/palmerpenguins>. El CSV de `data/penguins.csv` es
  `inst/extdata/penguins.csv` de ese repositorio, sin cambios.
- CC0 permite copiarlo y redistribuirlo; aun así se cita la fuente.

## Qué hay

| Capa | Archivo | Qué hace |
|---|---|---|
| Ingesta | `ingest/penguins.yaml` | CSV a `bronze.penguins` (`NA` pasa a NULL) |
| Silver | `pipelines/penguins.sql` | tipos, `id` por pingüino; `DROP ROW` de las 2 filas sin medidas (342 quedan) |
| Gold | `gold.penguins` (detalle), `gold.penguin_stats` (por especie, isla y sexo) | |
| Contrato | `contracts/penguin_stats.odcs.yaml` | esquema, llave única, masa plausible, frescura |
| Semántica | `semantic/penguins.ossie.yaml` | métricas `penguin_count`, `avg_bill_length`, `avg_bill_depth`, `avg_flipper_length`, `avg_body_mass` |
| Dashboard | `dashboards/especies.yaml` | un scatter pico largo vs profundo por especie, barras por isla, filtros isla y sexo |
| Job | `jobs/refresh.yaml` | ingesta + pipelines, diario |

El scatter muestra la paradoja de Simpson: mezclando especies, más pico largo parece
implicar menos profundidad; dentro de cada especie la relación es positiva.

## Cómo correrlo

```sh
cp -r examples/penguins /tmp/penguins && cd /tmp/penguins
duct init .
duct ingest              # bronze.penguins: 344 filas
duct run                 # silver 342 (2 descartadas), gold, y el contrato se hace cumplir
duct contract test       # reglas de calidad del contrato
duct semantic validate
duct dashboard lint
./setup.sh               # rol analistas + usuario ana (clave demo-pw-123)
duct semantic query --role analistas --dims species --measures penguin_count,avg_body_mass
duct jobs run refresh    # el job a mano
duct serve               # http://127.0.0.1:8484/dashboards, entra como ana
```
