# Documento de decisiones técnicas

Este documento explica **cómo se diseñó el repositorio analítico y por qué**, cómo se resolvió cada caso ambiguo del enunciado y cómo se garantiza la idempotencia del DAG. Las cifras son las reales obtenidas al correr el pipeline con los archivos de Kaggle incluidos en `data/raw/`.

---

## 1. Arquitectura general

```
data/raw/*.csv ──► FileSensor ──► extracción (Parquet por run en data/tmp/<run_id>/)
                                       │
seeds/*.csv ──────────────────────────►│ transformación + unificación + calidad
                                       ▼
                     PostgreSQL DWH (contenedor postgres-dwh)
                     ├── staging.*  tablas de paso (COPY masivo)
                     ├── dwh.*      modelo estrella que consulta el negocio
                     └── audit.*    rechazos y bitácora de ejecuciones
```

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| Airflow **3.1.5** con LocalExecutor | CeleryExecutor | Un solo nodo no necesita Redis ni workers; menos contenedores y arranque más rápido. |
| Dos PostgreSQL separados (`postgres-airflow` y `postgres-dwh`) | Un solo Postgres con dos bases | El enunciado lo exige y además aísla los metadatos de Airflow de la carga analítica. |
| Imagen oficial `apache/airflow:3.1.5` sin `Dockerfile` | Extender la imagen | pandas 2.1.4, pyarrow, SQLAlchemy y el provider de Postgres ya vienen en la imagen; no hace falta instalar nada. |
| Paquete del ETL en `include/datamart` + `PYTHONPATH` | Código dentro de `dags/` | En Airflow 3 la carpeta `dags/` no queda en el `sys.path` de todas las piezas; además así el DAG solo orquesta y la lógica se prueba sin Airflow. |
| Las tareas se pasan **rutas a Parquet** por XCom | Pasar DataFrames por XCom | XCom guarda JSON en la base de metadatos; no está hecho para un millón de filas. |
| Carga con `COPY` a `staging` y luego `INSERT ... SELECT` | `to_sql` de pandas | `COPY` carga 522 mil filas en segundos y los JOIN con las dimensiones los resuelve Postgres. |
| Credenciales solo en `.env` → Connection `dwh_postgres` | Credenciales en el código | Requisito del enunciado; el DAG solo conoce el `conn_id`. |

---

## 2. Modelo del repositorio analítico

**Esquema estrella con dos hechos separados (ventas y devoluciones) que comparten dimensiones, más un agregado diario.** El diagrama está en [`modelo_datos.md`](modelo_datos.md) (Mermaid) y [`modelo_datos.dbml`](modelo_datos.dbml) (para importar en dbdiagram.io).

| Tabla | Grano | Para qué |
|---|---|---|
| `dwh.dates_dim` | Un día | Agrupar por año, mes, trimestre, día de semana. Llave `id_date` = `YYYYMMDD`. |
| `dwh.products_dim` | Un `stock_code` | Nombre canónico, categoría y número de variantes de descripción. |
| `dwh.customers_dim` | Un cliente | `id_customer = -1` es el cliente genérico **ANONYMOUS**. |
| `dwh.countries_dim` | Un país normalizado | EIRE → Ireland, RSA → South Africa, Unspecified → Unknown. |
| `dwh.sales_fact` | Una línea de factura con cantidad > 0 | Revenue bruto (`gross_revenue = quantity × unit_price`). |
| `dwh.returns_fact` | Una línea con cantidad ≤ 0 | Devoluciones y ajustes en positivo (`quantity_returned`, `return_amount`) y su tipo. |
| `dwh.daily_product_revenue_agg` | Producto × día | Bruto, devoluciones y **neto** por código y día (regla de negocio). |
| `audit.rejected_records` | Una fila rechazada | Fuente, fila original, motivo y registro crudo en JSONB. |
| `audit.pipeline_runs` | Una ejecución del DAG | Filas leídas, válidas, rechazadas, duplicadas y estado. |

### ¿Por qué dos tablas de hechos y no una con un signo?

- La regla de negocio dice que las devoluciones **no son ventas** y deben **separarse**: con dos tablas nadie puede sumar una devolución como venta por error.
- Cada hecho tiene restricciones `CHECK` propias (`quantity > 0` y `unit_price > 0` en ventas; valores ≥ 0 en devoluciones).
- Como comparten dimensiones, el neto se calcula restando por producto, día, país o cliente.

### ¿Por qué un agregado diario materializado?

La regla de negocio define el revenue neto como *"bruto después de aplicar los ajustes por devolución del mismo código de producto en el mismo periodo diario"*. Ese es exactamente el grano de `daily_product_revenue_agg`, que además tiene un `CHECK (net_revenue = gross_revenue - returns_amount)`. Las preguntas mensuales suman esa tabla (277.958 filas) en vez de recorrer más de medio millón de líneas.

> **Limitación conocida.** Muchas devoluciones ocurren un día distinto al de la venta, así que el neto diario de un producto puede ser negativo. Por eso las respuestas de negocio se leen a nivel mensual o total, donde la venta y su devolución quedan en el mismo periodo.

### Otras decisiones del modelo

- **`row_hash` como llave primaria de los hechos**: MD5 de la clave natural (factura, código, cantidad, precio, fecha UTC al minuto, cliente). No depende de una secuencia, así que dos ejecuciones dejan filas idénticas, y la base rechaza cualquier duplicado.
- **Fechas**: `invoice_ts_utc` (`TIMESTAMPTZ`) en los hechos y `id_date` calculado sobre la fecha UTC.
- **Dinero**: `NUMERIC(12,3)` para el precio (hay precios con 3 decimales) y `NUMERIC(14,2)` para montos. Moneda en columna `currency` (Variable `datamart_currency` = `GBP`).
- **Constraints con nombre**: `fk_fact_*`, `uq_*`, `ck_*_positive`, como en mis proyectos anteriores.

---

## 3. Reglas de negocio: cómo se implementó cada una

| Regla | Implementación | Dónde |
|---|---|---|
| Cantidad ≤ 0 es devolución o ajuste | Va a `returns_fact` en valor absoluto. `return_type = cancellation` si la factura empieza con `C` (8.668), `adjustment` si no (1.336: daños, inventario). | `quality.split_sales_returns` |
| Precio ≤ 0 no es venta válida | Rechazo con motivo `non_positive_price` en `audit.rejected_records` (1.161 por fuente). Una devolución con precio 0 sí se acepta (ajuste de inventario sin valor). | `transform.normalize_source` |
| Fechas en UTC | Formato explícito por fuente, se localiza en `Europe/London` (Variable) y se convierte a UTC. En el cambio de hora de octubre se asume horario estándar; en marzo la hora inexistente se corre hacia adelante. | `transform.to_utc` |
| Códigos en mayúsculas y sin espacios | `upper()` + quitar todo espacio antes de cualquier cruce (`85123a ` → `85123A`). | `transform.normalize_code` |
| Revenue bruto y neto | Bruto por línea en `sales_fact`; neto por código y día en `daily_product_revenue_agg`. | `load.build_daily_revenue` |

### Motivos de rechazo (se evalúan en orden, gana el primero)

| Orden | Motivo | Regla | Filas por fuente |
|---|---|---|---|
| 1 | `required_field_null` | Falta factura, código, cantidad o precio | 0 |
| 2 | `invalid_date` | Ningún formato de fecha la interpreta | 0 |
| 3 | `not_a_product_code` | El código no sigue el patrón del catálogo (5 dígitos + sufijo, o `DCGS…`) | 2.950 |
| 4 | `non_positive_price` | Venta con precio ≤ 0 | 1.161 |
| 5 | `negative_price_on_return` | Devolución con precio negativo | 0 |
| 6 | `duplicate_in_source` | Misma clave natural repetida dentro del mismo archivo | 5.259 |

Total rechazado: **18.740 filas de 1.083.818 leídas (1,73 %)**. Si el porcentaje supera la Variable `datamart_max_reject_ratio` (10 %), la tarea `check_quality` falla y no se carga nada.

---

## 4. Casos ambiguos y cómo se resolvieron

### 4.1 Transacciones sin CustomerID

- **Hallazgo:** 135.080 filas (24,93 %) de `data.csv` no tienen cliente.
- **Decisión:** se **incluyen** y se asignan al cliente genérico `id_customer = -1` (`ANONYMOUS`, `is_identified = false`).
- **Por qué:** excluirlas borraría el **14,74 % del revenue bruto** (1,51 M GBP). Incluirlas con un cliente explícito mantiene la venta real y permite comparar identificados contra anónimos (pregunta 5).
- **Impacto:** cualquier análisis por cliente debe filtrar `id_customer <> -1`; las consultas de clientes únicos ya lo hacen.

### 4.2 Descripciones distintas para el mismo código

- **Hallazgo:** 645 códigos llegan con más de una forma de escribir la descripción (`Candle Holder White`, `CANDLE HOLDER WHITE`, o textos como `damaged`, `check`, `?` en ajustes).
- **Decisión:** nombre canónico = **la descripción más frecuente** del código, en mayúsculas y con espacios normalizados. En empate gana la **más reciente** y luego el orden alfabético.
- **Por qué:** es una regla determinística (mismo resultado en cada ejecución) y la descripción más usada es la del catálogo, no la del ajuste manual.
- **Trazabilidad:** `products_dim.description_variants` guarda cuántas formas distintas llegaron (contando también filas rechazadas), lo que responde la pregunta 6.

### 4.3 Solapamiento y duplicados entre las dos fuentes

- **Hallazgo:** el archivo histórico entregado (`online_retail.csv`) cubre el **mismo periodo** que `data.csv` (01/12/2010 a 09/12/2011) y contiene **las mismas líneas** con otro formato: columna `index` extra y `CustomerID` como decimal (`17850.0`). Las 536.480 combinaciones únicas de factura-código-cantidad-fecha están en ambos.
- **Decisión:** se detecta el duplicado real con una **clave compuesta** normalizada (factura, código, cantidad, precio, fecha UTC al minuto, cliente) y **`data.csv` tiene prioridad**: el histórico solo aporta las líneas que no estén en el volcado diario.
- **Por qué:** el volcado diario es el sistema operacional vigente. La descripción no forma parte de la clave porque es justo el campo que cambia de formato.
- **Impacto:** las 532.539 líneas válidas del histórico se reconocen como duplicadas y no se cargan dos veces (se registran en `audit.pipeline_runs.duplicates_cross_source`, no en el log de rechazos, porque no son datos malos). Si mañana llega el `online_retail_II.csv` con 2009-2010, esas líneas sí se cargarían con `source_system = history`: la extracción ya reconoce sus nombres de columna (`Invoice`, `Price`, `Customer ID`) y sus fechas ISO.

### 4.4 Códigos que no son productos

`POST`, `DOT`, `C2` (portes), `M` (manual), `D` (descuento), `S` (muestras), `BANK CHARGES`, `AMAZONFEE`, `CRUK`, `PADS`, `B` y los vales `GIFT_0001_*` no son mercancía: se rechazan con `not_a_product_code` para que no distorsionen los rankings de producto. Se conservan los `DCGS…` porque son sets de regalo vendidos como producto.

### 4.5 Categoría de producto sin la API

La API opcional no se construyó. La estrategia es una **semilla de reglas** (`seeds/category_rules.csv`) con palabra clave, categoría y prioridad: se asigna la regla de menor prioridad que coincida como palabra completa en el nombre canónico (`T-LIGHT` con prioridad 0 gana sobre `LIGHT`, para que un portavelas no quede como Electronics). Lo que no coincide queda como `Uncategorized`. La regla vive en un archivo versionado y fácil de ampliar; si se construye la API, solo cambia la tarea `extract_category_rules`.

### 4.6 Moneda y países

Todo el dataset está en libras (`GBP`); no hay datos de Colombia, México ni Perú, así que no se hace conversión, pero la moneda queda en una columna para cuando lleguen otras. Los nombres de país se normalizan con `seeds/country_mapping.csv`.

---

## 5. Idempotencia del DAG

**Prueba realizada:** se ejecutó el DAG dos veces seguidas con los mismos archivos y se comparó un hash MD5 de todas las filas de cada tabla. El resultado fue **idéntico** (522.535 ventas, 10.004 devoluciones, 277.958 filas del agregado, mismas llaves en las dimensiones).

| Mecanismo | Cómo funciona |
|---|---|
| Transformación determinística | Mismas entradas → mismas filas, mismo nombre canónico y mismo `row_hash`. Nada de `now()` ni aleatorios en llaves. |
| Dimensiones con upsert | `UPDATE` de lo que cambió e `INSERT ... WHERE NOT EXISTS` de lo nuevo: las llaves subrogadas no cambian y las secuencias no se saltan valores. |
| Hechos con delete-insert | En una sola transacción se borran las filas de las fuentes procesadas y se insertan desde staging. Si el conteo insertado no coincide con staging, se hace rollback. |
| `row_hash` como PK | Segunda red: la base rechaza cualquier línea repetida. |
| Agregado y rechazos | Se reconstruyen completos en cada ejecución. |
| `max_active_runs=1` | Dos ejecuciones nunca escriben a la vez. |

> **Nota sobre el filtro por fecha.** Los datos son de 2010-2011 y el DAG corre con fecha de hoy, así que filtrar por `data_interval_start` dejaría cada ejecución vacía. Cada run procesa los archivos completos que estén en `data/raw` (el "volcado diario"). En producción el siguiente paso sería procesar solo el archivo del día y borrar-insertar solo ese rango de fechas.

`audit.pipeline_runs` es la única tabla que cambia entre ejecuciones a propósito: es una bitácora (una fila por `run_id`).

---

## 6. Robustez del DAG

- `retries=2` con espera exponencial (2 a 10 minutos) en todas las tareas.
- `FileSensor` en modo `reschedule` espera hasta 10 minutos a que aparezcan los CSV sin ocupar un slot del executor.
- `prepare_tmp_dir` es una tarea **setup** y `cleanup_tmp` una **teardown**: los Parquet del run se borran siempre, incluso si algo falla, y esa limpieza no cambia el estado final del DAG.
- Contrato de datos antes de cargar (`quality.check_contract`): columnas exactas, llaves únicas, campos obligatorios, reglas de precio y cantidad, y cuadre de filas `leídas = válidas + rechazadas + duplicadas entre fuentes`.
- Validación después de cargar (`load.validate_load`): conteos DWH = staging, el agregado cuadra con los hechos y no hay ventas con precio ≤ 0.

---

## 7. Pendientes y cómo los resolvería

| Pendiente | Cómo lo haría |
|---|---|
| API de productos (plus opcional) | Servicio FastAPI en el compose que sirva las mismas reglas de `category_rules.csv`, Connection `product_api` de tipo HTTP y una Variable `datamart_use_api` para elegir la fuente en `extract_category_rules`. |
| Carga incremental por día | Archivo diario con fecha en el nombre (`ventas_{{ ds }}.csv`) y delete-insert solo del rango `data_interval_start`–`data_interval_end`. |
| Clasificación de categorías | El 18 % de los productos queda `Uncategorized`; se puede bajar ampliando la semilla o con el catálogo maestro cuando exista. |
| Devoluciones que cruzan días | Relacionar cada cancelación con su factura original (cliente + código + precio) para un neto por venta y no solo por día. |
| Alertas | `on_failure_callback` con aviso a Slack o correo. |
