# DataMart S.A.S. — Pipeline ETL con Apache Airflow

[English](README.md) | **Español**

## Primera plataforma de datos: de CSV operacionales a un Data Warehouse en PostgreSQL

Pipeline de extremo a extremo orquestado con **Apache Airflow 3.1.5** dentro de Docker: lee dos fuentes CSV de Kaggle con formatos distintos, las limpia y unifica, separa ventas de devoluciones, registra cada rechazo con su motivo y carga un **modelo estrella** en un PostgreSQL analítico, listo para responder las preguntas de negocio.

**Autor:** Vanessa Fontalvo Reniz · Ingeniera de Sistemas y Computación
**Módulo:** Ingeniería de Datos — Prueba de desempeño (Airflow + Data Warehouse)

---

## 1. Inicio rápido (menos de 10 minutos)

**Requisitos:** Docker Desktop (o Docker Engine + Compose v2) con al menos **4 GB de RAM** asignados y Git. No hace falta instalar Python ni descargar nada de Kaggle: los datos vienen en el repositorio.

```bash
# 1. Clonar
git clone https://github.com/vanefRiwi/practice-test-airflow.git
cd practice-test-airflow

# 2. Crear el .env a partir del ejemplo (en Linux, pon tu UID: echo "AIRFLOW_UID=$(id -u)" >> .env)
cp .env.example .env

# 3. Levantar todo
docker compose up -d
```

Al terminar el arranque (1 a 2 minutos la primera vez, más la descarga de imágenes) queda todo listo **sin pasos manuales**:

| Servicio | Qué hace | Acceso |
|---|---|---|
| `airflow-apiserver` | UI y API de Airflow | http://localhost:8080 (usuario y clave de `.env`, por defecto `admin` / `change_this_admin_password`) |
| `airflow-scheduler` | Detecta y ejecuta el DAG | — |
| `airflow-dag-processor` | Parsea los DAGs | — |
| `airflow-triggerer` | Tareas diferidas | — |
| `postgres-airflow` | Metadatos de Airflow | interno |
| `postgres-dwh` | Repositorio analítico con las tablas creadas al iniciar | `localhost:5433` |
| `airflow-init` | Migra metadatos, crea el admin, las **Connections** y las **Variables**, y termina | — |

El DAG `datamart_daily_etl` queda **activo** y el scheduler lanza su primera ejecución automáticamente (tarda unos 2 minutos). También se puede lanzar a mano con el botón ▶ de la UI.

Para empezar de cero (borra las bases): `docker compose down -v`.

---

## 2. Verificación paso a paso

### 2.1 Servicios arriba

```bash
docker compose ps          # todos "healthy"; airflow-init "exited (0)"
```

### 2.2 Connections y Variables creadas solas

```bash
docker compose exec airflow-scheduler airflow connections get dwh_postgres -o yaml
docker compose exec airflow-scheduler airflow connections get fs_datamart -o yaml
docker compose exec airflow-scheduler airflow variables list
```

También se ven en la UI: **Admin → Connections** y **Admin → Variables**.

| Connection | Tipo | Uso |
|---|---|---|
| `dwh_postgres` | postgres | Repositorio analítico (host `postgres-dwh`, credenciales tomadas de `.env`) |
| `fs_datamart` | fs | Carpeta `/opt/airflow/data` que vigilan los `FileSensor` |

| Variable | Valor inicial | Para qué |
|---|---|---|
| `datamart_sales_file` | `raw/data.csv` | Archivo diario de órdenes |
| `datamart_history_file` | `raw/online_retail.csv` | Historial extendido |
| `datamart_source_timezone` | `Europe/London` | Zona horaria de la fuente para convertir a UTC |
| `datamart_max_reject_ratio` | `0.10` | Umbral de calidad: si se rechaza más del 10 %, el DAG falla antes de cargar |
| `datamart_currency` | `GBP` | Moneda de los montos |

Se definen en [`airflow/init/variables.json`](airflow/init/variables.json) y las crea [`airflow/init/init_airflow.sh`](airflow/init/init_airflow.sh) en cada arranque.

### 2.3 El DAG terminó bien

```bash
docker compose exec airflow-scheduler airflow dags list-runs datamart_daily_etl
```

### 2.4 Los datos llegaron al repositorio analítico

```bash
docker compose exec postgres-dwh psql -U dwh_user -d datamart_dwh -c "
SELECT 'sales_fact' AS tabla, COUNT(*) FROM dwh.sales_fact
UNION ALL SELECT 'returns_fact', COUNT(*) FROM dwh.returns_fact
UNION ALL SELECT 'daily_product_revenue_agg', COUNT(*) FROM dwh.daily_product_revenue_agg
UNION ALL SELECT 'products_dim', COUNT(*) FROM dwh.products_dim
UNION ALL SELECT 'customers_dim', COUNT(*) FROM dwh.customers_dim
UNION ALL SELECT 'rejected_records', COUNT(*) FROM audit.rejected_records;"

docker compose exec postgres-dwh psql -U dwh_user -d datamart_dwh -c "SELECT * FROM audit.pipeline_runs;"
```

Resultado esperado: **522.535** ventas, **10.004** devoluciones, **277.958** filas producto-día, **3.927** productos, **4.363** clientes (incluido ANONYMOUS) y **18.740** rechazos.

Las consultas de negocio se ejecutan igual, por ejemplo:

```bash
docker compose exec -T postgres-dwh psql -U dwh_user -d datamart_dwh < sql/queries/q1_monthly_net_sales.sql
```

Desde DBeaver o Power BI: host `localhost`, puerto `5433`, base `datamart_dwh` y el usuario/clave de `.env`.

---

## 3. Fuentes de datos

| Fuente | Archivo | Filas | Problemas principales |
|---|---|---:|---|
| Ventas diarias (Kaggle `carrie1/ecommerce-data`) | `data/raw/data.csv` | 541.909 | Encoding ISO-8859-1, cantidades negativas, precios ≤ 0, 24,9 % sin cliente, códigos de servicio (POST, DOT...), duplicados |
| Historial (Kaggle `thedevastator/online-retail-transaction-dataset`) | `data/raw/online_retail.csv` | 541.909 | Columna `index` extra, CustomerID decimal (`17850.0`) y **mismo periodo y líneas** que la fuente diaria |

Los CSV están en `data/raw/` tal como vienen de Kaggle (45 y 50 MB). La exploración completa está en [`docs/exploracion_datos.md`](docs/exploracion_datos.md).

---

## 4. Diagrama del pipeline

```
                ┌───────────────────────── DAG datamart_daily_etl (@daily) ──────────────────────────┐
                │                                                                                    │
 data/raw/      │  prepare_tmp_dir (setup)                     create_dwh_tables                     │
 data.csv ──────┼─► wait_for_sales_file ───► extract_daily_sales ───┐                                │
 online_retail ─┼─► wait_for_history_file ─► extract_history ───────┼─► transform_and_merge          │
 seeds/*.csv ───┼──────────────────────────► extract_category_rules ┘            │                   │
                │                                                         check_quality              │
                │                                            ┌───────────────────┴──────────┐        │
                │                                       load_staging                load_rejections  │
                │                                            │                              │        │
                │                                     load_dimensions                       │        │
                │                              ┌─────────────┴─────────────┐                │        │
                │                       load_sales_fact            load_returns_fact        │        │
                │                              └─────────────┬─────────────┘                │        │
                │                                   build_daily_revenue                     │        │
                │                                            └──────────► validate_load ◄───┘        │
                │                                                               │                    │
                │                                                     cleanup_tmp (teardown)         │
                └────────────────────────────────────────────────────────────────────────────────────┘
                                                      │
                                                      ▼
                         PostgreSQL DWH: staging.* → dwh.* (estrella) + audit.*
```

| Tarea | Qué hace |
|---|---|
| `wait_for_*_file` | `FileSensor` (Connection `fs_datamart`) espera el CSV hasta 10 minutos, en modo `reschedule` |
| `extract_*` | Lee el CSV (detecta el encoding), lleva las columnas a un esquema común y guarda Parquet en `data/tmp/<run_id>/` |
| `extract_category_rules` | Lee la semilla de categorías (estrategia en lugar de la API opcional) |
| `transform_and_merge` | Normaliza códigos, clientes y países, pasa fechas a UTC, marca rechazos, deduplica entre fuentes y arma la dimensión producto |
| `check_quality` | Separa ventas y devoluciones y valida el contrato de datos y el umbral de rechazos |
| `load_staging` / `load_rejections` | `COPY` masivo a `staging.*` y al log `audit.rejected_records` |
| `load_dimensions` | Upsert de fechas, productos, clientes y países |
| `load_*_fact` | Delete-insert de `sales_fact` y `returns_fact` en una transacción |
| `build_daily_revenue` | Reconstruye bruto, devoluciones y neto por producto y día |
| `validate_load` | Chequeos SQL post-carga y registro en `audit.pipeline_runs` |
| `cleanup_tmp` | Borra los Parquet del run aunque algo falle |

Todas las tareas tienen `retries=2` con espera exponencial; el DAG tiene `max_active_runs=1` y `catchup=False`.

---

## 5. Proceso ETL

### Extract
Cada fuente se lee como texto (`dtype=str`) para no inferir tipos a ciegas. Si UTF-8 falla se reintenta con ISO-8859-1. Las columnas se renombran a un esquema común (`InvoiceNo`/`Invoice` → `invoice_no`, `UnitPrice`/`Price` → `unit_price`, `CustomerID`/`Customer ID` → `customer_id`) y se guarda el número de fila original para trazabilidad.

### Transform
1. Códigos en mayúsculas y sin espacios; descripciones sin espacios repetidos.
2. CustomerID como texto entero (`17850.0` → `17850`); vacío = cliente ANONYMOUS.
3. Fechas con formato explícito por fuente → `Europe/London` → **UTC**.
4. Países normalizados con `seeds/country_mapping.csv`.
5. Cada fila recibe un motivo de rechazo o queda válida (ver tabla en [decisiones técnicas](docs/decisiones_tecnicas.md#motivos-de-rechazo-se-evalúan-en-orden-gana-el-primero)).
6. `row_hash` = MD5 de la clave natural; duplicados dentro de cada fuente y entre fuentes (gana `data.csv`).
7. Nombre canónico (descripción más frecuente) y categoría por palabra clave.

### Validaciones
Contrato de datos antes de cargar (columnas, llaves únicas, nulos, precio y cantidad, cuadre de filas, umbral de rechazos) y chequeos SQL después de cargar.

### Load
`COPY` a tablas `staging` → upsert de dimensiones → delete-insert de hechos con JOIN a las dimensiones → agregado diario. Todo dentro de transacciones.

---

## 6. Modelo de datos

Esquema estrella con **dos hechos separados** (ventas y devoluciones) que comparten cuatro dimensiones, más un agregado diario de revenue neto.

```
               dates_dim          products_dim
                    │  ╲          ╱    │
                    │   sales_fact     │
 customers_dim ─────┼──╱          ╲────┼───── countries_dim
                    │   returns_fact   │
                    │  ╱          ╲    │
          daily_product_revenue_agg (id_date, id_product)
```

- Diagrama completo: [`docs/modelo_datos.md`](docs/modelo_datos.md) (se dibuja en GitHub) y [`docs/modelo_datos.dbml`](docs/modelo_datos.dbml) para dbdiagram.io.
- DDL: [`sql/ddl/`](sql/ddl). Corre al crear el contenedor (`docker-entrypoint-initdb.d`) y otra vez en la tarea `create_dwh_tables` (todo es `IF NOT EXISTS`).
- Por qué este diseño: [`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md#2-modelo-del-repositorio-analítico).

---

## 7. Decisiones técnicas y casos ambiguos

Resumen; la justificación completa, con cifras y alternativas descartadas, está en **[`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md)**.

| Caso | Decisión |
|---|---|
| Transacciones sin CustomerID (24,9 %) | Se incluyen con el cliente genérico `ANONYMOUS` (`id_customer = -1`): excluirlas borraría el 14,7 % del revenue |
| Descripciones distintas para un código | Nombre canónico = la más frecuente en mayúsculas; empate → la más reciente. Se guarda el número de variantes |
| Fuentes que se solapan | Clave natural compuesta normalizada; si la línea está en ambas, gana `data.csv` |
| Códigos POST, DOT, M, D, BANK CHARGES... | Rechazo `not_a_product_code` |
| Venta con precio ≤ 0 | Rechazo `non_positive_price` en `audit.rejected_records` |
| Categoría sin API | Semilla de reglas por palabra clave con prioridad (`seeds/category_rules.csv`) |
| Idempotencia | Transformación determinística + upsert de dimensiones + delete-insert de hechos + `row_hash` como PK. Probado: dos ejecuciones dan exactamente las mismas filas |

---

## 8. Preguntas de negocio

Una consulta por pregunta en [`sql/queries/`](sql/queries); respuestas con cifras en **[`docs/respuestas_negocio.md`](docs/respuestas_negocio.md)**.

| # | Pregunta | Consulta | Respuesta corta |
|---|---|---|---|
| 1 | Evolución mensual de ventas netas | `q1_monthly_net_sales.sql` | Pico en noviembre 2011 (1,43 M); sep-nov = 35 % del año |
| 2 | Categorías por revenue y devoluciones | `q2_category_revenue_returns.sql` | Home = 59,8 % del bruto; Stationery la de más devolución (16,2 %) |
| 3 | Top 10 neto y top 10 tasa de devolución | `q3_top_products.sql` | #1 REGENCY CAKESTAND (164 mil neto) |
| 4 | Países y ticket promedio | `q4_country_avg_ticket.sql` | Reino Unido = 90,5 % de facturas; Países Bajos ticket 3.053 |
| 5 | Identificados vs. sin CustomerID | `q5_identified_vs_anonymous.sql` | Sin cliente: ticket 1.101 vs 475, 96 productos por factura |
| 6 | Descripciones inconsistentes y códigos únicos | `q6_inconsistent_descriptions.sql` | 3.927 códigos; 637 (16,2 %) inconsistentes |
| 7 | Recomendación al equipo de producto | `q7_product_recommendation.sql` | Dos pedidos cancelados = 51,7 % de las devoluciones → tope de cantidad por línea |

---

## 9. Desarrollo local con uv

El código del ETL se puede probar sin Docker. El entorno virtual se maneja con [uv](https://docs.astral.sh/uv/) y usa **las mismas versiones** que trae la imagen de Airflow 3.1.5 (pandas 2.1.4, SQLAlchemy 1.4.54, pyarrow 18.1.0).

```bash
uv sync                                        # crea .venv con dependencias y herramientas de desarrollo
uv run pytest                                  # pruebas de reglas de negocio (el test del DAG se omite sin Airflow)
uv run ruff check . && uv run ruff format --check .
uv run python scripts/explore_sources.py      # perfil de las dos fuentes
```

Airflow no se instala en el entorno local a propósito: es pesado y ya corre en Docker. Por eso la lógica vive en `include/datamart/` (pandas + SQL puros) y el DAG solo orquesta.

---

## 10. Estructura del repositorio

```
practice-test-airflow/
├── docker-compose.yml          # Airflow 3.1.5 + Postgres de metadatos + Postgres DWH
├── .env.example                # Variables de entorno necesarias (el .env real está en .gitignore)
├── pyproject.toml / uv.lock    # Entorno local con uv
├── airflow/init/
│   ├── init_airflow.sh         # Connections + Variables al arrancar
│   └── variables.json          # Valores iniciales de las Airflow Variables
├── dags/
│   └── datamart_etl_dag.py     # Orquestación (solo define tareas y dependencias)
├── include/datamart/           # Lógica del ETL
│   ├── config.py               # Rutas, ids de Airflow, reglas y motivos de rechazo
│   ├── extract.py              # Lectura de CSV y semillas
│   ├── transform.py            # Limpieza, UTC, deduplicación, nombre canónico, categoría
│   ├── quality.py              # Ventas vs devoluciones y contrato de datos
│   ├── load.py                 # COPY, upserts, delete-insert, agregado y validación
│   └── log_utils.py            # Helpers de log
├── sql/
│   ├── ddl/                    # Esquemas y tablas (se ejecutan al iniciar el DWH)
│   └── queries/                # Una consulta por pregunta de negocio
├── seeds/                      # Reglas de categoría y mapeo de países
├── data/raw/                   # Fuentes CSV de Kaggle
├── docs/                       # Decisiones, exploración, modelo y respuestas
├── scripts/explore_sources.py  # Exploración de datos
├── tests/                      # Pruebas con pytest
├── config/ logs/ plugins/      # Carpetas que monta Airflow
└── README.md / README.es.md   # Documentación en inglés y en español
```

---

## 11. Solución de problemas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `airflow-init` falla con `Permission denied` en `logs/` (Linux) | UID distinto al de la imagen | `echo "AIRFLOW_UID=$(id -u)" >> .env` y `docker compose up -d` |
| `Define DWH_PASSWORD in .env` al levantar | No existe `.env` | `cp .env.example .env` |
| Las tablas del DWH no se crean | El volumen ya existía de una ejecución anterior | `docker compose down -v` y levantar de nuevo (igual la tarea `create_dwh_tables` las crea) |
| La tarea `transform_and_merge` se queda sin memoria | Docker con poca RAM | Asignar al menos 4 GB a Docker |
| `airflow-init` falla con `set: pipefail: invalid option name` o `$'\r': command not found` (Windows) | El repositorio se clonó con finales de línea CRLF | Con `.gitattributes` ya no pasa en un clon nuevo. En una copia existente: `git rm -r --cached . -q` y `git reset --hard` (descarta cambios locales sin commit) y luego `docker compose up -d` |
| El DAG falla después de actualizar el repositorio (nombres de Variables, tablas o valores cambiaron) | La base de metadatos y el DWH conservan datos de la versión anterior | `docker compose down -v` y `docker compose up -d` para empezar de cero |
| El puerto 5433 u 8080 está ocupado | Otro servicio local | Cambiar `DWH_HOST_PORT` en `.env` o el puerto del api-server en el compose |
