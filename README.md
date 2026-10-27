# DataMart S.A.S. — ETL Pipeline with Apache Airflow

**English** | [Español](README.es.md)

## A first data platform: from operational CSVs to a PostgreSQL data warehouse

End-to-end pipeline orchestrated with **Apache Airflow 3.1.5** in Docker: it reads two Kaggle CSV sources with different formats, cleans and merges them, splits sales from returns, logs every rejected row with its reason and loads a **star schema** into an analytical PostgreSQL database, ready to answer the business questions.

**Author:** Vanessa Fontalvo Reniz · Systems and Computer Engineer
**Module:** Data Engineering — Performance test (Airflow + Data Warehouse)

> The supporting documents in `docs/` (technical decisions, data exploration, data model and business answers) are written in Spanish.

---

## 1. Quick start (under 10 minutes)

**Requirements:** Docker Desktop (or Docker Engine + Compose v2) with at least **4 GB of RAM** and Git. No need to install Python or download anything from Kaggle: the data is in the repository.

```bash
# 1. Clone
git clone https://github.com/vanefRiwi/practice-test-airflow.git
cd practice-test-airflow

# 2. Create .env from the example (on Linux, set your UID: echo "AIRFLOW_UID=$(id -u)" >> .env)
cp .env.example .env

# 3. Start everything
docker compose up -d
```

Once it finishes starting (1 to 2 minutes the first time, plus the image download) everything is ready **with no manual steps**:

| Service | What it does | Access |
|---|---|---|
| `airflow-apiserver` | Airflow UI and API | http://localhost:8080 (user and password from `.env`, default `admin` / `change_this_admin_password`) |
| `airflow-scheduler` | Picks up and runs the DAG | — |
| `airflow-dag-processor` | Parses the DAGs | — |
| `airflow-triggerer` | Deferred tasks | — |
| `postgres-airflow` | Airflow metadata | internal |
| `postgres-dwh` | Analytical warehouse, tables created on startup | `localhost:5433` |
| `airflow-init` | Migrates metadata, creates the admin, the **Connections** and the **Variables**, then exits | — |

The `datamart_daily_etl` DAG starts **unpaused** and the scheduler triggers its first run automatically (about 2 minutes). It can also be triggered by hand with the ▶ button in the UI.

To start from scratch (wipes the databases): `docker compose down -v`.

---

## 2. Step-by-step check

### 2.1 Services up

```bash
docker compose ps          # all "healthy"; airflow-init "exited (0)"
```

### 2.2 Connections and Variables created automatically

```bash
docker compose exec airflow-scheduler airflow connections get dwh_postgres -o yaml
docker compose exec airflow-scheduler airflow connections get fs_datamart -o yaml
docker compose exec airflow-scheduler airflow variables list
```

They also show up in the UI: **Admin → Connections** and **Admin → Variables**.

| Connection | Type | Used for |
|---|---|---|
| `dwh_postgres` | postgres | Analytical warehouse (host `postgres-dwh`, credentials taken from `.env`) |
| `fs_datamart` | fs | `/opt/airflow/data` folder watched by the `FileSensor`s |

| Variable | Initial value | Purpose |
|---|---|---|
| `datamart_sales_file` | `raw/data.csv` | Daily orders file |
| `datamart_history_file` | `raw/online_retail.csv` | Extended history |
| `datamart_source_timezone` | `Europe/London` | Source timezone, used to convert to UTC |
| `datamart_max_reject_ratio` | `0.10` | Quality threshold: if more than 10 % is rejected the DAG fails before loading |
| `datamart_currency` | `GBP` | Currency of the amounts |

They are defined in [`airflow/init/variables.json`](airflow/init/variables.json) and created by [`airflow/init/init_airflow.sh`](airflow/init/init_airflow.sh) on every startup.

### 2.3 The DAG finished successfully

```bash
docker compose exec airflow-scheduler airflow dags list-runs datamart_daily_etl
```

### 2.4 The data reached the warehouse

```bash
docker compose exec postgres-dwh psql -U dwh_user -d datamart_dwh -c "
SELECT 'sales_fact' AS table_name, COUNT(*) FROM dwh.sales_fact
UNION ALL SELECT 'returns_fact', COUNT(*) FROM dwh.returns_fact
UNION ALL SELECT 'daily_product_revenue_agg', COUNT(*) FROM dwh.daily_product_revenue_agg
UNION ALL SELECT 'products_dim', COUNT(*) FROM dwh.products_dim
UNION ALL SELECT 'customers_dim', COUNT(*) FROM dwh.customers_dim
UNION ALL SELECT 'rejected_records', COUNT(*) FROM audit.rejected_records;"

docker compose exec postgres-dwh psql -U dwh_user -d datamart_dwh -c "SELECT * FROM audit.pipeline_runs;"
```

Expected result: **522,535** sales, **10,004** returns, **277,958** product-day rows, **3,927** products, **4,363** customers (ANONYMOUS included) and **18,740** rejections.

The business queries run the same way, for example:

```bash
docker compose exec -T postgres-dwh psql -U dwh_user -d datamart_dwh < sql/queries/q1_monthly_net_sales.sql
```

From DBeaver or Power BI: host `localhost`, port `5433`, database `datamart_dwh` and the user/password from `.env`.

---

## 3. Data sources

| Source | File | Rows | Main issues |
|---|---|---:|---|
| Daily sales (Kaggle `carrie1/ecommerce-data`) | `data/raw/data.csv` | 541,909 | ISO-8859-1 encoding, negative quantities, prices ≤ 0, 24.9 % without customer, service codes (POST, DOT...), duplicates |
| History (Kaggle `thedevastator/online-retail-transaction-dataset`) | `data/raw/online_retail.csv` | 541,909 | Extra `index` column, decimal CustomerID (`17850.0`) and **same period and lines** as the daily source |

The CSVs are in `data/raw/` exactly as they come from Kaggle (45 and 50 MB). The full exploration is in [`docs/exploracion_datos.md`](docs/exploracion_datos.md).

---

## 4. Pipeline diagram

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
                         PostgreSQL DWH: staging.* → dwh.* (star) + audit.*
```

| Task | What it does |
|---|---|
| `wait_for_*_file` | `FileSensor` (Connection `fs_datamart`) waits up to 10 minutes for the CSV, in `reschedule` mode |
| `extract_*` | Reads the CSV (detects the encoding), maps the columns to a common schema and saves Parquet in `data/tmp/<run_id>/` |
| `extract_category_rules` | Reads the category seed (used instead of the optional API) |
| `transform_and_merge` | Normalizes codes, customers and countries, converts dates to UTC, tags rejections, dedupes across sources and builds the product dimension |
| `check_quality` | Splits sales and returns and checks the data contract and the rejection threshold |
| `load_staging` / `load_rejections` | Bulk `COPY` into `staging.*` and into the `audit.rejected_records` log |
| `load_dimensions` | Upsert of dates, products, customers and countries |
| `load_*_fact` | Delete-insert of `sales_fact` and `returns_fact` in one transaction |
| `build_daily_revenue` | Rebuilds gross, returns and net revenue per product and day |
| `validate_load` | Post-load SQL checks and entry in `audit.pipeline_runs` |
| `cleanup_tmp` | Deletes the run's Parquet files even if something failed |

Every task has `retries=2` with exponential backoff; the DAG has `max_active_runs=1` and `catchup=False`.

---

## 5. ETL process

### Extract
Each source is read as text (`dtype=str`) so types are not guessed blindly. If UTF-8 fails it retries with ISO-8859-1. Columns are renamed to a common schema (`InvoiceNo`/`Invoice` → `invoice_no`, `UnitPrice`/`Price` → `unit_price`, `CustomerID`/`Customer ID` → `customer_id`) and the original row number is kept for traceability.

### Transform
1. Codes uppercased with no spaces; descriptions without repeated spaces.
2. CustomerID as an integer string (`17850.0` → `17850`); empty = ANONYMOUS customer.
3. Dates with an explicit format per source → `Europe/London` → **UTC**.
4. Countries normalized with `seeds/country_mapping.csv`.
5. Each row gets a rejection reason or stays valid (see the table in the [technical decisions](docs/decisiones_tecnicas.md#motivos-de-rechazo-se-evalúan-en-orden-gana-el-primero)).
6. `row_hash` = MD5 of the natural key; duplicates within each source and across sources (`data.csv` wins).
7. Canonical name (most frequent description) and category by keyword.

### Checks
Data contract before loading (columns, unique keys, nulls, price and quantity, row reconciliation, rejection threshold) and SQL checks after loading.

### Load
`COPY` into `staging` tables → dimension upsert → fact delete-insert joined to the dimensions → daily aggregate. All inside transactions.

---

## 6. Data model

Star schema with **two separate facts** (sales and returns) sharing four dimensions, plus a daily net revenue aggregate.

```
               dates_dim          products_dim
                    │  ╲          ╱    │
                    │   sales_fact     │
 customers_dim ─────┼──╱          ╲────┼───── countries_dim
                    │   returns_fact   │
                    │  ╱          ╲    │
          daily_product_revenue_agg (id_date, id_product)
```

- Full diagram: [`docs/modelo_datos.md`](docs/modelo_datos.md) (rendered on GitHub) and [`docs/modelo_datos.dbml`](docs/modelo_datos.dbml) for dbdiagram.io.
- DDL: [`sql/ddl/`](sql/ddl). Runs when the container is created (`docker-entrypoint-initdb.d`) and again in the `create_dwh_tables` task (everything is `IF NOT EXISTS`).
- Why this design: [`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md#2-modelo-del-repositorio-analítico).

---

## 7. Technical decisions and ambiguous cases

Summary; the full reasoning, with numbers and discarded alternatives, is in **[`docs/decisiones_tecnicas.md`](docs/decisiones_tecnicas.md)**.

| Case | Decision |
|---|---|
| Transactions without CustomerID (24.9 %) | Kept with the generic `ANONYMOUS` customer (`id_customer = -1`): dropping them would remove 14.7 % of revenue |
| Different descriptions for one code | Canonical name = the most frequent one in uppercase; on a tie → the most recent. The number of variants is stored |
| Overlapping sources | Normalized composite natural key; if a line is in both, `data.csv` wins |
| POST, DOT, M, D, BANK CHARGES... codes | Rejected as `not_a_product_code` |
| Sale with price ≤ 0 | Rejected as `non_positive_price` in `audit.rejected_records` |
| Category without an API | Keyword rules seed with priority (`seeds/category_rules.csv`) |
| Idempotency | Deterministic transform + dimension upsert + fact delete-insert + `row_hash` as PK. Tested: two runs leave exactly the same rows |

---

## 8. Business questions

One query per question in [`sql/queries/`](sql/queries); answers with numbers in **[`docs/respuestas_negocio.md`](docs/respuestas_negocio.md)**.

| # | Question | Query | Short answer |
|---|---|---|---|
| 1 | Monthly net sales trend | `q1_monthly_net_sales.sql` | Peak in November 2011 (1.43 M); Sep-Nov = 35 % of the year |
| 2 | Categories by revenue and returns | `q2_category_revenue_returns.sql` | Home = 59.8 % of gross; Stationery has the most returns (16.2 %) |
| 3 | Top 10 by net revenue and top 10 by return rate | `q3_top_products.sql` | #1 REGENCY CAKESTAND (164k net) |
| 4 | Countries and average ticket | `q4_country_avg_ticket.sql` | United Kingdom = 90.5 % of invoices; Netherlands ticket 3,053 |
| 5 | Identified vs. no CustomerID | `q5_identified_vs_anonymous.sql` | No customer: ticket 1,101 vs 475, 96 products per invoice |
| 6 | Inconsistent descriptions and unique codes | `q6_inconsistent_descriptions.sql` | 3,927 codes; 637 (16.2 %) inconsistent |
| 7 | Recommendation to the product team | `q7_product_recommendation.sql` | Two cancelled orders = 51.7 % of returns → cap quantity per line |

---

## 9. Local development with uv

The ETL code can be tested without Docker. The virtual environment is managed with [uv](https://docs.astral.sh/uv/) and uses **the same versions** shipped in the Airflow 3.1.5 image (pandas 2.1.4, SQLAlchemy 1.4.54, pyarrow 18.1.0).

```bash
uv sync                                        # creates .venv with dependencies and dev tools
uv run pytest                                  # business rule tests (the DAG test is skipped without Airflow)
uv run ruff check . && uv run ruff format --check .
uv run python scripts/explore_sources.py       # profile of the two sources
```

Airflow is not installed in the local environment on purpose: it is heavy and already runs in Docker. That's why the logic lives in `include/datamart/` (plain pandas + SQL) and the DAG only orchestrates.

---

## 10. Repository layout

```
practice-test-airflow/
├── docker-compose.yml          # Airflow 3.1.5 + metadata Postgres + DWH Postgres
├── .env.example                # Required environment variables (the real .env is in .gitignore)
├── pyproject.toml / uv.lock    # Local environment with uv
├── airflow/init/
│   ├── init_airflow.sh         # Connections + Variables on startup
│   └── variables.json          # Initial values of the Airflow Variables
├── dags/
│   └── datamart_etl_dag.py     # Orchestration (only tasks and dependencies)
├── include/datamart/           # ETL logic
│   ├── config.py               # Paths, Airflow ids, rules and rejection reasons
│   ├── extract.py              # Reading CSVs and seeds
│   ├── transform.py            # Cleaning, UTC, dedup, canonical name, category
│   ├── quality.py              # Sales vs returns and data contract
│   ├── load.py                 # COPY, upserts, delete-insert, aggregate and checks
│   └── log_utils.py            # Logging helpers
├── sql/
│   ├── ddl/                    # Schemas and tables (run when the DWH starts)
│   └── queries/                # One query per business question
├── seeds/                      # Category rules and country mapping
├── data/raw/                   # Kaggle source CSVs
├── docs/                       # Decisions, exploration, model and answers (Spanish)
├── scripts/explore_sources.py  # Data exploration
├── tests/                      # pytest tests
├── config/ logs/ plugins/      # Folders mounted by Airflow
└── README.md / README.es.md    # Docs in English and Spanish
```

---

## 11. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `airflow-init` fails with `Permission denied` on `logs/` (Linux) | UID different from the image's | `echo "AIRFLOW_UID=$(id -u)" >> .env` and `docker compose up -d` |
| `Define DWH_PASSWORD in .env` on startup | There is no `.env` | `cp .env.example .env` |
| The DWH tables are not created | The volume already existed from a previous run | `docker compose down -v` and start again (the `create_dwh_tables` task creates them anyway) |
| The `transform_and_merge` task runs out of memory | Docker has too little RAM | Give Docker at least 4 GB |
| `airflow-init` fails with `set: pipefail: invalid option name` or `$'\r': command not found` (Windows) | The repo was cloned with CRLF line endings | With `.gitattributes` this no longer happens on a fresh clone. On an existing copy: `git rm -r --cached . -q` and `git reset --hard` (discards uncommitted changes), then `docker compose up -d` |
| The DAG fails after updating the repo (Variable names, tables or values changed) | The metadata DB and the DWH still hold data from the previous version | `docker compose down -v` and `docker compose up -d` to start from scratch |
| Port 5433 or 8080 is taken | Another local service | Change `DWH_HOST_PORT` in `.env` or the api-server port in the compose file |
