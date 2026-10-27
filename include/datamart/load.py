"""Loading into the Postgres DWH."""

import io
from pathlib import Path

from sqlalchemy import text

from datamart import config as cfg
from datamart.log_utils import fmt_int, section, step

# =============================================================================
# 1. HELPERS
# =============================================================================


def run_ddl(engine, ddl_dir=cfg.DDL_DIR):
    """Runs the .sql files in sql/ddl in order."""
    section("DWH DDL")
    scripts = sorted(Path(ddl_dir).glob("*.sql"))
    if not scripts:
        raise ValueError(f"No DDL scripts in {ddl_dir}")
    with engine.begin() as conn:
        for script in scripts:
            step(f"Running {script.name}")
            conn.exec_driver_sql(script.read_text(encoding="utf-8"))


def _copy(conn, df, table, columns):
    """COPY from a DataFrame (to_sql was too slow with this many rows)."""
    buffer = io.StringIO()
    df[columns].to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor = conn.connection.cursor()
    cursor.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)", buffer)
    step(f"COPY {table}: {fmt_int(len(df))} rows")


def _scalar(conn, sql, **params):
    """Returns the scalar result of a query."""
    return conn.execute(text(sql), params).scalar()


# =============================================================================
# 2. STAGING AND REJECTIONS
# =============================================================================


def load_staging(engine, products, sales, returns):
    """Truncate + COPY of the staging tables."""
    section("STAGING LOAD")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE staging.products_stg, staging.sales_stg, staging.returns_stg;"))
        _copy(
            conn,
            products,
            "staging.products_stg",
            ["stock_code", "product_description", "category", "description_variants"],
        )
        _copy(
            conn,
            sales,
            "staging.sales_stg",
            [
                "row_hash",
                "source_system",
                "invoice_no",
                "invoice_ts_utc",
                "stock_code",
                "customer_code",
                "country_name",
                "quantity",
                "unit_price",
                "gross_revenue",
                "currency",
            ],
        )
        _copy(
            conn,
            returns,
            "staging.returns_stg",
            [
                "row_hash",
                "source_system",
                "invoice_no",
                "return_type",
                "invoice_ts_utc",
                "stock_code",
                "customer_code",
                "country_name",
                "quantity_returned",
                "unit_price",
                "return_amount",
                "currency",
            ],
        )


def load_rejections(engine, rejections, run_id):
    """Deletes the previous rejections and loads the ones from this run."""
    section("REJECTION LOG")
    rejections = rejections.assign(run_id=run_id)
    with engine.begin() as conn:
        deleted = conn.execute(
            text("DELETE FROM audit.rejected_records WHERE source_system = ANY(:sources);"),
            {"sources": cfg.SOURCES},
        ).rowcount
        step(f"Previous rejections deleted: {fmt_int(deleted)}")
        _copy(
            conn,
            rejections,
            "audit.rejected_records",
            ["source_system", "source_row", "reason", "raw_record", "run_id"],
        )
    return len(rejections)


# =============================================================================
# 3. DIMENSIONS (upsert so ids stay the same between runs)
# =============================================================================

SQL_DIM_DATES = """
INSERT INTO dwh.dates_dim (id_date, full_date, year, quarter, month, month_name, year_month,
                           day, day_of_week, day_name, is_weekend)
SELECT TO_CHAR(d, 'YYYYMMDD')::INTEGER,
       d::DATE,
       EXTRACT(YEAR FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH FROM d),
       TRIM(TO_CHAR(d, 'Month')),
       TO_CHAR(d, 'YYYY-MM'),
       EXTRACT(DAY FROM d),
       EXTRACT(ISODOW FROM d),
       TRIM(TO_CHAR(d, 'Day')),
       EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(
        (SELECT MIN(f)::DATE FROM (
            SELECT MIN(invoice_ts_utc AT TIME ZONE 'UTC') AS f FROM staging.sales_stg
            UNION ALL
            SELECT MIN(invoice_ts_utc AT TIME ZONE 'UTC') FROM staging.returns_stg) m),
        (SELECT MAX(f)::DATE FROM (
            SELECT MAX(invoice_ts_utc AT TIME ZONE 'UTC') AS f FROM staging.sales_stg
            UNION ALL
            SELECT MAX(invoice_ts_utc AT TIME ZONE 'UTC') FROM staging.returns_stg) m),
        INTERVAL '1 day') AS d
WHERE NOT EXISTS (SELECT 1 FROM dwh.dates_dim x WHERE x.full_date = d::DATE);
"""

SQL_DIM_PRODUCTS_UPDATE = """
UPDATE dwh.products_dim p
SET product_description  = s.product_description,
    category             = s.category,
    description_variants = s.description_variants
FROM staging.products_stg s
WHERE p.stock_code = s.stock_code
  AND (p.product_description, p.category, p.description_variants)
      IS DISTINCT FROM (s.product_description, s.category, s.description_variants);
"""

SQL_DIM_PRODUCTS_INSERT = """
INSERT INTO dwh.products_dim (stock_code, product_description, category, description_variants)
SELECT s.stock_code, s.product_description, s.category, s.description_variants
FROM staging.products_stg s
WHERE NOT EXISTS (SELECT 1 FROM dwh.products_dim p WHERE p.stock_code = s.stock_code)
ORDER BY s.stock_code;
"""

SQL_DIM_CUSTOMERS = """
INSERT INTO dwh.customers_dim (customer_code, is_identified)
SELECT DISTINCT customer_code, TRUE
FROM (SELECT customer_code FROM staging.sales_stg
      UNION
      SELECT customer_code FROM staging.returns_stg) c
WHERE customer_code IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dwh.customers_dim d WHERE d.customer_code = c.customer_code)
ORDER BY customer_code;
"""

SQL_DIM_COUNTRIES = """
INSERT INTO dwh.countries_dim (country_name)
SELECT DISTINCT country_name
FROM (SELECT country_name FROM staging.sales_stg
      UNION
      SELECT country_name FROM staging.returns_stg) c
WHERE NOT EXISTS (SELECT 1 FROM dwh.countries_dim d WHERE d.country_name = c.country_name)
ORDER BY country_name;
"""


def load_dimensions(engine):
    """Inserts what is new and updates what changed in the dimensions."""
    section("DIMENSIONS LOAD")
    summary = {}
    with engine.begin() as conn:
        summary["new_dates"] = conn.execute(text(SQL_DIM_DATES)).rowcount
        summary["updated_products"] = conn.execute(text(SQL_DIM_PRODUCTS_UPDATE)).rowcount
        summary["new_products"] = conn.execute(text(SQL_DIM_PRODUCTS_INSERT)).rowcount
        summary["new_customers"] = conn.execute(text(SQL_DIM_CUSTOMERS)).rowcount
        summary["new_countries"] = conn.execute(text(SQL_DIM_COUNTRIES)).rowcount
    step(f"Dimensions summary: {summary}")
    return summary


# =============================================================================
# 4. FACTS (delete-insert)
# =============================================================================

SQL_FACT_SALES = """
INSERT INTO dwh.sales_fact (row_hash, source_system, invoice_no, invoice_ts_utc, id_date,
                            id_product, id_customer, id_country, quantity, unit_price,
                            gross_revenue, currency)
SELECT s.row_hash, s.source_system, s.invoice_no, s.invoice_ts_utc,
       TO_CHAR(s.invoice_ts_utc AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
       p.id_product, COALESCE(c.id_customer, -1), co.id_country,
       s.quantity, s.unit_price, s.gross_revenue, s.currency
FROM staging.sales_stg s
JOIN dwh.products_dim p        ON p.stock_code = s.stock_code
LEFT JOIN dwh.customers_dim c  ON c.customer_code = s.customer_code
JOIN dwh.countries_dim co      ON co.country_name = s.country_name
ON CONFLICT (row_hash) DO NOTHING;
"""

SQL_FACT_RETURNS = """
INSERT INTO dwh.returns_fact (row_hash, source_system, invoice_no, return_type, invoice_ts_utc,
                              id_date, id_product, id_customer, id_country, quantity_returned,
                              unit_price, return_amount, currency)
SELECT s.row_hash, s.source_system, s.invoice_no, s.return_type, s.invoice_ts_utc,
       TO_CHAR(s.invoice_ts_utc AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
       p.id_product, COALESCE(c.id_customer, -1), co.id_country,
       s.quantity_returned, s.unit_price, s.return_amount, s.currency
FROM staging.returns_stg s
JOIN dwh.products_dim p        ON p.stock_code = s.stock_code
LEFT JOIN dwh.customers_dim c  ON c.customer_code = s.customer_code
JOIN dwh.countries_dim co      ON co.country_name = s.country_name
ON CONFLICT (row_hash) DO NOTHING;
"""


def _load_fact(engine, fact_table, staging_table, sql_insert):
    """Delete-insert from staging, checking that no rows get lost."""
    with engine.begin() as conn:
        expected = _scalar(conn, f"SELECT COUNT(*) FROM {staging_table}")
        deleted = conn.execute(
            text(f"DELETE FROM {fact_table} WHERE source_system = ANY(:sources);"),
            {"sources": cfg.SOURCES},
        ).rowcount
        inserted = conn.execute(text(sql_insert)).rowcount
        step(
            f"{fact_table}: deleted {fmt_int(deleted)} | inserted {fmt_int(inserted)}"
            f" | in staging {fmt_int(expected)}"
        )
        # if the JOIN dropped rows something is missing in the dimensions -> rollback
        if inserted != expected:
            raise ValueError(f"{fact_table}: inserted {inserted} of {expected} staging rows")
    return inserted


def load_sales_fact(engine):
    """Loads dwh.sales_fact."""
    section("SALES FACT LOAD")
    return _load_fact(engine, "dwh.sales_fact", "staging.sales_stg", SQL_FACT_SALES)


def load_returns_fact(engine):
    """Loads dwh.returns_fact."""
    section("RETURNS FACT LOAD")
    return _load_fact(engine, "dwh.returns_fact", "staging.returns_stg", SQL_FACT_RETURNS)


# =============================================================================
# 5. DAILY AGGREGATE: GROSS AND NET REVENUE PER PRODUCT
# =============================================================================

SQL_DAILY_AGG = """
INSERT INTO dwh.daily_product_revenue_agg (id_date, id_product, sales_lines, units_sold,
                                           gross_revenue, return_lines, units_returned,
                                           returns_amount, net_revenue)
WITH sales AS (
    SELECT id_date, id_product, COUNT(*) AS lines, SUM(quantity) AS units,
           SUM(gross_revenue) AS gross
    FROM dwh.sales_fact
    GROUP BY id_date, id_product
),
returns AS (
    SELECT id_date, id_product, COUNT(*) AS lines, SUM(quantity_returned) AS units,
           SUM(return_amount) AS amount
    FROM dwh.returns_fact
    GROUP BY id_date, id_product
)
SELECT COALESCE(s.id_date, r.id_date),
       COALESCE(s.id_product, r.id_product),
       COALESCE(s.lines, 0),
       COALESCE(s.units, 0),
       COALESCE(s.gross, 0),
       COALESCE(r.lines, 0),
       COALESCE(r.units, 0),
       COALESCE(r.amount, 0),
       COALESCE(s.gross, 0) - COALESCE(r.amount, 0)
FROM sales s
FULL OUTER JOIN returns r
  ON r.id_date = s.id_date AND r.id_product = s.id_product;
"""


def build_daily_revenue(engine):
    """Rebuilds gross, returns and net revenue per product and day."""
    section("DAILY REVENUE AGGREGATE")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dwh.daily_product_revenue_agg;"))
        rows = conn.execute(text(SQL_DAILY_AGG)).rowcount
    step(f"Product-day rows: {fmt_int(rows)}")
    return rows


# =============================================================================
# 6. POST-LOAD CHECKS AND RUN LOG
# =============================================================================

POST_LOAD_CHECKS = {
    "sales_in_dwh_match_staging": """
        SELECT (SELECT COUNT(*) FROM dwh.sales_fact WHERE source_system = ANY(:sources))
             = (SELECT COUNT(*) FROM staging.sales_stg)""",
    "returns_in_dwh_match_staging": """
        SELECT (SELECT COUNT(*) FROM dwh.returns_fact WHERE source_system = ANY(:sources))
             = (SELECT COUNT(*) FROM staging.returns_stg)""",
    "agg_gross_matches_sales": """
        SELECT (SELECT COALESCE(SUM(gross_revenue), 0) FROM dwh.daily_product_revenue_agg)
             = (SELECT COALESCE(SUM(gross_revenue), 0) FROM dwh.sales_fact)""",
    "agg_returns_match_returns": """
        SELECT (SELECT COALESCE(SUM(returns_amount), 0) FROM dwh.daily_product_revenue_agg)
             = (SELECT COALESCE(SUM(return_amount), 0) FROM dwh.returns_fact)""",
    "no_sales_with_non_positive_price": """
        SELECT NOT EXISTS (SELECT 1 FROM dwh.sales_fact WHERE unit_price <= 0)""",
    "all_products_have_category": """
        SELECT NOT EXISTS (SELECT 1 FROM dwh.products_dim WHERE category IS NULL)""",
}


def validate_load(engine, metrics, run_id, logical_date):
    """Checks after loading + entry in audit.pipeline_runs."""
    section("POST-LOAD CHECKS")
    failed = []
    with engine.connect() as conn:
        for name, sql in POST_LOAD_CHECKS.items():
            ok = bool(_scalar(conn, sql, sources=cfg.SOURCES))
            step(f"{'OK  ' if ok else 'FAIL'} {name}")
            if not ok:
                failed.append(name)
        sales_rows = _scalar(conn, "SELECT COUNT(*) FROM dwh.sales_fact")
        returns_rows = _scalar(conn, "SELECT COUNT(*) FROM dwh.returns_fact")

    status = "success" if not failed else "failed"
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO audit.pipeline_runs (run_id, logical_date, rows_read_sales,
                    rows_read_history, rows_valid, rows_rejected, duplicates_cross_source,
                    rows_sales_fact, rows_returns_fact, rejected_ratio, status, updated_at)
                VALUES (:run_id, :logical_date, :read_sales, :read_history, :valid,
                    :rejected, :duplicates, :sales, :returns, :ratio, :status, now())
                ON CONFLICT (run_id) DO UPDATE SET
                    rows_read_sales = EXCLUDED.rows_read_sales,
                    rows_read_history = EXCLUDED.rows_read_history,
                    rows_valid = EXCLUDED.rows_valid,
                    rows_rejected = EXCLUDED.rows_rejected,
                    duplicates_cross_source = EXCLUDED.duplicates_cross_source,
                    rows_sales_fact = EXCLUDED.rows_sales_fact,
                    rows_returns_fact = EXCLUDED.rows_returns_fact,
                    rejected_ratio = EXCLUDED.rejected_ratio,
                    status = EXCLUDED.status,
                    updated_at = now();
                """
            ),
            {
                "run_id": run_id,
                "logical_date": logical_date,
                "read_sales": metrics["rows_read_sales"],
                "read_history": metrics["rows_read_history"],
                "valid": metrics["rows_valid"],
                "rejected": metrics["rows_rejected"],
                "duplicates": metrics["cross_source_duplicates"],
                "sales": sales_rows,
                "returns": returns_rows,
                "ratio": metrics["reject_ratio"],
                "status": status,
            },
        )

    if failed:
        raise ValueError(f"Post-load checks failed: {failed}")
    step(f"Sales in DWH: {fmt_int(sales_rows)} | Returns: {fmt_int(returns_rows)}")
    return status
