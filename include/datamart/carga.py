"""Carga al DWH: staging con COPY, upsert de dimensiones y delete-insert de hechos."""

import io
from pathlib import Path

from sqlalchemy import text

from datamart import configuracion as cfg
from datamart.utilidades import formato_numero, sub, titulo

# =============================================================================
# 1. HELPERS
# =============================================================================


def ejecutar_ddl(engine, directorio_ddl=cfg.DIRECTORIO_DDL):
    """Ejecuta los scripts DDL en orden; son idempotentes (IF NOT EXISTS / ON CONFLICT)."""
    titulo("DDL DEL REPOSITORIO ANALÍTICO")
    scripts = sorted(Path(directorio_ddl).glob("*.sql"))
    if not scripts:
        raise ValueError(f"No hay scripts DDL en {directorio_ddl}")
    with engine.begin() as conexion:
        for script in scripts:
            sub(f"Ejecutando {script.name}")
            conexion.exec_driver_sql(script.read_text(encoding="utf-8"))


def _copiar(conexion, df, tabla, columnas):
    """Carga masiva con COPY (mucho más rápido que INSERT fila a fila para ~1M de filas)."""
    buffer = io.StringIO()
    df[columnas].to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor = conexion.connection.cursor()
    cursor.copy_expert(f"COPY {tabla} ({', '.join(columnas)}) FROM STDIN WITH (FORMAT csv)", buffer)
    sub(f"COPY {tabla}: {formato_numero(len(df))} filas")


def _contar(conexion, sql, **parametros):
    """Devuelve el resultado escalar de una consulta."""
    return conexion.execute(text(sql), parametros).scalar()


# =============================================================================
# 2. STAGING Y RECHAZOS
# =============================================================================


def cargar_staging(engine, productos, ventas, devoluciones):
    """Vacía y llena las tablas staging en una sola transacción."""
    titulo("CARGA A STAGING")
    with engine.begin() as conexion:
        conexion.execute(
            text("TRUNCATE staging.products_stg, staging.sales_stg, staging.returns_stg;")
        )
        _copiar(
            conexion,
            productos,
            "staging.products_stg",
            ["stock_code", "product_description", "category", "description_variants"],
        )
        _copiar(
            conexion,
            ventas,
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
        _copiar(
            conexion,
            devoluciones,
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


def cargar_rechazos(engine, rechazos, run_id):
    """Reemplaza el log de rechazos de las fuentes procesadas (delete-insert idempotente)."""
    titulo("LOG DE RECHAZOS")
    rechazos = rechazos.assign(run_id=run_id)
    with engine.begin() as conexion:
        borradas = conexion.execute(
            text("DELETE FROM audit.rejected_records WHERE source_system = ANY(:fuentes);"),
            {"fuentes": cfg.FUENTES},
        ).rowcount
        sub(f"Rechazos anteriores borrados: {formato_numero(borradas)}")
        _copiar(
            conexion,
            rechazos,
            "audit.rejected_records",
            ["source_system", "source_row", "reason", "raw_record", "run_id"],
        )
    return len(rechazos)


# =============================================================================
# 3. DIMENSIONES (upsert: las llaves subrogadas no cambian entre ejecuciones)
# =============================================================================

SQL_DIM_FECHAS = """
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

SQL_DIM_PRODUCTOS_UPDATE = """
UPDATE dwh.products_dim p
SET product_description  = s.product_description,
    category             = s.category,
    description_variants = s.description_variants
FROM staging.products_stg s
WHERE p.stock_code = s.stock_code
  AND (p.product_description, p.category, p.description_variants)
      IS DISTINCT FROM (s.product_description, s.category, s.description_variants);
"""

SQL_DIM_PRODUCTOS_INSERT = """
INSERT INTO dwh.products_dim (stock_code, product_description, category, description_variants)
SELECT s.stock_code, s.product_description, s.category, s.description_variants
FROM staging.products_stg s
WHERE NOT EXISTS (SELECT 1 FROM dwh.products_dim p WHERE p.stock_code = s.stock_code)
ORDER BY s.stock_code;
"""

SQL_DIM_CLIENTES = """
INSERT INTO dwh.customers_dim (customer_code, is_identified)
SELECT DISTINCT customer_code, TRUE
FROM (SELECT customer_code FROM staging.sales_stg
      UNION
      SELECT customer_code FROM staging.returns_stg) c
WHERE customer_code IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dwh.customers_dim d WHERE d.customer_code = c.customer_code)
ORDER BY customer_code;
"""

SQL_DIM_PAISES = """
INSERT INTO dwh.countries_dim (country_name)
SELECT DISTINCT country_name
FROM (SELECT country_name FROM staging.sales_stg
      UNION
      SELECT country_name FROM staging.returns_stg) c
WHERE NOT EXISTS (SELECT 1 FROM dwh.countries_dim d WHERE d.country_name = c.country_name)
ORDER BY country_name;
"""


def cargar_dimensiones(engine):
    """Upsert de las 4 dimensiones a partir de staging (sin huecos en las secuencias)."""
    titulo("CARGA DE DIMENSIONES")
    resumen = {}
    with engine.begin() as conexion:
        resumen["fechas_nuevas"] = conexion.execute(text(SQL_DIM_FECHAS)).rowcount
        resumen["productos_actualizados"] = conexion.execute(
            text(SQL_DIM_PRODUCTOS_UPDATE)
        ).rowcount
        resumen["productos_nuevos"] = conexion.execute(text(SQL_DIM_PRODUCTOS_INSERT)).rowcount
        resumen["clientes_nuevos"] = conexion.execute(text(SQL_DIM_CLIENTES)).rowcount
        resumen["paises_nuevos"] = conexion.execute(text(SQL_DIM_PAISES)).rowcount
    sub(f"Resumen dimensiones: {resumen}")
    return resumen


# =============================================================================
# 4. HECHOS (delete-insert por fuente dentro de una transacción)
# =============================================================================

SQL_HECHO_VENTAS = """
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

SQL_HECHO_DEVOLUCIONES = """
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


def _cargar_hecho(engine, tabla_hecho, tabla_staging, sql_insert):
    """Borra lo de las fuentes procesadas e inserta desde staging; verifica el conteo."""
    with engine.begin() as conexion:
        esperadas = _contar(conexion, f"SELECT COUNT(*) FROM {tabla_staging}")
        borradas = conexion.execute(
            text(f"DELETE FROM {tabla_hecho} WHERE source_system = ANY(:fuentes);"),
            {"fuentes": cfg.FUENTES},
        ).rowcount
        insertadas = conexion.execute(text(sql_insert)).rowcount
        sub(
            f"{tabla_hecho}: borradas {formato_numero(borradas)} | insertadas "
            f"{formato_numero(insertadas)} | en staging {formato_numero(esperadas)}"
        )
        # Si un JOIN perdió filas (dimensión faltante) se aborta y la transacción hace rollback
        if insertadas != esperadas:
            raise ValueError(
                f"{tabla_hecho}: se insertaron {insertadas} filas de {esperadas} en staging"
            )
    return insertadas


def cargar_hecho_ventas(engine):
    """Carga dwh.sales_fact."""
    titulo("CARGA HECHO VENTAS")
    return _cargar_hecho(engine, "dwh.sales_fact", "staging.sales_stg", SQL_HECHO_VENTAS)


def cargar_hecho_devoluciones(engine):
    """Carga dwh.returns_fact."""
    titulo("CARGA HECHO DEVOLUCIONES")
    return _cargar_hecho(engine, "dwh.returns_fact", "staging.returns_stg", SQL_HECHO_DEVOLUCIONES)


# =============================================================================
# 5. AGREGADO DIARIO: REVENUE BRUTO Y NETO POR PRODUCTO
# =============================================================================

SQL_AGREGADO = """
INSERT INTO dwh.daily_product_revenue_agg (id_date, id_product, sales_lines, units_sold,
                                           gross_revenue, return_lines, units_returned,
                                           returns_amount, net_revenue)
WITH ventas AS (
    SELECT id_date, id_product, COUNT(*) AS lineas, SUM(quantity) AS unidades,
           SUM(gross_revenue) AS bruto
    FROM dwh.sales_fact
    GROUP BY id_date, id_product
),
devoluciones AS (
    SELECT id_date, id_product, COUNT(*) AS lineas, SUM(quantity_returned) AS unidades,
           SUM(return_amount) AS monto
    FROM dwh.returns_fact
    GROUP BY id_date, id_product
)
SELECT COALESCE(v.id_date, d.id_date),
       COALESCE(v.id_product, d.id_product),
       COALESCE(v.lineas, 0),
       COALESCE(v.unidades, 0),
       COALESCE(v.bruto, 0),
       COALESCE(d.lineas, 0),
       COALESCE(d.unidades, 0),
       COALESCE(d.monto, 0),
       COALESCE(v.bruto, 0) - COALESCE(d.monto, 0)
FROM ventas v
FULL OUTER JOIN devoluciones d
  ON d.id_date = v.id_date AND d.id_product = v.id_product;
"""


def construir_revenue_diario(engine):
    """Reconstruye el agregado completo (bruto, devoluciones y neto por producto y día)."""
    titulo("AGREGADO DIARIO DE REVENUE")
    with engine.begin() as conexion:
        conexion.execute(text("DELETE FROM dwh.daily_product_revenue_agg;"))
        filas = conexion.execute(text(SQL_AGREGADO)).rowcount
    sub(f"Filas producto-día: {formato_numero(filas)}")
    return filas


# =============================================================================
# 6. VALIDACIÓN POST-CARGA Y BITÁCORA
# =============================================================================

CHEQUEOS_POST_CARGA = {
    "ventas_en_dwh_igual_a_staging": """
        SELECT (SELECT COUNT(*) FROM dwh.sales_fact WHERE source_system = ANY(:fuentes))
             = (SELECT COUNT(*) FROM staging.sales_stg)""",
    "devoluciones_en_dwh_igual_a_staging": """
        SELECT (SELECT COUNT(*) FROM dwh.returns_fact WHERE source_system = ANY(:fuentes))
             = (SELECT COUNT(*) FROM staging.returns_stg)""",
    "bruto_agregado_cuadra_con_ventas": """
        SELECT (SELECT COALESCE(SUM(gross_revenue), 0) FROM dwh.daily_product_revenue_agg)
             = (SELECT COALESCE(SUM(gross_revenue), 0) FROM dwh.sales_fact)""",
    "devoluciones_agregado_cuadra": """
        SELECT (SELECT COALESCE(SUM(returns_amount), 0) FROM dwh.daily_product_revenue_agg)
             = (SELECT COALESCE(SUM(return_amount), 0) FROM dwh.returns_fact)""",
    "sin_ventas_con_precio_no_positivo": """
        SELECT NOT EXISTS (SELECT 1 FROM dwh.sales_fact WHERE unit_price <= 0)""",
    "todos_los_productos_tienen_categoria": """
        SELECT NOT EXISTS (SELECT 1 FROM dwh.products_dim WHERE category IS NULL)""",
}


def validar_carga(engine, metricas, run_id, logical_date):
    """Corre los chequeos SQL post-carga y registra la ejecución en audit.pipeline_runs."""
    titulo("VALIDACIÓN POST-CARGA")
    fallidos = []
    with engine.connect() as conexion:
        for nombre, sql in CHEQUEOS_POST_CARGA.items():
            ok = bool(_contar(conexion, sql, fuentes=cfg.FUENTES))
            sub(f"{'OK   ' if ok else 'FALLA'} {nombre}")
            if not ok:
                fallidos.append(nombre)
        filas_ventas = _contar(conexion, "SELECT COUNT(*) FROM dwh.sales_fact")
        filas_devoluciones = _contar(conexion, "SELECT COUNT(*) FROM dwh.returns_fact")

    estado = "exitoso" if not fallidos else "fallido"
    with engine.begin() as conexion:
        conexion.execute(
            text(
                """
                INSERT INTO audit.pipeline_runs (run_id, logical_date, rows_read_sales,
                    rows_read_history, rows_valid, rows_rejected, duplicates_cross_source,
                    rows_sales_fact, rows_returns_fact, rejected_ratio, status, updated_at)
                VALUES (:run_id, :logical_date, :leidas_ventas, :leidas_historico, :validas,
                    :rechazadas, :duplicados, :ventas, :devoluciones, :ratio, :estado, now())
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
                "leidas_ventas": metricas["filas_leidas_ventas"],
                "leidas_historico": metricas["filas_leidas_historico"],
                "validas": metricas["filas_validas"],
                "rechazadas": metricas["filas_rechazadas"],
                "duplicados": metricas["duplicados_entre_fuentes"],
                "ventas": filas_ventas,
                "devoluciones": filas_devoluciones,
                "ratio": metricas["ratio_rechazos"],
                "estado": estado,
            },
        )

    if fallidos:
        raise ValueError(f"Fallaron los chequeos post-carga: {fallidos}")
    sub(
        f"Ventas en DWH: {formato_numero(filas_ventas)} | "
        f"Devoluciones: {formato_numero(filas_devoluciones)}"
    )
    return estado
