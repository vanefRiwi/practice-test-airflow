-- =============================================================================
-- P2. Categorías con más revenue bruto y con mayor proporción de devoluciones
-- =============================================================================
SELECT p.category,
       COUNT(DISTINCT p.id_product)                          AS productos,
       SUM(a.gross_revenue)                                  AS gross_revenue,
       ROUND(100.0 * SUM(a.gross_revenue)
             / SUM(SUM(a.gross_revenue)) OVER (), 2)         AS pct_del_bruto_total,
       SUM(a.returns_amount)                                 AS returns_amount,
       ROUND(100.0 * SUM(a.returns_amount)
             / NULLIF(SUM(a.gross_revenue), 0), 2)           AS pct_devolucion_monto,
       ROUND(100.0 * SUM(a.units_returned)
             / NULLIF(SUM(a.units_sold), 0), 2)              AS pct_devolucion_unidades
FROM dwh.daily_product_revenue_agg a
JOIN dwh.products_dim p ON p.id_product = a.id_product
GROUP BY p.category
ORDER BY gross_revenue DESC;
