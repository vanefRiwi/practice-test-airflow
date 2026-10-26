-- =============================================================================
-- P7. Base numérica de la recomendación al equipo de producto:
-- productos con mucho revenue bruto que pierden una parte relevante en devoluciones.
-- =============================================================================
WITH producto AS (
    SELECT p.stock_code,
           p.product_description,
           p.category,
           SUM(a.gross_revenue)  AS gross_revenue,
           SUM(a.returns_amount) AS returns_amount,
           SUM(a.net_revenue)    AS net_revenue
    FROM dwh.daily_product_revenue_agg a
    JOIN dwh.products_dim p ON p.id_product = a.id_product
    GROUP BY p.stock_code, p.product_description, p.category
)
SELECT stock_code,
       product_description,
       category,
       gross_revenue,
       returns_amount,
       net_revenue,
       ROUND(100.0 * returns_amount / gross_revenue, 2)                          AS pct_devolucion,
       ROUND(100.0 * returns_amount / SUM(returns_amount) OVER (), 2)           AS pct_de_todas_las_devoluciones
FROM producto
WHERE gross_revenue >= 5000
ORDER BY returns_amount DESC
LIMIT 15;
