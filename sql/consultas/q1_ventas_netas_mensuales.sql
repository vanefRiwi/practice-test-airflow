-- =============================================================================
-- P1. Evolución mensual de las ventas netas (bruto - devoluciones)
-- =============================================================================
SELECT d.year_month,
       SUM(a.gross_revenue)                                  AS gross_revenue,
       SUM(a.returns_amount)                                 AS returns_amount,
       SUM(a.net_revenue)                                    AS net_revenue,
       ROUND(100.0 * SUM(a.returns_amount)
             / NULLIF(SUM(a.gross_revenue), 0), 2)           AS pct_devolucion,
       ROUND(100.0 * (SUM(a.net_revenue) - LAG(SUM(a.net_revenue)) OVER (ORDER BY d.year_month))
             / NULLIF(LAG(SUM(a.net_revenue)) OVER (ORDER BY d.year_month), 0), 2)
                                                             AS variacion_pct_vs_mes_anterior
FROM dwh.daily_product_revenue_agg a
JOIN dwh.dates_dim d ON d.id_date = a.id_date
GROUP BY d.year_month
ORDER BY d.year_month;
