-- =============================================================================
-- Q1. Monthly net sales trend (gross - returns)
-- =============================================================================
SELECT d.year_month,
       SUM(a.gross_revenue)                                  AS gross_revenue,
       SUM(a.returns_amount)                                 AS returns_amount,
       SUM(a.net_revenue)                                    AS net_revenue,
       ROUND(100.0 * SUM(a.returns_amount)
             / NULLIF(SUM(a.gross_revenue), 0), 2)           AS return_pct,
       ROUND(100.0 * (SUM(a.net_revenue) - LAG(SUM(a.net_revenue)) OVER (ORDER BY d.year_month))
             / NULLIF(LAG(SUM(a.net_revenue)) OVER (ORDER BY d.year_month), 0), 2)
                                                             AS pct_change_vs_prev_month
FROM dwh.daily_product_revenue_agg a
JOIN dwh.dates_dim d ON d.id_date = a.id_date
GROUP BY d.year_month
ORDER BY d.year_month;
