-- =============================================================================
-- Q2. Categories with the most gross revenue and the highest share of returns
-- =============================================================================
SELECT p.category,
       COUNT(DISTINCT p.id_product)                          AS products,
       SUM(a.gross_revenue)                                  AS gross_revenue,
       ROUND(100.0 * SUM(a.gross_revenue)
             / SUM(SUM(a.gross_revenue)) OVER (), 2)         AS pct_of_total_gross,
       SUM(a.returns_amount)                                 AS returns_amount,
       ROUND(100.0 * SUM(a.returns_amount)
             / NULLIF(SUM(a.gross_revenue), 0), 2)           AS return_pct_amount,
       ROUND(100.0 * SUM(a.units_returned)
             / NULLIF(SUM(a.units_sold), 0), 2)              AS return_pct_units
FROM dwh.daily_product_revenue_agg a
JOIN dwh.products_dim p ON p.id_product = a.id_product
GROUP BY p.category
ORDER BY gross_revenue DESC;
