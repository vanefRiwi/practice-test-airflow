-- =============================================================================
-- Q3a. Top 10 products by net revenue
-- =============================================================================
SELECT p.stock_code,
       p.product_description,
       p.category,
       SUM(a.gross_revenue)  AS gross_revenue,
       SUM(a.returns_amount) AS returns_amount,
       SUM(a.net_revenue)    AS net_revenue
FROM dwh.daily_product_revenue_agg a
JOIN dwh.products_dim p ON p.id_product = a.id_product
GROUP BY p.stock_code, p.product_description, p.category
ORDER BY net_revenue DESC
LIMIT 10;

-- =============================================================================
-- Q3b. Top 10 products by return rate (returned amount / gross).
-- Only products with at least 1,000 GBP of gross sales: without that floor a
-- product with a single returned sale would show up at 100 %.
-- =============================================================================
SELECT p.stock_code,
       p.product_description,
       p.category,
       SUM(a.units_sold)     AS units_sold,
       SUM(a.units_returned) AS units_returned,
       SUM(a.gross_revenue)  AS gross_revenue,
       SUM(a.returns_amount) AS returns_amount,
       ROUND(100.0 * SUM(a.returns_amount) / SUM(a.gross_revenue), 2) AS return_pct
FROM dwh.daily_product_revenue_agg a
JOIN dwh.products_dim p ON p.id_product = a.id_product
GROUP BY p.stock_code, p.product_description, p.category
HAVING SUM(a.gross_revenue) >= 1000
ORDER BY return_pct DESC
LIMIT 10;
