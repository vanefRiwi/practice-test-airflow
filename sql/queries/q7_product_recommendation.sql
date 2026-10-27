-- =============================================================================
-- Q7. Numbers behind the recommendation to the product team:
-- products with high gross revenue that lose a relevant share to returns.
-- =============================================================================
WITH product AS (
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
       ROUND(100.0 * returns_amount / gross_revenue, 2)                AS return_pct,
       ROUND(100.0 * returns_amount / SUM(returns_amount) OVER (), 2)  AS pct_of_all_returns
FROM product
WHERE gross_revenue >= 5000
ORDER BY returns_amount DESC
LIMIT 15;
