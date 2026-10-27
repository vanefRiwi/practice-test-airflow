-- =============================================================================
-- Q5. Buying behaviour: identified customers vs. transactions without
-- CustomerID (generic customer id_customer = -1, ANONYMOUS)
-- =============================================================================
WITH sales AS (
    SELECT cu.is_identified,
           COUNT(DISTINCT s.invoice_no)                         AS invoices,
           COUNT(*)                                             AS lines,
           SUM(s.quantity)                                      AS units,
           SUM(s.gross_revenue)                                 AS gross_revenue,
           ROUND(AVG(s.unit_price), 2)                          AS avg_unit_price
    FROM dwh.sales_fact s
    JOIN dwh.customers_dim cu ON cu.id_customer = s.id_customer
    GROUP BY cu.is_identified
),
returns AS (
    SELECT cu.is_identified, SUM(r.return_amount) AS returns_amount
    FROM dwh.returns_fact r
    JOIN dwh.customers_dim cu ON cu.id_customer = r.id_customer
    GROUP BY cu.is_identified
)
SELECT CASE WHEN s.is_identified THEN 'Identified' ELSE 'No CustomerID' END AS customer_type,
       s.invoices,
       ROUND(100.0 * s.gross_revenue / SUM(s.gross_revenue) OVER (), 2) AS pct_of_gross,
       s.gross_revenue,
       ROUND(s.gross_revenue / s.invoices, 2)           AS avg_ticket,
       ROUND(1.0 * s.lines / s.invoices, 1)             AS products_per_invoice,
       ROUND(1.0 * s.units / s.lines, 1)                AS units_per_line,
       s.avg_unit_price,
       ROUND(100.0 * COALESCE(r.returns_amount, 0) / s.gross_revenue, 2) AS return_pct
FROM sales s
LEFT JOIN returns r ON r.is_identified = s.is_identified
ORDER BY s.is_identified DESC;
