-- =============================================================================
-- Q4. Countries with the most transactions and average ticket per country
-- Transaction = sales invoice (invoice_no). Ticket = gross / invoices.
-- =============================================================================
SELECT c.country_name,
       COUNT(DISTINCT s.invoice_no)                                   AS invoices,
       ROUND(100.0 * COUNT(DISTINCT s.invoice_no)
             / SUM(COUNT(DISTINCT s.invoice_no)) OVER (), 2)          AS pct_invoices,
       SUM(s.gross_revenue)                                           AS gross_revenue,
       ROUND(SUM(s.gross_revenue) / COUNT(DISTINCT s.invoice_no), 2)  AS avg_ticket,
       COUNT(DISTINCT s.id_customer) FILTER (WHERE s.id_customer <> -1) AS customers
FROM dwh.sales_fact s
JOIN dwh.countries_dim c ON c.id_country = s.id_country
GROUP BY c.country_name
ORDER BY invoices DESC;
