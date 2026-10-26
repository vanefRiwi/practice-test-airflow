-- =============================================================================
-- P4. Países que concentran las transacciones y ticket promedio por país
-- Transacción = factura de venta (invoice_no). Ticket = bruto / facturas.
-- =============================================================================
SELECT c.country_name,
       COUNT(DISTINCT s.invoice_no)                                   AS facturas,
       ROUND(100.0 * COUNT(DISTINCT s.invoice_no)
             / SUM(COUNT(DISTINCT s.invoice_no)) OVER (), 2)          AS pct_facturas,
       SUM(s.gross_revenue)                                           AS gross_revenue,
       ROUND(SUM(s.gross_revenue) / COUNT(DISTINCT s.invoice_no), 2)  AS ticket_promedio,
       COUNT(DISTINCT s.id_customer) FILTER (WHERE s.id_customer <> -1) AS clientes
FROM dwh.sales_fact s
JOIN dwh.countries_dim c ON c.id_country = s.id_country
GROUP BY c.country_name
ORDER BY facturas DESC;
