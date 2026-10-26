-- =============================================================================
-- P5. Comportamiento de compra: clientes identificados vs. transacciones sin
-- CustomerID (cliente genérico id_customer = -1, ANONIMO)
-- =============================================================================
WITH ventas AS (
    SELECT cu.is_identified,
           COUNT(DISTINCT s.invoice_no)                         AS facturas,
           COUNT(*)                                             AS lineas,
           SUM(s.quantity)                                      AS unidades,
           SUM(s.gross_revenue)                                 AS gross_revenue,
           ROUND(AVG(s.unit_price), 2)                          AS precio_unitario_promedio
    FROM dwh.sales_fact s
    JOIN dwh.customers_dim cu ON cu.id_customer = s.id_customer
    GROUP BY cu.is_identified
),
devoluciones AS (
    SELECT cu.is_identified, SUM(r.return_amount) AS returns_amount
    FROM dwh.returns_fact r
    JOIN dwh.customers_dim cu ON cu.id_customer = r.id_customer
    GROUP BY cu.is_identified
)
SELECT CASE WHEN v.is_identified THEN 'Identificado' ELSE 'Sin CustomerID' END AS tipo_cliente,
       v.facturas,
       ROUND(100.0 * v.gross_revenue / SUM(v.gross_revenue) OVER (), 2) AS pct_del_bruto,
       v.gross_revenue,
       ROUND(v.gross_revenue / v.facturas, 2)           AS ticket_promedio,
       ROUND(1.0 * v.lineas / v.facturas, 1)            AS productos_por_factura,
       ROUND(1.0 * v.unidades / v.lineas, 1)            AS unidades_por_linea,
       v.precio_unitario_promedio,
       ROUND(100.0 * COALESCE(d.returns_amount, 0) / v.gross_revenue, 2) AS pct_devolucion
FROM ventas v
LEFT JOIN devoluciones d ON d.is_identified = v.is_identified
ORDER BY v.is_identified DESC;
