-- =============================================================================
-- P6a. Productos sin descripción consistente (más de una forma de escribirla)
-- =============================================================================
SELECT stock_code,
       product_description AS nombre_canonico,
       description_variants,
       category
FROM dwh.products_dim
WHERE description_variants > 1
ORDER BY description_variants DESC, stock_code;

-- =============================================================================
-- P6b. Totales: códigos de producto únicos y cuántos tienen descripción inconsistente
-- =============================================================================
SELECT COUNT(*)                                          AS codigos_producto_unicos,
       COUNT(*) FILTER (WHERE description_variants > 1)  AS con_descripcion_inconsistente,
       ROUND(100.0 * COUNT(*) FILTER (WHERE description_variants > 1) / COUNT(*), 2)
                                                         AS pct_inconsistentes,
       COUNT(*) FILTER (WHERE description_variants = 0)  AS sin_descripcion
FROM dwh.products_dim;

-- P6c. Códigos que llegaron en las fuentes pero no son productos del catálogo
SELECT UPPER(REGEXP_REPLACE(raw_record ->> 'stock_code', '\s', '', 'g')) AS codigo,
       COUNT(*) AS lineas
FROM audit.rejected_records
WHERE reason = 'codigo_no_producto' AND source_system = 'ventas_diarias'
GROUP BY 1
ORDER BY lineas DESC;
