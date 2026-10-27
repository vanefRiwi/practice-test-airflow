-- =============================================================================
-- Q6a. Products without a consistent description (more than one spelling)
-- =============================================================================
SELECT stock_code,
       product_description AS canonical_name,
       description_variants,
       category
FROM dwh.products_dim
WHERE description_variants > 1
ORDER BY description_variants DESC, stock_code;

-- =============================================================================
-- Q6b. Totals: unique product codes and how many have inconsistent descriptions
-- =============================================================================
SELECT COUNT(*)                                          AS unique_product_codes,
       COUNT(*) FILTER (WHERE description_variants > 1)  AS with_inconsistent_description,
       ROUND(100.0 * COUNT(*) FILTER (WHERE description_variants > 1) / COUNT(*), 2)
                                                         AS pct_inconsistent,
       COUNT(*) FILTER (WHERE description_variants = 0)  AS without_description
FROM dwh.products_dim;

-- Q6c. Codes that came in the sources but are not catalog products
SELECT UPPER(REGEXP_REPLACE(raw_record ->> 'stock_code', '\s', '', 'g')) AS code,
       COUNT(*) AS lines
FROM audit.rejected_records
WHERE reason = 'not_a_product_code' AND source_system = 'daily_sales'
GROUP BY 1
ORDER BY lines DESC;
