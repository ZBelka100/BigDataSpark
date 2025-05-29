\echo '–– Сколько строк в измерениях и факте'
SELECT 'dim_date'      AS table, COUNT(*) FROM staging.dim_date
UNION ALL
SELECT 'dim_product', COUNT(*) FROM staging.dim_product
UNION ALL
SELECT 'dim_customer',COUNT(*) FROM staging.dim_customer
UNION ALL
SELECT 'dim_seller',  COUNT(*) FROM staging.dim_seller
UNION ALL
SELECT 'fact_sales',  COUNT(*) FROM staging.fact_sales;

\echo '–– Быстрый кастомный sanity-check'
SELECT MIN(sale_date)  AS min_date,
       MAX(sale_date)  AS max_date,
       SUM(qty)        AS total_qty,
       SUM(revenue)    AS total_revenue
FROM   staging.fact_sales;
