INSERT INTO warehouse.dim_product(product_id, product_name, product_type, product_price)
SELECT DISTINCT product_id, product_name, product_type, product_price 
FROM staging.product_list
ON CONFLICT (product_id) DO NOTHING;

TRUNCATE TABLE staging.product_list;