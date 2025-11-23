INSERT INTO warehouse.dim_order (order_id, order_transac_date, order_estimated_arrival)
SELECT DISTINCT order_id, order_transac_date, order_estimated_arrival 
FROM staging.order;