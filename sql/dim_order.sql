INSERT INTO warehouse.dim_order (order_id, order_transac_date_key, order_estimated_arrival)
SELECT DISTINCT s.order_id, COALESCE(w.date_key, -1), s.transact_estimated_arrival_days
FROM staging.order_data AS s
LEFT JOIN warehouse.dim_order_date AS w
    ON s.transact_date = w.date_full;