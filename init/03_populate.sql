INSERT INTO warehouse.dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT,
    datum,
    EXTRACT(ISODOW FROM datum),
    EXTRACT(MONTH FROM datum),
    TO_CHAR(datum, 'Day'),
    EXTRACT(DAY FROM datum),
    EXTRACT(DOY FROM datum),
    EXTRACT(WEEK FROM datum),
    TO_CHAR(datum, 'Month'),
    EXTRACT(QUARTER FROM datum),
    EXTRACT(YEAR FROM datum)

FROM (
    SELECT generate_series(
        '2020-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) AS datum
) date_sequence
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_order_date
SELECT * FROM warehouse.dim_date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_user_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT,
    datum,
    EXTRACT(ISODOW FROM datum),
    EXTRACT(MONTH FROM datum),
    TO_CHAR(datum, 'Day'),
    EXTRACT(DAY FROM datum),
    EXTRACT(DOY FROM datum),
    EXTRACT(WEEK FROM datum),
    TO_CHAR(datum, 'Month'),
    EXTRACT(QUARTER FROM datum),
    EXTRACT(YEAR FROM datum)
FROM (
    SELECT generate_series(
        '1970-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) AS datum
) date_sequence
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_staff_date
SELECT * FROM warehouse.dim_date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_merchant_date
SELECT * FROM warehouse.dim_date
ON CONFLICT (date_key) DO NOTHING;