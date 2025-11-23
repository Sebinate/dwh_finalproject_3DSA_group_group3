-- populate dim_date
INSERT INTO warehouse.dim_date
SELECT
    -- 1. date_key (YYYYMMDD)
    TO_CHAR(datum, 'yyyymmdd')::INT,

    -- 2. date_full
    datum,

    -- 3. date_day_of_week (Postgres ISODOW: Monday=1, Sunday=7)
    EXTRACT(ISODOW FROM datum),

    -- 4. date_month_num
    EXTRACT(MONTH FROM datum),

    -- 5. date_day_name
    TO_CHAR(datum, 'Day'),

    -- 6. date_day_of_month
    EXTRACT(DAY FROM datum),

    -- 7. date_day_of_year
    EXTRACT(DOY FROM datum),

    -- 8. date_week_of_year
    EXTRACT(WEEK FROM datum),

    -- 9. date_month_name
    TO_CHAR(datum, 'Month'),

    -- 10. date_quarter
    EXTRACT(QUARTER FROM datum),

    -- 11. date_year
    EXTRACT(YEAR FROM datum)

FROM (
    -- Generates 10 years of dates. Adjust start/end as needed.
    SELECT generate_series(
        '2020-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) AS datum
) date_sequence
ON CONFLICT (date_key) DO NOTHING;

-- populate outriggers
INSERT INTO warehouse.dim_order_date
SELECT * FROM dim_date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_user_date
SELECT
    -- 1. date_key (YYYYMMDD)
    TO_CHAR(datum, 'yyyymmdd')::INT,

    -- 2. date_full
    datum,

    -- 3. date_day_of_week (Postgres ISODOW: Monday=1, Sunday=7)
    EXTRACT(ISODOW FROM datum),

    -- 4. date_month_num
    EXTRACT(MONTH FROM datum),

    -- 5. date_day_name
    TO_CHAR(datum, 'Day'),

    -- 6. date_day_of_month
    EXTRACT(DAY FROM datum),

    -- 7. date_day_of_year
    EXTRACT(DOY FROM datum),

    -- 8. date_week_of_year
    EXTRACT(WEEK FROM datum),

    -- 9. date_month_name
    TO_CHAR(datum, 'Month'),

    -- 10. date_quarter
    EXTRACT(QUARTER FROM datum),

    -- 11. date_year
    EXTRACT(YEAR FROM datum)

FROM (
    -- Generates 10 years of dates. Adjust start/end as needed.
    SELECT generate_series(
        '1970-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) AS datum
) date_sequence
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_staff_date
SELECT * FROM dim_date
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO warehouse.dim_merchant_date
SELECT * FROM dim_date
ON CONFLICT (date_key) DO NOTHING;