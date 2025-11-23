INSERT INTO warehouse.dim_date (
    date_key,
    date_full,
    date_year,
    date_day_of_week,
    date_month_num,
    date_day_name,
    date_day_of_month,
    date_day_of_year,
    date_week_of_year,
    date_month_name,
    date_quarter
)
SELECT
    CAST(FORMAT(date_full, 'yyyyMMdd') AS INT) AS date_key,
    date_full,
    YEAR(date_full) AS date_year,
    DATEPART(dw, date_full) AS date_day_of_week,
    MONTH(date_full) AS date_month_num,
    DATENAME(dw, date_full) AS date_day_name,
    DAY(date_full) AS date_day_of_month,
    DATEPART(dy, date_full) AS date_day_of_year,
    DATEPART(wk, date_full) AS date_week_of_year,
    DATENAME(month, date_full) AS date_month_name,
    DATEPART(qq, date_full) AS date_quarter
FROM staging.date;