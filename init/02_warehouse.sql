-- Insert CREATE TABLE table IF NOT EXISTS here, date outriggers first then dims, facts last
CREATE TABLE IF NOT EXISTS dim_order_date (
    date_key             INT PRIMARY KEY,        -- 20251123 -
    date_full            DATE NOT NULL,          -- 2025-11-23 -
    date_day_of_week     INT NOT NULL,           -- 1 to 7 -
    date_month_num       INT NOT NULL,           -- 1 - 12 -
    date_day_name        VARCHAR(10) NOT NULL,   -- 'Sunday' -
    date_day_of_month    INT NOT NULL,           -- 23
    date_day_of_year     INT NOT NULL,           -- 327
    date_week_of_year    INT NOT NULL,           -- 47
    date_day_of_month    INT NOT NULL,           -- 11 -
    date_month_name      VARCHAR(10) NOT NULL,   -- 'November' -
    date_quarter         INT NOT NULL,           -- 4 -
    date_year            INT NOT NULL            -- 2025 -
);