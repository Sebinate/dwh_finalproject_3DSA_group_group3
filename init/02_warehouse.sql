-- Insert CREATE TABLE table IF NOT EXISTS here, date outriggers first then dims, facts last

-- Outriggers
CREATE TABLE IF NOT EXISTS warehouse.dim_order_date (
    date_key             SERIAL PRIMARY KEY,        -- 20251123 -
    date_full            DATE NOT NULL,          -- 2025-11-23 -
    date_day_of_week     INT NOT NULL,           -- 1 to 7 -
    date_month_num       INT NOT NULL,           -- 1 - 12 -
    date_day_name        VARCHAR(10) NOT NULL,   -- 'Sunday'
    date_day_of_year     INT NOT NULL,           -- 327
    date_week_of_year    INT NOT NULL,           -- 47
    date_day_of_month    INT NOT NULL,           -- 11 -
    date_month_name      VARCHAR(10) NOT NULL,   -- 'November' -
    date_quarter         INT NOT NULL,           -- 4 -
    date_year            INT NOT NULL            -- 2025 -
);

CREATE TABLE IF NOT EXISTS warehouse.dim_user_date (
    date_key              SERIAL PRIMARY KEY,
    date_full             DATE NOT NULL,
    date_day_of_week      INT NOT NULL,
    date_month_num        INT NOT NULL,
    date_day_name         VARCHAR(10) NOT NULL,
    date_day_of_month     INT NOT NULL,
    date_day_of_year      INT NOT NULL,
    date_week_of_year     INT NOT NULL,
    date_month_name       VARCHAR(10) NOT NULL,
    date_quarter          INT NOT NULL,
    date_year             INT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_merchant_date (
    date_key              SERIAL PRIMARY KEY,
    date_full             DATE NOT NULL,
    date_day_of_week      INT NOT NULL,
    date_month_num        INT NOT NULL,
    date_day_name         VARCHAR(10) NOT NULL,
    date_day_of_month     INT NOT NULL,
    date_day_of_year      INT NOT NULL,
    date_week_of_year     INT NOT NULL,
    date_month_name       VARCHAR(10) NOT NULL,
    date_quarter          INT NOT NULL,
    date_year             INT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_staff_date (
    date_key              SERIAL PRIMARY KEY,
    date_full             DATE NOT NULL,
    date_day_of_week      INT NOT NULL,
    date_month_num        INT NOT NULL,
    date_day_name         VARCHAR(10) NOT NULL,
    date_day_of_month     INT NOT NULL,
    date_day_of_year      INT NOT NULL,
    date_week_of_year     INT NOT NULL,
    date_month_name       VARCHAR(10) NOT NULL,
    date_quarter          INT NOT NULL,
    date_year             INT NOT NULL
);

-- Dimensions
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key              SERIAL PRIMARY KEY,
    date_full             DATE NOT NULL,
    date_day_of_week      INT NOT NULL,
    date_month_num        INT NOT NULL,
    date_day_name         VARCHAR(10) NOT NULL,
    date_day_of_month     INT NOT NULL,
    date_day_of_year      INT NOT NULL,
    date_week_of_year     INT NOT NULL,
    date_month_name       VARCHAR(10) NOT NULL,
    date_quarter          INT NOT NULL,
    date_year             INT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key           SERIAL PRIMARY KEY,        
    product_id            VARCHAR(16) NOT NULL,
    product_name          VARCHAR(255),
    product_type          VARCHAR(50),
    product_price         NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_campaign (
    campaign_key          SERIAL PRIMARY KEY,        
    campaign_id           VARCHAR(16) NOT NULL,
    campaign_name         VARCHAR(255),
    campaign_desc         VARCHAR(500),
    campaign_discount     NUMERIC(5, 2)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_staff (
    staff_key             SERIAL PRIMARY KEY,        
    staff_id              VARCHAR(16) NOT NULL,
    staff_name            VARCHAR(255),
    staff_job_level       VARCHAR(50),
    staff_street          VARCHAR(255),
    staff_state           VARCHAR(100),
    staff_city            VARCHAR(100),
    staff_country         VARCHAR(100),
    staff_contact_number  VARCHAR(20),
    staff_creation_date_key INT REFERENCES warehouse.dim_staff_date(date_key)
);


CREATE TABLE IF NOT EXISTS warehouse.dim_user (
    user_key              SERIAL PRIMARY KEY,        
    user_id               VARCHAR(16) NOT NULL,
    user_name             VARCHAR(255),
    user_ccn              VARCHAR(50),           
    user_bank             VARCHAR(100),
    user_street           VARCHAR(255),
    user_city             VARCHAR(100),
    user_country          VARCHAR(100),
    user_gender           VARCHAR(10),
    user_device_type      VARCHAR(50),
    user_type             VARCHAR(50),
    user_job_title        VARCHAR(100),
    user_job_level        VARCHAR(50),
    user_creation_date_key INT REFERENCES warehouse.dim_user_date(date_key),
    user_birthdate_key     INT REFERENCES warehouse.dim_user_date(date_key) 
);

CREATE TABLE IF NOT EXISTS warehouse.dim_merchant (
    merchant_key          SERIAL PRIMARY KEY,        
    merchant_id           VARCHAR(16) NOT NULL,
    merchant_name         VARCHAR(255),
    merchant_street       VARCHAR(255),
    merchant_state        VARCHAR(100),
    merchant_city         VARCHAR(100),
    merchant_country      VARCHAR(100),
    merchant_contact      VARCHAR(20),
    merchant_creation_date_key INT REFERENCES warehouse.dim_merchant_date(date_key)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_order (
    order_key                 SERIAL PRIMARY KEY,    
    order_id                  VARCHAR(100) NOT NULL,       
    order_estimated_arrival   INT,
    order_transac_date_key INT REFERENCES warehouse.dim_order_date(date_key)
);

-- Fact Tables
CREATE TABLE IF NOT EXISTS warehouse.fact_promotion_eligibility (
    campaign_key          INT REFERENCES warehouse.dim_campaign(campaign_key),
    order_key             INT REFERENCES warehouse.dim_order(order_key),
    PRIMARY KEY (campaign_key, order_key)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_transaction (
    date_key              INT REFERENCES warehouse.dim_date(date_key),
    staff_key             INT REFERENCES warehouse.dim_staff(staff_key),
    merchant_key          INT REFERENCES warehouse.dim_merchant(merchant_key),
    user_key              INT REFERENCES warehouse.dim_user(user_key),
    product_key           INT REFERENCES warehouse.dim_product(product_key),
    order_key             INT REFERENCES warehouse.dim_order(order_key),
    -- Measures
    delay                 INT,                    
    price                 NUMERIC(10, 2),
    quantity              INT,

    PRIMARY KEY (date_key, staff_key, merchant_key, user_key, product_key, order_key)
);