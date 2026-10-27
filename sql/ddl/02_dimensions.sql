-- =============================================================================
-- 02. Dimensions (surrogate keys id_<entity>, natural key with UNIQUE)
-- =============================================================================

CREATE TABLE IF NOT EXISTS dwh.dates_dim (
    id_date      INTEGER PRIMARY KEY,           -- YYYYMMDD
    full_date    DATE        NOT NULL,
    year         SMALLINT    NOT NULL,
    quarter      SMALLINT    NOT NULL,
    month        SMALLINT    NOT NULL,
    month_name   VARCHAR(15) NOT NULL,
    year_month   CHAR(7)     NOT NULL,          -- 2011-11
    day          SMALLINT    NOT NULL,
    day_of_week  SMALLINT    NOT NULL,          -- 1 = Monday ... 7 = Sunday (ISO)
    day_name     VARCHAR(15) NOT NULL,
    is_weekend   BOOLEAN     NOT NULL,
    CONSTRAINT uq_dates_full_date UNIQUE (full_date),
    CONSTRAINT ck_dates_month_range CHECK (month BETWEEN 1 AND 12)
);

CREATE TABLE IF NOT EXISTS dwh.products_dim (
    id_product           SERIAL PRIMARY KEY,
    stock_code           VARCHAR(20)  NOT NULL,
    product_description  VARCHAR(255) NOT NULL,   -- canonical name
    category             VARCHAR(50)  NOT NULL,
    description_variants SMALLINT     NOT NULL,   -- different spellings in the sources
    CONSTRAINT uq_products_stock_code UNIQUE (stock_code),
    CONSTRAINT ck_products_variants_positive CHECK (description_variants >= 0)
);

CREATE TABLE IF NOT EXISTS dwh.customers_dim (
    id_customer   SERIAL PRIMARY KEY,
    customer_code VARCHAR(20) NOT NULL,
    is_identified BOOLEAN     NOT NULL,
    CONSTRAINT uq_customers_code UNIQUE (customer_code)
);

-- Generic customer for transactions without CustomerID (documented decision)
INSERT INTO dwh.customers_dim (id_customer, customer_code, is_identified)
VALUES (-1, 'ANONYMOUS', FALSE)
ON CONFLICT (id_customer) DO NOTHING;

CREATE TABLE IF NOT EXISTS dwh.countries_dim (
    id_country   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    CONSTRAINT uq_countries_name UNIQUE (country_name)
);
