-- =============================================================================
-- 03. Facts: separate sales and returns + daily revenue aggregate
-- Grain of sales and returns: one invoice line.
-- row_hash = MD5 of the natural key (invoice, product, quantity, price, UTC date,
-- customer). It is the primary key: the database rejects any duplicate and, since it
-- doesn't depend on a sequence, two runs leave exactly the same rows.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dwh.sales_fact (
    row_hash       CHAR(32)      PRIMARY KEY,   -- hashed natural key (stable between runs)
    source_system  VARCHAR(20)   NOT NULL,
    invoice_no     VARCHAR(20)   NOT NULL,
    invoice_ts_utc TIMESTAMPTZ   NOT NULL,
    id_date        INTEGER       NOT NULL,
    id_product     INTEGER       NOT NULL,
    id_customer    INTEGER       NOT NULL,
    id_country     INTEGER       NOT NULL,
    quantity       INTEGER       NOT NULL,
    unit_price     NUMERIC(12,3) NOT NULL,
    gross_revenue  NUMERIC(14,2) NOT NULL,
    currency       CHAR(3)       NOT NULL,
    CONSTRAINT fk_fact_sales_date     FOREIGN KEY (id_date)     REFERENCES dwh.dates_dim (id_date),
    CONSTRAINT fk_fact_sales_product  FOREIGN KEY (id_product)  REFERENCES dwh.products_dim (id_product),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (id_customer) REFERENCES dwh.customers_dim (id_customer),
    CONSTRAINT fk_fact_sales_country  FOREIGN KEY (id_country)  REFERENCES dwh.countries_dim (id_country),
    CONSTRAINT ck_sales_quantity_positive CHECK (quantity > 0),
    CONSTRAINT ck_sales_price_positive    CHECK (unit_price > 0),
    CONSTRAINT ck_sales_revenue_positive  CHECK (gross_revenue >= 0)
);

CREATE INDEX IF NOT EXISTS ix_sales_date     ON dwh.sales_fact (id_date);
CREATE INDEX IF NOT EXISTS ix_sales_product  ON dwh.sales_fact (id_product);
CREATE INDEX IF NOT EXISTS ix_sales_customer ON dwh.sales_fact (id_customer);
CREATE INDEX IF NOT EXISTS ix_sales_country  ON dwh.sales_fact (id_country);
CREATE INDEX IF NOT EXISTS ix_sales_source   ON dwh.sales_fact (source_system);

CREATE TABLE IF NOT EXISTS dwh.returns_fact (
    row_hash          CHAR(32)      PRIMARY KEY,
    source_system     VARCHAR(20)   NOT NULL,
    invoice_no        VARCHAR(20)   NOT NULL,
    return_type       VARCHAR(20)   NOT NULL,   -- cancellation (C invoice) | adjustment
    invoice_ts_utc    TIMESTAMPTZ   NOT NULL,
    id_date           INTEGER       NOT NULL,
    id_product        INTEGER       NOT NULL,
    id_customer       INTEGER       NOT NULL,
    id_country        INTEGER       NOT NULL,
    quantity_returned INTEGER       NOT NULL,   -- as a positive number
    unit_price        NUMERIC(12,3) NOT NULL,
    return_amount     NUMERIC(14,2) NOT NULL,   -- as a positive number
    currency          CHAR(3)       NOT NULL,
    CONSTRAINT fk_fact_returns_date     FOREIGN KEY (id_date)     REFERENCES dwh.dates_dim (id_date),
    CONSTRAINT fk_fact_returns_product  FOREIGN KEY (id_product)  REFERENCES dwh.products_dim (id_product),
    CONSTRAINT fk_fact_returns_customer FOREIGN KEY (id_customer) REFERENCES dwh.customers_dim (id_customer),
    CONSTRAINT fk_fact_returns_country  FOREIGN KEY (id_country)  REFERENCES dwh.countries_dim (id_country),
    CONSTRAINT ck_returns_quantity_positive CHECK (quantity_returned >= 0),
    CONSTRAINT ck_returns_price_positive    CHECK (unit_price >= 0),
    CONSTRAINT ck_returns_type_valid        CHECK (return_type IN ('cancellation', 'adjustment'))
);

CREATE INDEX IF NOT EXISTS ix_returns_date    ON dwh.returns_fact (id_date);
CREATE INDEX IF NOT EXISTS ix_returns_product ON dwh.returns_fact (id_product);
CREATE INDEX IF NOT EXISTS ix_returns_source  ON dwh.returns_fact (source_system);

-- Gross and net revenue per product and day (net = gross - returns of the same
-- code on the same UTC day). Rebuilt from scratch on every run.
CREATE TABLE IF NOT EXISTS dwh.daily_product_revenue_agg (
    id_date         INTEGER       NOT NULL,
    id_product      INTEGER       NOT NULL,
    sales_lines     INTEGER       NOT NULL,
    units_sold      INTEGER       NOT NULL,
    gross_revenue   NUMERIC(14,2) NOT NULL,
    return_lines    INTEGER       NOT NULL,
    units_returned  INTEGER       NOT NULL,
    returns_amount  NUMERIC(14,2) NOT NULL,
    net_revenue     NUMERIC(14,2) NOT NULL,
    CONSTRAINT pk_daily_product_revenue PRIMARY KEY (id_date, id_product),
    CONSTRAINT fk_fact_agg_date    FOREIGN KEY (id_date)    REFERENCES dwh.dates_dim (id_date),
    CONSTRAINT fk_fact_agg_product FOREIGN KEY (id_product) REFERENCES dwh.products_dim (id_product),
    CONSTRAINT ck_agg_net_matches CHECK (net_revenue = gross_revenue - returns_amount)
);

CREATE INDEX IF NOT EXISTS ix_agg_product ON dwh.daily_product_revenue_agg (id_product);
