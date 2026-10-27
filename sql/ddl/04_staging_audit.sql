-- =============================================================================
-- 04. Staging (UNLOGGED: landing tables, no WAL needed) and audit
-- =============================================================================

CREATE UNLOGGED TABLE IF NOT EXISTS staging.products_stg (
    stock_code           VARCHAR(20)  NOT NULL,
    product_description  VARCHAR(255) NOT NULL,
    category             VARCHAR(50)  NOT NULL,
    description_variants SMALLINT     NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.sales_stg (
    row_hash       CHAR(32)      NOT NULL,
    source_system  VARCHAR(20)   NOT NULL,
    invoice_no     VARCHAR(20)   NOT NULL,
    invoice_ts_utc TIMESTAMPTZ   NOT NULL,
    stock_code     VARCHAR(20)   NOT NULL,
    customer_code  VARCHAR(20),
    country_name   VARCHAR(100)  NOT NULL,
    quantity       INTEGER       NOT NULL,
    unit_price     NUMERIC(12,3) NOT NULL,
    gross_revenue  NUMERIC(14,2) NOT NULL,
    currency       CHAR(3)       NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.returns_stg (
    row_hash          CHAR(32)      NOT NULL,
    source_system     VARCHAR(20)   NOT NULL,
    invoice_no        VARCHAR(20)   NOT NULL,
    return_type       VARCHAR(20)   NOT NULL,
    invoice_ts_utc    TIMESTAMPTZ   NOT NULL,
    stock_code        VARCHAR(20)   NOT NULL,
    customer_code     VARCHAR(20),
    country_name      VARCHAR(100)  NOT NULL,
    quantity_returned INTEGER       NOT NULL,
    unit_price        NUMERIC(12,3) NOT NULL,
    return_amount     NUMERIC(14,2) NOT NULL,
    currency          CHAR(3)       NOT NULL
);

-- Rejected records log: the original record is stored as is in JSONB
CREATE TABLE IF NOT EXISTS audit.rejected_records (
    source_system VARCHAR(20)  NOT NULL,
    source_row    INTEGER      NOT NULL,      -- data row in the original CSV
    reason        VARCHAR(50)  NOT NULL,
    raw_record    JSONB        NOT NULL,
    run_id        VARCHAR(250) NOT NULL,      -- run that loaded the record
    CONSTRAINT pk_rejected_records PRIMARY KEY (source_system, source_row)
);

CREATE INDEX IF NOT EXISTS ix_rejected_reason ON audit.rejected_records (reason);

-- Run log (one row per DAG run)
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id                   VARCHAR(250) PRIMARY KEY,
    logical_date             TIMESTAMPTZ,
    rows_read_sales          INTEGER,
    rows_read_history        INTEGER,
    rows_valid               INTEGER,
    rows_rejected            INTEGER,
    duplicates_cross_source  INTEGER,
    rows_sales_fact          INTEGER,
    rows_returns_fact        INTEGER,
    rejected_ratio           NUMERIC(6,4),
    status                   VARCHAR(20)  NOT NULL,
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);
