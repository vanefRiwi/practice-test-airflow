"""Paths, Connection/Variable names and pipeline rules."""

import os
from pathlib import Path

# =============================================================================
# PATHS (inside the container everything lives under /opt/airflow)
# =============================================================================
BASE_DIR = Path(os.environ.get("DATAMART_BASE_DIR", "/opt/airflow"))
DATA_DIR = BASE_DIR / "data"
TMP_DIR = DATA_DIR / "tmp"
SEEDS_DIR = BASE_DIR / "seeds"
DDL_DIR = BASE_DIR / "sql" / "ddl"

CATEGORY_RULES_FILE = SEEDS_DIR / "category_rules.csv"
COUNTRY_MAPPING_FILE = SEEDS_DIR / "country_mapping.csv"

# =============================================================================
# AIRFLOW: CONNECTIONS AND VARIABLES
# =============================================================================
CONN_DWH = "dwh_postgres"
CONN_FILES = "fs_datamart"

VAR_SALES_FILE = "datamart_sales_file"
VAR_HISTORY_FILE = "datamart_history_file"
VAR_TIMEZONE = "datamart_source_timezone"
VAR_MAX_REJECT_RATIO = "datamart_max_reject_ratio"
VAR_CURRENCY = "datamart_currency"

# =============================================================================
# SOURCES
# =============================================================================
SOURCE_SALES = "daily_sales"  # data.csv (daily dump, takes priority)
SOURCE_HISTORY = "history"  # online_retail.csv (extended Kaggle history)
SOURCES = [SOURCE_SALES, SOURCE_HISTORY]

# data.csv and online_retail.csv don't use the same column names
COLUMN_ALIASES = {
    "invoiceno": "invoice_no",
    "invoice": "invoice_no",
    "stockcode": "stock_code",
    "description": "description",
    "quantity": "quantity",
    "invoicedate": "invoice_date_raw",
    "unitprice": "unit_price",
    "price": "unit_price",
    "customerid": "customer_id",
    "customer id": "customer_id",
    "country": "country",
}
STANDARD_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date_raw",
    "unit_price",
    "customer_id",
    "country",
]

# Date formats tried in order for each source
DATE_FORMATS = {
    SOURCE_SALES: ["%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S"],
    SOURCE_HISTORY: [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M",
    ],
}

# =============================================================================
# BUSINESS RULES
# =============================================================================
# Products start with 5 digits (85123A, 15056BL) or are DCGS sets.
# POST, DOT, M, D, C2, BANK CHARGES, AMAZONFEE, GIFT_0001... are not products.
PRODUCT_CODE_PATTERN = r"^(\d{5}[A-Z0-9]{0,4}|DCGS[A-Z0-9]+)$"

ANONYMOUS_CUSTOMER_ID = -1
ANONYMOUS_CUSTOMER_CODE = "ANONYMOUS"
UNCATEGORIZED = "Uncategorized"
EMPTY_DESCRIPTION = "NO DESCRIPTION"
UNKNOWN_COUNTRY = "Unknown"

# Rejection reasons (stored in audit.rejected_records)
REASON_NULL_FIELD = "required_field_null"
REASON_INVALID_DATE = "invalid_date"
REASON_NOT_A_PRODUCT = "not_a_product_code"
REASON_NON_POSITIVE_PRICE = "non_positive_price"
REASON_NEGATIVE_RETURN_PRICE = "negative_price_on_return"
REASON_DUPLICATE_IN_SOURCE = "duplicate_in_source"  # same natural key repeated

# Return types (quantity <= 0)
RETURN_CANCELLATION = "cancellation"  # invoice with C prefix
RETURN_ADJUSTMENT = "adjustment"  # negative quantity without C invoice (damage, stock)
