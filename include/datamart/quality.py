"""Sales/returns split and checks before loading."""

import numpy as np

from datamart import config as cfg
from datamart.log_utils import fmt_int, section, step

SALES_COLUMNS = {
    "row_hash",
    "source_system",
    "invoice_no",
    "invoice_ts_utc",
    "stock_code",
    "customer_code",
    "country_name",
    "quantity",
    "unit_price",
    "gross_revenue",
    "currency",
}
RETURNS_COLUMNS = {
    "row_hash",
    "source_system",
    "invoice_no",
    "return_type",
    "invoice_ts_utc",
    "stock_code",
    "customer_code",
    "country_name",
    "quantity_returned",
    "unit_price",
    "return_amount",
    "currency",
}


def split_sales_returns(lines, currency):
    """quantity > 0 is a sale, quantity <= 0 is a return or adjustment."""
    section("SALES / RETURNS SPLIT")
    is_sale = lines["quantity"] > 0

    sales = lines[is_sale].copy()
    sales["gross_revenue"] = (sales["quantity"] * sales["unit_price"]).round(2)
    sales["currency"] = currency

    returns = lines[~is_sale].copy()
    returns["quantity_returned"] = returns["quantity"].abs()
    returns["return_amount"] = (returns["quantity_returned"] * returns["unit_price"]).round(2)
    returns["return_type"] = np.where(
        returns["invoice_no"].str.startswith("C"),
        cfg.RETURN_CANCELLATION,
        cfg.RETURN_ADJUSTMENT,
    )
    returns["currency"] = currency
    returns = returns.drop(columns=["quantity"])

    step(f"Sales: {fmt_int(len(sales))} | Returns/adjustments: {fmt_int(len(returns))}")
    step(f"Return types: {returns['return_type'].value_counts().to_dict()}")
    return sales.reset_index(drop=True), returns.reset_index(drop=True)


def check_contract(sales, returns, products, rejections, metrics, max_reject_ratio):
    """Data contract checks before writing anything."""
    section("DATA CONTRACT")

    # 1. Exact expected columns
    for name, df, expected in [
        ("sales", sales, SALES_COLUMNS),
        ("returns", returns, RETURNS_COLUMNS),
    ]:
        if set(df.columns) != expected:
            raise ValueError(f"Unexpected columns in {name}: {sorted(df.columns)}")

    # 2. Unique keys
    for name, df, key in [
        ("sales", sales, "row_hash"),
        ("returns", returns, "row_hash"),
        ("products", products, "stock_code"),
    ]:
        if df[key].duplicated().any():
            raise ValueError(f"Duplicated {key} values in {name}.")

    # 3. Nulls (customer_code can be null, that's the anonymous customer)
    required = ["row_hash", "invoice_no", "invoice_ts_utc", "stock_code", "country_name"]
    for name, df in [("sales", sales), ("returns", returns)]:
        for column in required:
            if df[column].isna().any():
                raise ValueError(f"{name} has null {column}.")

    # 4. Business rules
    if (sales["quantity"] <= 0).any() or (sales["unit_price"] <= 0).any():
        raise ValueError("There are sales with non-positive quantity or price.")
    if (returns["quantity_returned"] < 0).any() or (returns["unit_price"] < 0).any():
        raise ValueError("There are returns with negative quantity or price.")
    if not set(sales["stock_code"]).union(returns["stock_code"]) <= set(products["stock_code"]):
        raise ValueError("There are lines with products missing from the dimension.")

    # 5. read = valid + rejected + cross-source duplicates
    read = metrics["rows_read_sales"] + metrics["rows_read_history"]
    explained = len(sales) + len(returns) + len(rejections)
    explained += metrics["cross_source_duplicates"]
    if read != explained:
        raise ValueError(f"Row counts don't match: read {read} vs explained {explained}")

    # 6. Rejection threshold (Variable datamart_max_reject_ratio)
    ratio = len(rejections) / read if read else 0
    step(f"Reject ratio: {ratio:.2%} (max allowed {max_reject_ratio:.2%})")
    if ratio > max_reject_ratio:
        raise ValueError(
            f"Rejections {ratio:.2%} exceed the threshold {max_reject_ratio:.2%}: check sources."
        )

    step("Data contract OK")
    return ratio
