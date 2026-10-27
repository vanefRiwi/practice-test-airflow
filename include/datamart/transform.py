"""Cleaning and merging of the two sources."""

import hashlib
import re

import numpy as np
import pandas as pd

from datamart import config as cfg
from datamart.log_utils import fmt_int, section, step

# =============================================================================
# 1. FIELD NORMALIZATION
# =============================================================================


def normalize_code(series):
    """Uppercase, no spaces."""
    return series.str.upper().str.replace(r"\s+", "", regex=True).replace("", pd.NA)


def normalize_text(series):
    """Trims extra spaces without changing case."""
    return series.str.strip().str.replace(r"\s+", " ", regex=True).replace("", pd.NA)


def normalize_customer(series):
    """17850.0 -> '17850'."""
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64").astype("string")


def parse_dates(series, formats):
    """Tries the formats in order; anything that can't be parsed stays NaT."""
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    for fmt in formats:
        pending = result.isna() & series.notna()
        if not pending.any():
            break
        result[pending] = pd.to_datetime(series[pending].str.strip(), format=fmt, errors="coerce")
    return result


def to_utc(local_dates, timezone):
    """London time -> UTC."""
    # DST change: in October take winter time, in March shift forward
    ambiguous = np.zeros(len(local_dates), dtype=bool)
    return local_dates.dt.tz_localize(
        timezone, ambiguous=ambiguous, nonexistent="shift_forward"
    ).dt.tz_convert("UTC")


def compute_row_hash(df):
    """MD5 of invoice, code, quantity, price, date and customer."""
    key = (
        df["invoice_no"].astype(str)
        + "|"
        + df["stock_code"].astype(str)
        + "|"
        + df["quantity"].astype(str)
        + "|"
        + df["unit_price"].map("{:.3f}".format)
        + "|"
        + df["invoice_ts_utc"].dt.floor("min").dt.strftime("%Y-%m-%dT%H:%M")
        + "|"
        + df["customer_code"].fillna("").astype(str)
    )
    return [hashlib.md5(value.encode("utf-8")).hexdigest() for value in key]


# =============================================================================
# 2. PER-SOURCE CLEANING (each row ends up valid or with a rejection reason)
# =============================================================================


def normalize_source(df, timezone, country_mapping):
    """Cleans one source and tags each row with its rejection reason."""
    source = df["source_system"].iloc[0] if len(df) else "unknown"
    step(f"Normalizing {source}: {fmt_int(len(df))} rows")
    out = df.copy()

    out["invoice_no"] = normalize_code(out["invoice_no"])
    out["stock_code"] = normalize_code(out["stock_code"])
    out["description"] = normalize_text(out["description"])
    out["description_norm"] = out["description"].str.upper()
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce").astype("Int64")
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="coerce").round(3)
    out["customer_code"] = normalize_customer(out["customer_id"])

    country = normalize_text(out["country"])
    out["country_name"] = country.map(lambda c: country_mapping.get(c, c) if pd.notna(c) else c)
    out["country_name"] = out["country_name"].fillna(cfg.UNKNOWN_COUNTRY)

    local_dates = parse_dates(out["invoice_date_raw"], cfg.DATE_FORMATS[source])
    out["invoice_ts_utc"] = to_utc(local_dates, timezone)

    # If a row has several problems it keeps the first reason
    reason = pd.Series(pd.NA, index=out.index, dtype="object")

    # 1. Required fields
    nulls = (
        out["invoice_no"].isna()
        | out["stock_code"].isna()
        | out["quantity"].isna()
        | out["unit_price"].isna()
    )
    reason[nulls] = cfg.REASON_NULL_FIELD

    # 2. Date that couldn't be parsed
    reason[reason.isna() & out["invoice_ts_utc"].isna()] = cfg.REASON_INVALID_DATE

    # 3. Codes that are not products
    is_product = out["stock_code"].str.match(cfg.PRODUCT_CODE_PATTERN).fillna(False)
    reason[reason.isna() & ~is_product] = cfg.REASON_NOT_A_PRODUCT

    # 4. A sale (quantity > 0) needs price > 0
    sale_without_price = (out["quantity"] > 0) & (out["unit_price"] <= 0)
    reason[reason.isna() & sale_without_price.fillna(False)] = cfg.REASON_NON_POSITIVE_PRICE

    # 5. Return with negative price (price 0 is fine, those are stock adjustments)
    negative_return = (out["quantity"] <= 0) & (out["unit_price"] < 0)
    reason[reason.isna() & negative_return.fillna(False)] = cfg.REASON_NEGATIVE_RETURN_PRICE

    out["rejection_reason"] = reason

    # 6. Duplicates inside the same file, keep the first one
    valid = out["rejection_reason"].isna()
    out["row_hash"] = pd.NA
    out.loc[valid, "row_hash"] = compute_row_hash(out.loc[valid])
    duplicate = valid & out["row_hash"].duplicated(keep="first") & out["row_hash"].notna()
    out.loc[duplicate, "rejection_reason"] = cfg.REASON_DUPLICATE_IN_SOURCE

    step(f"Rejection reasons in {source}: {out['rejection_reason'].value_counts().to_dict()}")
    return out


# =============================================================================
# 3. MERGING THE SOURCES
# =============================================================================


def resolve_cross_source_duplicates(sales, history):
    """If a line is in both sources, the data.csv one wins."""
    sales_hashes = set(sales["row_hash"])
    repeated = history["row_hash"].isin(sales_hashes)
    step(
        f"History lines already in daily sales: {fmt_int(repeated.sum())}"
        f" of {fmt_int(len(history))}"
    )
    merged = pd.concat([sales, history[~repeated]], ignore_index=True)
    return merged, int(repeated.sum())


# =============================================================================
# 4. PRODUCT DIMENSION: CANONICAL NAME AND CATEGORY
# =============================================================================


def build_products(lines, category_rules, source_descriptions):
    """One row per stock_code with its name, variants and category."""
    with_description = lines.dropna(subset=["description_norm"])

    # Name = the most used description; on a tie, the most recent one
    counts = (
        with_description.groupby(["stock_code", "description_norm"])
        .agg(times=("row_hash", "size"), last_seen=("invoice_ts_utc", "max"))
        .reset_index()
        .sort_values(
            ["stock_code", "times", "last_seen", "description_norm"],
            ascending=[True, False, False, True],
        )
    )
    canonical = counts.drop_duplicates("stock_code").set_index("stock_code")["description_norm"]

    # variants are counted over every source row, rejected ones included
    variants = (
        source_descriptions.dropna(subset=["description"])
        .groupby("stock_code")["description"]
        .nunique()
    )

    products = pd.DataFrame({"stock_code": sorted(lines["stock_code"].unique())})
    products["product_description"] = (
        products["stock_code"].map(canonical).fillna(cfg.EMPTY_DESCRIPTION)
    )
    products["description_variants"] = products["stock_code"].map(variants).fillna(0).astype(int)
    products["category"] = assign_category(products["product_description"], category_rules)
    return products


def assign_category(descriptions, rules):
    """Category by keyword, the rule with the lowest priority wins."""
    category = pd.Series(pd.NA, index=descriptions.index, dtype="object")
    for rule in rules.itertuples():
        # \b so CARD doesn't match CARDIGAN
        pattern = rf"\b{re.escape(rule.keyword)}\b"
        matches = descriptions.str.contains(pattern, regex=True, na=False)
        category[category.isna() & matches] = rule.category
    return category.fillna(cfg.UNCATEGORIZED)


# =============================================================================
# 5. TRANSFORM ENTRY POINT
# =============================================================================


def _rejections(df):
    """Rejected rows with the original record as JSON."""
    rejected = df[df["rejection_reason"].notna()]
    raw = rejected[cfg.STANDARD_COLUMNS].to_json(orient="records", lines=True, force_ascii=False)
    return pd.DataFrame(
        {
            "source_system": rejected["source_system"].to_numpy(),
            "source_row": rejected["source_row"].to_numpy(),
            "reason": rejected["rejection_reason"].to_numpy(),
            "raw_record": raw.splitlines() if len(rejected) else [],
        }
    )


def transform_and_merge(sales_raw, history_raw, category_rules, country_mapping, timezone):
    """Returns valid lines, rejections, products and metrics."""
    section("TRANSFORM AND MERGE")

    sales = normalize_source(sales_raw, timezone, country_mapping)
    history = normalize_source(history_raw, timezone, country_mapping)

    rejections = pd.concat([_rejections(sales), _rejections(history)], ignore_index=True)

    valid_sales = sales[sales["rejection_reason"].isna()]
    valid_history = history[history["rejection_reason"].isna()]
    lines, cross_source_duplicates = resolve_cross_source_duplicates(valid_sales, valid_history)

    source_descriptions = pd.concat(
        [
            df.loc[
                ~df["rejection_reason"].isin([cfg.REASON_NULL_FIELD, cfg.REASON_NOT_A_PRODUCT]),
                ["stock_code", "description"],
            ]
            for df in (sales, history)
        ],
        ignore_index=True,
    )
    products = build_products(lines, category_rules, source_descriptions)
    step(f"Unique products: {fmt_int(len(products))}")
    step(f"Categories: {products['category'].value_counts().to_dict()}")

    line_columns = [
        "row_hash",
        "source_system",
        "invoice_no",
        "invoice_ts_utc",
        "stock_code",
        "customer_code",
        "country_name",
        "quantity",
        "unit_price",
    ]
    lines = lines[line_columns].reset_index(drop=True)

    # int() because this goes through XCom and numpy types don't serialize
    metrics = {
        "rows_read_sales": len(sales_raw),
        "rows_read_history": len(history_raw),
        "rows_valid": len(lines),
        "rows_rejected": len(rejections),
        "cross_source_duplicates": cross_source_duplicates,
        "rejections_by_reason": {k: int(v) for k, v in rejections["reason"].value_counts().items()},
    }
    step(f"Metrics: {metrics}")
    return {"lines": lines, "rejections": rejections, "products": products, "metrics": metrics}
