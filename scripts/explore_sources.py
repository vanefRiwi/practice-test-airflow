"""Profiles the two source CSVs: structure, nulls, ranges, duplicates and issues.

Usage:  uv run python scripts/explore_sources.py
The findings are summarized in docs/exploracion_datos.md.
"""

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))

from datamart import config as cfg  # noqa: E402
from datamart.extract import read_csv_any_encoding, standardize_columns  # noqa: E402

SOURCES = {
    cfg.SOURCE_SALES: ROOT / "data" / "raw" / "data.csv",
    cfg.SOURCE_HISTORY: ROOT / "data" / "raw" / "online_retail.csv",
}


def section(text):
    print("\n" + "=" * 78 + f"\n{text.upper()}\n" + "=" * 78)


def step(text):
    print(f"\n--- {text}")


def explore(name, path):
    """Prints the profile of one source."""
    section(f"Source: {name} ({path.name})")
    raw, encoding = read_csv_any_encoding(path)
    print(f"Encoding: {encoding} | Rows: {len(raw):,} | Columns: {raw.columns.tolist()}")
    print(raw.head(3).to_string())

    df = standardize_columns(raw, name)
    quantity = pd.to_numeric(df["quantity"], errors="coerce")
    price = pd.to_numeric(df["unit_price"], errors="coerce")
    code = df["stock_code"].str.upper().str.replace(r"\s+", "", regex=True)
    dates = pd.to_datetime(df["invoice_date_raw"], format="mixed", errors="coerce")

    step("Nulls per column")
    print(df.isna().sum().to_string())

    step("Ranges")
    print(f"Dates: {dates.min()} -> {dates.max()}")
    print(f"Quantity: min {quantity.min()} | max {quantity.max()}")
    print(f"Price: min {price.min()} | max {price.max()}")

    step("Quality issues")
    print(f"Quantity <= 0 (returns/adjustments): {(quantity <= 0).sum():,}")
    print(f"Invoices starting with C: {df['invoice_no'].str.startswith('C').sum():,}")
    print(
        f"Price <= 0: {(price <= 0).sum():,} | Sale with price <= 0: "
        f"{((quantity > 0) & (price <= 0)).sum():,}"
    )
    print(
        f"No CustomerID: {df['customer_id'].isna().sum():,} ({df['customer_id'].isna().mean():.2%})"
    )
    not_product = ~code.str.match(cfg.PRODUCT_CODE_PATTERN)
    print(f"Codes that are not products: {not_product.sum():,}")
    print(code[not_product].value_counts().head(15).to_string())
    print(f"Exact duplicate rows: {df.drop(columns=['source_row']).duplicated().sum():,}")
    print(f"Unique codes: {code.nunique():,}")
    variants = (
        df.assign(c=code).dropna(subset=["description"]).groupby("c")["description"].nunique()
    )
    print(f"Codes with more than one description: {(variants > 1).sum():,}")

    step("Top 10 countries")
    print(df["country"].value_counts().head(10).to_string())
    return df


if __name__ == "__main__":
    profiles = {name: explore(name, path) for name, path in SOURCES.items()}

    section("Overlap between sources")
    keys = ["invoice_no", "stock_code", "quantity", "invoice_date_raw"]
    a = profiles[cfg.SOURCE_SALES][keys].drop_duplicates()
    b = profiles[cfg.SOURCE_HISTORY][keys].drop_duplicates()
    common = a.merge(b, on=keys)
    print(f"Unique combos in sales: {len(a):,} | in history: {len(b):,} | common: {len(common):,}")
