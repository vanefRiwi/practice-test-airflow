"""Reading the source CSVs and the seed files."""

from pathlib import Path

import pandas as pd

from datamart import config as cfg
from datamart.log_utils import fmt_int, section, step


def read_csv_any_encoding(path):
    """Reads the CSV as text. The Kaggle data.csv comes in ISO-8859-1."""
    for encoding in ("utf-8", "ISO-8859-1"):
        try:
            # everything as text, types are set in the transform step
            return pd.read_csv(path, dtype=str, encoding=encoding), encoding
        except UnicodeDecodeError:
            step(f"{Path(path).name} is not {encoding}, retrying with another encoding")
    raise ValueError(f"Could not decode {path}")


def standardize_columns(df, source):
    """Renames the source columns to the common schema and drops the rest."""
    renames = {}
    for column in df.columns:
        key = column.strip().lower()
        if key in cfg.COLUMN_ALIASES:
            renames[column] = cfg.COLUMN_ALIASES[key]

    df = df.rename(columns=renames)
    missing = [c for c in cfg.STANDARD_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Source {source} is missing columns {missing}")

    df = df[cfg.STANDARD_COLUMNS].copy()
    # row number in the original CSV, so rejections can be traced back
    df.insert(0, "source_row", range(1, len(df) + 1))
    df.insert(0, "source_system", source)
    return df


def extract_source(csv_path, source, output_path):
    """Reads one source, applies the common column names and saves it as parquet."""
    section(f"EXTRACT - {source}")
    df, encoding = read_csv_any_encoding(csv_path)
    step(f"File: {csv_path} | encoding: {encoding}")
    step(f"Original columns: {df.columns.tolist()}")

    df = standardize_columns(df, source)
    step(f"Rows read: {fmt_int(len(df))}")
    step(f"Nulls per column: {df.isna().sum().to_dict()}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return {"path": str(output_path), "rows": len(df), "source": source}


def read_category_rules(path=cfg.CATEGORY_RULES_FILE):
    """Keyword -> category rules."""
    rules = pd.read_csv(path, dtype={"priority": int, "keyword": str, "category": str})
    rules["keyword"] = rules["keyword"].str.strip().str.upper()
    if rules.duplicated(subset=["keyword"]).any():
        raise ValueError("category_rules.csv has repeated keywords")
    return rules.sort_values(["priority", "keyword"]).reset_index(drop=True)


def read_country_mapping(path=cfg.COUNTRY_MAPPING_FILE):
    """Country name mapping (EIRE -> Ireland, RSA -> South Africa)."""
    mapping = pd.read_csv(path, dtype=str)
    return dict(
        zip(mapping["source_name"].str.strip(), mapping["country_name"].str.strip(), strict=True)
    )
