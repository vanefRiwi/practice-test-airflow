"""Transform rule tests."""

import pandas as pd

from datamart import config as cfg
from datamart import transform as tr


def test_normalize_code_uppercase_no_spaces():
    series = pd.Series([" 85123a ", "bank charges", "22 423"])
    assert tr.normalize_code(series).tolist() == ["85123A", "BANKCHARGES", "22423"]


def test_normalize_customer_drops_decimal():
    series = pd.Series(["17850.0", "17850", None])
    result = tr.normalize_customer(series)
    assert result.iloc[0] == "17850" and result.iloc[1] == "17850"
    assert pd.isna(result.iloc[2])


def test_utc_dates_respect_daylight_saving():
    local = tr.parse_dates(pd.Series(["12/1/2010 8:26", "6/1/2011 10:00"]), ["%m/%d/%Y %H:%M"])
    utc = tr.to_utc(local, "Europe/London")
    # Winter: London = UTC. Summer (BST): London = UTC+1
    assert utc.iloc[0].strftime("%H:%M") == "08:26"
    assert utc.iloc[1].strftime("%H:%M") == "09:00"
    assert str(utc.dt.tz) == "UTC"


def test_rejection_reasons(sales_raw, country_mapping):
    df = tr.normalize_source(sales_raw, "Europe/London", country_mapping)
    reasons = df.set_index("source_row")["rejection_reason"]
    assert reasons[2] == cfg.REASON_DUPLICATE_IN_SOURCE  # same line written differently
    assert reasons[5] == cfg.REASON_NOT_A_PRODUCT  # POST
    assert reasons[6] == cfg.REASON_NON_POSITIVE_PRICE  # sale with price 0
    assert pd.isna(reasons[4])  # the return (quantity -1) is valid
    assert df.loc[df["source_row"] == 7, "country_name"].item() == "Ireland"


def test_cross_source_duplicates_daily_sales_wins(sales_raw, history_raw, rules, country_mapping):
    result = tr.transform_and_merge(sales_raw, history_raw, rules, country_mapping, "Europe/London")
    lines = result["lines"]
    assert result["metrics"]["cross_source_duplicates"] == 1
    # Line 536365/85123A x6 is kept only once, from the priority source
    repeated = lines[(lines["invoice_no"] == "536365") & (lines["quantity"] == 6)]
    assert repeated["source_system"].tolist() == [cfg.SOURCE_SALES]
    # The line that only exists in the history file is kept
    assert (lines["source_system"] == cfg.SOURCE_HISTORY).sum() == 1


def test_canonical_description_and_category(sales_raw, history_raw, rules, country_mapping):
    products = tr.transform_and_merge(
        sales_raw, history_raw, rules, country_mapping, "Europe/London"
    )["products"].set_index("stock_code")
    assert products.loc["85123A", "product_description"] == "WHITE HANGING HEART T-LIGHT HOLDER"
    assert products.loc["85123A", "description_variants"] == 2
    # T-LIGHT (priority 0) wins over LIGHT (priority 1)
    assert products.loc["85123A", "category"] == "Home"
    assert products.loc["21232", "category"] == cfg.UNCATEGORIZED


def test_row_hash_is_deterministic(sales_raw, country_mapping):
    first = tr.normalize_source(sales_raw, "Europe/London", country_mapping)["row_hash"]
    second = tr.normalize_source(sales_raw, "Europe/London", country_mapping)["row_hash"]
    assert first.equals(second)
