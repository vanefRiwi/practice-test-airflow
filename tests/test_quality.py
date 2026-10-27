"""Quality step tests."""

import pytest

from datamart import config as cfg
from datamart import quality
from datamart import transform as tr


@pytest.fixture
def result(sales_raw, history_raw, rules, country_mapping):
    return tr.transform_and_merge(sales_raw, history_raw, rules, country_mapping, "Europe/London")


def test_split_sales_and_returns(result):
    sales, returns = quality.split_sales_returns(result["lines"], "GBP")
    assert (sales["quantity"] > 0).all()
    assert returns["quantity_returned"].tolist() == [1]
    assert returns["return_type"].tolist() == [cfg.RETURN_CANCELLATION]
    assert returns["return_amount"].tolist() == [2.55]
    row = sales[sales["stock_code"] == "22423"]
    assert row["gross_revenue"].tolist() == [51.0]


def test_contract_ok(result):
    sales, returns = quality.split_sales_returns(result["lines"], "GBP")
    ratio = quality.check_contract(
        sales,
        returns,
        result["products"],
        result["rejections"],
        result["metrics"],
        max_reject_ratio=0.9,
    )
    assert 0 < ratio < 0.9


def test_contract_fails_over_threshold(result):
    sales, returns = quality.split_sales_returns(result["lines"], "GBP")
    with pytest.raises(ValueError, match="exceed the threshold"):
        quality.check_contract(
            sales,
            returns,
            result["products"],
            result["rejections"],
            result["metrics"],
            max_reject_ratio=0.01,
        )


def test_contract_fails_with_zero_price(result):
    sales, returns = quality.split_sales_returns(result["lines"], "GBP")
    sales.loc[0, "unit_price"] = 0
    with pytest.raises(ValueError, match="non-positive quantity or price"):
        quality.check_contract(
            sales,
            returns,
            result["products"],
            result["rejections"],
            result["metrics"],
            max_reject_ratio=0.9,
        )
