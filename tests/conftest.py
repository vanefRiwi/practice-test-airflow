"""Sample rows in the format of each CSV."""

import pandas as pd
import pytest

from datamart import config as cfg
from datamart.extract import standardize_columns


@pytest.fixture
def sales_raw():
    """data.csv format."""
    df = pd.DataFrame(
        {
            "InvoiceNo": ["536365", "536365", "536365", "C536379", "536380", "536381", "536382"],
            "StockCode": ["85123A", "85123a ", "85123A", "85123A", "POST", "22423", "22423"],
            "Description": [
                "WHITE HANGING HEART T-LIGHT HOLDER",
                "White Hanging Heart T-Light Holder",
                "WHITE HANGING HEART T-LIGHT HOLDER",
                "WHITE HANGING HEART T-LIGHT HOLDER",
                "POSTAGE",
                "REGENCY CAKESTAND 3 TIER",
                "REGENCY CAKESTAND 3 TIER",
            ],
            "Quantity": ["6", "6", "2", "-1", "1", "1", "4"],
            "InvoiceDate": [
                "12/1/2010 8:26",
                "12/1/2010 8:26",
                "12/1/2010 8:26",
                "12/1/2010 9:41",
                "12/1/2010 9:45",
                "6/1/2011 10:00",
                "6/1/2011 11:00",
            ],
            "UnitPrice": ["2.55", "2.55", "2.55", "2.55", "18", "0", "12.75"],
            "CustomerID": ["17850", "17850", "17850", None, "12583", "12583", None],
            "Country": ["United Kingdom"] * 5 + ["EIRE", "EIRE"],
        },
        dtype="string",
    ).astype(object)
    return standardize_columns(df, cfg.SOURCE_SALES)


@pytest.fixture
def history_raw():
    """online_retail.csv format (has an index column and CustomerID with .0)."""
    df = pd.DataFrame(
        {
            "index": ["0", "1"],
            "InvoiceNo": ["536365", "489434"],
            "StockCode": ["85123A", "21232"],
            "Description": ["WHITE HANGING HEART T-LIGHT HOLDER", "STRAWBERRY CERAMIC TRINKET BOX"],
            "Quantity": ["6", "24"],
            "InvoiceDate": ["12/1/2010 8:26", "12/1/2009 7:45"],
            "UnitPrice": ["2.55", "1.25"],
            "CustomerID": ["17850.0", "13085.0"],
            "Country": ["United Kingdom", "United Kingdom"],
        }
    )
    return standardize_columns(df, cfg.SOURCE_HISTORY)


@pytest.fixture
def rules():
    return pd.DataFrame(
        {
            "priority": [0, 1, 5],
            "keyword": ["T-LIGHT", "LIGHT", "CAKESTAND"],
            "category": ["Home", "Electronics", "Home"],
        }
    )


@pytest.fixture
def country_mapping():
    return {"EIRE": "Ireland"}
