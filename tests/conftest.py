"""Datos de prueba pequeños que imitan las dos fuentes de Kaggle."""

import pandas as pd
import pytest

from datamart import configuracion as cfg
from datamart.extraccion import estandarizar_columnas


@pytest.fixture
def ventas_raw():
    """Muestra con el formato de data.csv (fechas M/D/YYYY, CustomerID entero)."""
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
    return estandarizar_columnas(df, cfg.FUENTE_VENTAS)


@pytest.fixture
def historico_raw():
    """Muestra con el formato de online_retail.csv (columna index, CustomerID con .0)."""
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
    return estandarizar_columnas(df, cfg.FUENTE_HISTORICO)


@pytest.fixture
def reglas():
    return pd.DataFrame(
        {
            "priority": [0, 1, 5],
            "keyword": ["T-LIGHT", "LIGHT", "CAKESTAND"],
            "category": ["Hogar", "Electrónica", "Hogar"],
        }
    )


@pytest.fixture
def mapeo_paises():
    return {"EIRE": "Ireland"}
