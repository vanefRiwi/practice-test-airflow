"""Tests de calidad."""

import pytest

from datamart import calidad
from datamart import configuracion as cfg
from datamart import transformacion as tr


@pytest.fixture
def resultado(ventas_raw, historico_raw, reglas, mapeo_paises):
    return tr.transformar_y_unificar(
        ventas_raw, historico_raw, reglas, mapeo_paises, "Europe/London"
    )


def test_separar_ventas_y_devoluciones(resultado):
    ventas, devoluciones = calidad.separar_ventas_devoluciones(resultado["lineas"], "GBP")
    assert (ventas["quantity"] > 0).all()
    assert devoluciones["quantity_returned"].tolist() == [1]
    assert devoluciones["return_type"].tolist() == [cfg.DEVOLUCION_CANCELACION]
    assert devoluciones["return_amount"].tolist() == [2.55]
    fila = ventas[ventas["stock_code"] == "22423"]
    assert fila["gross_revenue"].tolist() == [51.0]


def test_contrato_ok(resultado):
    ventas, devoluciones = calidad.separar_ventas_devoluciones(resultado["lineas"], "GBP")
    ratio = calidad.validar_contrato(
        ventas,
        devoluciones,
        resultado["productos"],
        resultado["rechazos"],
        resultado["metricas"],
        max_ratio_rechazos=0.9,
    )
    assert 0 < ratio < 0.9


def test_contrato_falla_si_supera_umbral(resultado):
    ventas, devoluciones = calidad.separar_ventas_devoluciones(resultado["lineas"], "GBP")
    with pytest.raises(ValueError, match="superan el umbral"):
        calidad.validar_contrato(
            ventas,
            devoluciones,
            resultado["productos"],
            resultado["rechazos"],
            resultado["metricas"],
            max_ratio_rechazos=0.01,
        )


def test_contrato_falla_con_precio_cero(resultado):
    ventas, devoluciones = calidad.separar_ventas_devoluciones(resultado["lineas"], "GBP")
    ventas.loc[0, "unit_price"] = 0
    with pytest.raises(ValueError, match="precio no positivo"):
        calidad.validar_contrato(
            ventas,
            devoluciones,
            resultado["productos"],
            resultado["rechazos"],
            resultado["metricas"],
            max_ratio_rechazos=0.9,
        )
