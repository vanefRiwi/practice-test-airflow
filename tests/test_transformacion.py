"""Tests de las reglas de transformación."""

import pandas as pd

from datamart import configuracion as cfg
from datamart import transformacion as tr


def test_normalizar_codigo_mayusculas_sin_espacios():
    serie = pd.Series([" 85123a ", "bank charges", "22 423"])
    assert tr.normalizar_codigo(serie).tolist() == ["85123A", "BANKCHARGES", "22423"]


def test_normalizar_cliente_quita_decimal():
    serie = pd.Series(["17850.0", "17850", None])
    resultado = tr.normalizar_cliente(serie)
    assert resultado.iloc[0] == "17850" and resultado.iloc[1] == "17850"
    assert pd.isna(resultado.iloc[2])


def test_fechas_a_utc_respetan_horario_de_verano():
    locales = tr.parsear_fechas(pd.Series(["12/1/2010 8:26", "6/1/2011 10:00"]), ["%m/%d/%Y %H:%M"])
    utc = tr.convertir_a_utc(locales, "Europe/London")
    # Invierno: Londres = UTC. Verano (BST): Londres = UTC+1
    assert utc.iloc[0].strftime("%H:%M") == "08:26"
    assert utc.iloc[1].strftime("%H:%M") == "09:00"
    assert str(utc.dt.tz) == "UTC"


def test_motivos_de_rechazo(ventas_raw, mapeo_paises):
    df = tr.normalizar_fuente(ventas_raw, "Europe/London", mapeo_paises)
    motivos = df.set_index("source_row")["rejection_reason"]
    assert motivos[2] == cfg.MOTIVO_DUPLICADO_EN_FUENTE  # misma línea con otro formato
    assert motivos[5] == cfg.MOTIVO_CODIGO_NO_PRODUCTO  # POST
    assert motivos[6] == cfg.MOTIVO_PRECIO_NO_POSITIVO  # venta con precio 0
    assert pd.isna(motivos[4])  # la devolución (cantidad -1) es válida
    assert df.loc[df["source_row"] == 7, "country_name"].item() == "Ireland"


def test_duplicados_entre_fuentes_gana_ventas_diarias(
    ventas_raw, historico_raw, reglas, mapeo_paises
):
    resultado = tr.transformar_y_unificar(
        ventas_raw, historico_raw, reglas, mapeo_paises, "Europe/London"
    )
    lineas = resultado["lineas"]
    assert resultado["metricas"]["duplicados_entre_fuentes"] == 1
    # La línea 536365/85123A x6 solo queda una vez, con la fuente prioritaria
    repetida = lineas[(lineas["invoice_no"] == "536365") & (lineas["quantity"] == 6)]
    assert repetida["source_system"].tolist() == [cfg.FUENTE_VENTAS]
    # La línea que solo existe en el histórico se conserva
    assert (lineas["source_system"] == cfg.FUENTE_HISTORICO).sum() == 1


def test_descripcion_canonica_y_categoria(ventas_raw, historico_raw, reglas, mapeo_paises):
    productos = tr.transformar_y_unificar(
        ventas_raw, historico_raw, reglas, mapeo_paises, "Europe/London"
    )["productos"].set_index("stock_code")
    assert productos.loc["85123A", "product_description"] == "WHITE HANGING HEART T-LIGHT HOLDER"
    assert productos.loc["85123A", "description_variants"] == 2
    # T-LIGHT (prioridad 0) gana sobre LIGHT (prioridad 1)
    assert productos.loc["85123A", "category"] == "Hogar"
    assert productos.loc["21232", "category"] == cfg.CATEGORIA_SIN_CLASIFICAR


def test_row_hash_es_deterministico(ventas_raw, mapeo_paises):
    primera = tr.normalizar_fuente(ventas_raw, "Europe/London", mapeo_paises)["row_hash"]
    segunda = tr.normalizar_fuente(ventas_raw, "Europe/London", mapeo_paises)["row_hash"]
    assert primera.equals(segunda)
