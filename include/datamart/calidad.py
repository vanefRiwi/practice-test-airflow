"""Calidad: separación ventas/devoluciones y contrato de datos antes de cargar."""

import numpy as np

from datamart import configuracion as cfg
from datamart.utilidades import formato_numero, sub, titulo

COLUMNAS_VENTAS = {
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
COLUMNAS_DEVOLUCIONES = {
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


def separar_ventas_devoluciones(lineas, moneda):
    """Cantidad > 0 es venta; cantidad <= 0 es devolución o ajuste (regla de negocio)."""
    titulo("SEPARACIÓN VENTAS / DEVOLUCIONES")
    es_venta = lineas["quantity"] > 0

    ventas = lineas[es_venta].copy()
    ventas["gross_revenue"] = (ventas["quantity"] * ventas["unit_price"]).round(2)
    ventas["currency"] = moneda

    devoluciones = lineas[~es_venta].copy()
    devoluciones["quantity_returned"] = devoluciones["quantity"].abs()
    devoluciones["return_amount"] = (
        devoluciones["quantity_returned"] * devoluciones["unit_price"]
    ).round(2)
    devoluciones["return_type"] = np.where(
        devoluciones["invoice_no"].str.startswith("C"),
        cfg.DEVOLUCION_CANCELACION,
        cfg.DEVOLUCION_AJUSTE,
    )
    devoluciones["currency"] = moneda
    devoluciones = devoluciones.drop(columns=["quantity"])

    sub(
        f"Ventas: {formato_numero(len(ventas))} | Devoluciones/ajustes: "
        f"{formato_numero(len(devoluciones))}"
    )
    sub(f"Tipos de devolución: {devoluciones['return_type'].value_counts().to_dict()}")
    return ventas.reset_index(drop=True), devoluciones.reset_index(drop=True)


def validar_contrato(ventas, devoluciones, productos, rechazos, metricas, max_ratio_rechazos):
    """Valida columnas, llaves y reglas antes de escribir; lanza ValueError si algo no cuadra."""
    titulo("CONTRATO DE DATOS")

    # 1. Columnas esperadas exactas
    for nombre, df, esperadas in [
        ("ventas", ventas, COLUMNAS_VENTAS),
        ("devoluciones", devoluciones, COLUMNAS_DEVOLUCIONES),
    ]:
        if set(df.columns) != esperadas:
            raise ValueError(f"Columnas inesperadas en {nombre}: {sorted(df.columns)}")

    # 2. Llaves únicas
    for nombre, df, llave in [
        ("ventas", ventas, "row_hash"),
        ("devoluciones", devoluciones, "row_hash"),
        ("productos", productos, "stock_code"),
    ]:
        if df[llave].duplicated().any():
            raise ValueError(f"Hay valores duplicados en {llave} de {nombre}.")

    # 3. Campos obligatorios sin nulos (customer_code puede ser nulo: cliente anónimo)
    obligatorias = ["row_hash", "invoice_no", "invoice_ts_utc", "stock_code", "country_name"]
    for nombre, df in [("ventas", ventas), ("devoluciones", devoluciones)]:
        for columna in obligatorias:
            if df[columna].isna().any():
                raise ValueError(f"Error crítico: {nombre} tiene {columna} nulo.")

    # 4. Reglas de negocio
    if (ventas["quantity"] <= 0).any() or (ventas["unit_price"] <= 0).any():
        raise ValueError("Hay ventas con cantidad o precio no positivo.")
    if (devoluciones["quantity_returned"] < 0).any() or (devoluciones["unit_price"] < 0).any():
        raise ValueError("Hay devoluciones con cantidad o precio negativo.")
    if not set(ventas["stock_code"]).union(devoluciones["stock_code"]) <= set(
        productos["stock_code"]
    ):
        raise ValueError("Hay líneas con productos que no están en la dimensión.")

    # 5. Cuadre de filas: leídas = válidas + rechazadas + duplicadas entre fuentes
    leidas = metricas["filas_leidas_ventas"] + metricas["filas_leidas_historico"]
    explicadas = len(ventas) + len(devoluciones) + len(rechazos)
    explicadas += metricas["duplicados_entre_fuentes"]
    if leidas != explicadas:
        raise ValueError(f"No cuadran las filas: leídas {leidas} vs explicadas {explicadas}")

    # 6. Umbral de calidad configurable con una Airflow Variable
    ratio = len(rechazos) / leidas if leidas else 0
    sub(f"Ratio de rechazos: {ratio:.2%} (máximo permitido {max_ratio_rechazos:.2%})")
    if ratio > max_ratio_rechazos:
        raise ValueError(
            f"Rechazos {ratio:.2%} superan el umbral {max_ratio_rechazos:.2%}: revisar fuentes."
        )

    sub("Contrato de datos OK")
    return ratio
