"""Exploración de las dos fuentes CSV: estructura, nulos, rangos, duplicados y problemas.

Uso:  uv run python scripts/explorar_fuentes.py
Los hallazgos están resumidos en docs/exploracion_datos.md.
"""

import sys
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "include"))

from datamart import configuracion as cfg  # noqa: E402
from datamart.extraccion import _leer_csv_con_encoding, estandarizar_columnas  # noqa: E402

FUENTES = {
    cfg.FUENTE_VENTAS: RAIZ / "data" / "raw" / "data.csv.gz",
    cfg.FUENTE_HISTORICO: RAIZ / "data" / "raw" / "online_retail.csv.gz",
}


def titulo(texto):
    print("\n" + "=" * 78 + f"\n{texto.upper()}\n" + "=" * 78)


def sub(texto):
    print(f"\n--- {texto}")


def explorar(nombre, ruta):
    """Imprime el perfil de una fuente."""
    titulo(f"Fuente: {nombre} ({ruta.name})")
    crudo, encoding = _leer_csv_con_encoding(ruta)
    print(f"Encoding: {encoding} | Filas: {len(crudo):,} | Columnas: {crudo.columns.tolist()}")
    print(crudo.head(3).to_string())

    df = estandarizar_columnas(crudo, nombre)
    cantidad = pd.to_numeric(df["quantity"], errors="coerce")
    precio = pd.to_numeric(df["unit_price"], errors="coerce")
    codigo = df["stock_code"].str.upper().str.replace(r"\s+", "", regex=True)
    fechas = pd.to_datetime(df["invoice_date_raw"], format="mixed", errors="coerce")

    sub("Nulos por columna")
    print(df.isna().sum().to_string())

    sub("Rangos")
    print(f"Fechas: {fechas.min()} -> {fechas.max()}")
    print(f"Cantidad: min {cantidad.min()} | max {cantidad.max()}")
    print(f"Precio: min {precio.min()} | max {precio.max()}")

    sub("Problemas de calidad")
    print(f"Cantidad <= 0 (devoluciones/ajustes): {(cantidad <= 0).sum():,}")
    print(f"Facturas que empiezan con C: {df['invoice_no'].str.startswith('C').sum():,}")
    print(
        f"Precio <= 0: {(precio <= 0).sum():,} | Venta con precio <= 0: "
        f"{((cantidad > 0) & (precio <= 0)).sum():,}"
    )
    print(
        f"Sin CustomerID: {df['customer_id'].isna().sum():,} "
        f"({df['customer_id'].isna().mean():.2%})"
    )
    no_producto = ~codigo.str.match(cfg.PATRON_CODIGO_PRODUCTO)
    print(f"Códigos que no son producto: {no_producto.sum():,}")
    print(codigo[no_producto].value_counts().head(15).to_string())
    print(f"Filas duplicadas exactas: {df.drop(columns=['source_row']).duplicated().sum():,}")
    print(f"Códigos únicos: {codigo.nunique():,}")
    variantes = (
        df.assign(c=codigo).dropna(subset=["description"]).groupby("c")["description"].nunique()
    )
    print(f"Códigos con más de una descripción: {(variantes > 1).sum():,}")

    sub("Top 10 países")
    print(df["country"].value_counts().head(10).to_string())
    return df


if __name__ == "__main__":
    perfiles = {nombre: explorar(nombre, ruta) for nombre, ruta in FUENTES.items()}

    titulo("Solapamiento entre fuentes")
    claves = ["invoice_no", "stock_code", "quantity", "invoice_date_raw"]
    a = perfiles[cfg.FUENTE_VENTAS][claves].drop_duplicates()
    b = perfiles[cfg.FUENTE_HISTORICO][claves].drop_duplicates()
    comunes = a.merge(b, on=claves)
    print(
        f"Combinaciones únicas en ventas: {len(a):,} | en histórico: {len(b):,} | "
        f"comunes: {len(comunes):,}"
    )
