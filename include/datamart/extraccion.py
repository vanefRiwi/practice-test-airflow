"""Lectura de los CSV y de los archivos de seeds."""

from pathlib import Path

import pandas as pd

from datamart import configuracion as cfg
from datamart.utilidades import formato_numero, sub, titulo


def _leer_csv_con_encoding(ruta):
    """Lee el CSV como texto. data.csv de Kaggle viene en ISO-8859-1."""
    for encoding in ("utf-8", "ISO-8859-1"):
        try:
            # todo como texto, los tipos se ponen en la transformación
            return pd.read_csv(ruta, dtype=str, encoding=encoding), encoding
        except UnicodeDecodeError:
            sub(f"{Path(ruta).name} no es {encoding}, se reintenta con otro encoding")
    raise ValueError(f"No se pudo decodificar {ruta}")


def estandarizar_columnas(df, fuente):
    """Renombra las columnas de la fuente al esquema común y descarta las que sobran."""
    renombres = {}
    for columna in df.columns:
        clave = columna.strip().lower()
        if clave in cfg.ALIAS_COLUMNAS:
            renombres[columna] = cfg.ALIAS_COLUMNAS[clave]

    df = df.rename(columns=renombres)
    faltantes = [c for c in cfg.COLUMNAS_ESTANDAR if c not in df.columns]
    if faltantes:
        raise ValueError(f"La fuente {fuente} no trae las columnas {faltantes}")

    df = df[cfg.COLUMNAS_ESTANDAR].copy()
    # número de fila en el CSV original, para poder rastrear los rechazos
    df.insert(0, "source_row", range(1, len(df) + 1))
    df.insert(0, "source_system", fuente)
    return df


def extraer_fuente(ruta_csv, fuente, ruta_salida):
    """Lee una fuente, le pone los nombres de columna comunes y la guarda en parquet."""
    titulo(f"EXTRACCIÓN - {fuente}")
    df, encoding = _leer_csv_con_encoding(ruta_csv)
    sub(f"Archivo: {ruta_csv} | encoding: {encoding}")
    sub(f"Columnas originales: {df.columns.tolist()}")

    df = estandarizar_columnas(df, fuente)
    sub(f"Filas leídas: {formato_numero(len(df))}")
    sub(f"Nulos por columna: {df.isna().sum().to_dict()}")

    Path(ruta_salida).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ruta_salida, index=False)
    return {"ruta": str(ruta_salida), "filas": len(df), "fuente": fuente}


def leer_reglas_categoria(ruta=cfg.ARCHIVO_REGLAS_CATEGORIA):
    """Reglas palabra clave -> categoría."""
    reglas = pd.read_csv(ruta, dtype={"priority": int, "keyword": str, "category": str})
    reglas["keyword"] = reglas["keyword"].str.strip().str.upper()
    if reglas.duplicated(subset=["keyword"]).any():
        raise ValueError("Hay palabras clave repetidas en category_rules.csv")
    return reglas.sort_values(["priority", "keyword"]).reset_index(drop=True)


def leer_mapeo_paises(ruta=cfg.ARCHIVO_MAPEO_PAISES):
    """Mapeo de nombres de país (EIRE -> Ireland, RSA -> South Africa)."""
    mapeo = pd.read_csv(ruta, dtype=str)
    return dict(
        zip(mapeo["source_name"].str.strip(), mapeo["country_name"].str.strip(), strict=True)
    )
