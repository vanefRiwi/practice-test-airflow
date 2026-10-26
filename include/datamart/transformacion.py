"""Transformación: limpieza, normalización, UTC, deduplicación y enriquecimiento de las fuentes."""

import hashlib
import re

import numpy as np
import pandas as pd

from datamart import configuracion as cfg
from datamart.utilidades import formato_numero, sub, titulo

# =============================================================================
# 1. NORMALIZACIÓN DE CAMPOS
# =============================================================================


def normalizar_codigo(serie):
    """Códigos de producto en mayúsculas y sin espacios (regla de negocio)."""
    return serie.str.upper().str.replace(r"\s+", "", regex=True).replace("", pd.NA)


def normalizar_texto(serie):
    """Quita espacios a los lados y colapsa espacios repetidos, conservando mayúsculas."""
    return serie.str.strip().str.replace(r"\s+", " ", regex=True).replace("", pd.NA)


def normalizar_cliente(serie):
    """CustomerID llega como '17850' o '17850.0': se deja como texto entero ('17850')."""
    numerico = pd.to_numeric(serie, errors="coerce")
    return numerico.astype("Int64").astype("string")


def parsear_fechas(serie, formatos):
    """Parsea con formatos explícitos en orden; lo que ningún formato entiende queda NaT."""
    resultado = pd.Series(pd.NaT, index=serie.index, dtype="datetime64[ns]")
    for formato in formatos:
        pendientes = resultado.isna() & serie.notna()
        if not pendientes.any():
            break
        resultado[pendientes] = pd.to_datetime(
            serie[pendientes].str.strip(), format=formato, errors="coerce"
        )
    return resultado


def convertir_a_utc(fechas_locales, zona_horaria):
    """Localiza la hora de la tienda (Europe/London) y la convierte a UTC.

    En el cambio de horario de octubre una hora se repite: se asume horario estándar
    (ambiguous=False). En marzo una hora no existe: se corre hacia adelante.
    """
    ambiguas = np.zeros(len(fechas_locales), dtype=bool)
    return fechas_locales.dt.tz_localize(
        zona_horaria, ambiguous=ambiguas, nonexistent="shift_forward"
    ).dt.tz_convert("UTC")


def calcular_row_hash(df):
    """Hash MD5 de la clave natural de una línea de factura (misma línea = mismo hash)."""
    clave = (
        df["invoice_no"].astype(str)
        + "|"
        + df["stock_code"].astype(str)
        + "|"
        + df["quantity"].astype(str)
        + "|"
        + df["unit_price"].map("{:.3f}".format)
        + "|"
        + df["invoice_ts_utc"].dt.floor("min").dt.strftime("%Y-%m-%dT%H:%M")
        + "|"
        + df["customer_code"].fillna("").astype(str)
    )
    return [hashlib.md5(valor.encode("utf-8")).hexdigest() for valor in clave]


# =============================================================================
# 2. LIMPIEZA POR FUENTE (cada fila queda válida o con su motivo de rechazo)
# =============================================================================


def normalizar_fuente(df, zona_horaria, mapeo_paises):
    """Tipa y normaliza una fuente estandarizada y marca el motivo de rechazo de cada fila."""
    fuente = df["source_system"].iloc[0] if len(df) else "desconocida"
    sub(f"Normalizando {fuente}: {formato_numero(len(df))} filas")
    out = df.copy()

    out["invoice_no"] = normalizar_codigo(out["invoice_no"])
    out["stock_code"] = normalizar_codigo(out["stock_code"])
    out["description"] = normalizar_texto(out["description"])
    out["description_norm"] = out["description"].str.upper()
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce").astype("Int64")
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="coerce").round(3)
    out["customer_code"] = normalizar_cliente(out["customer_id"])

    pais = normalizar_texto(out["country"])
    out["country_name"] = pais.map(lambda p: mapeo_paises.get(p, p) if pd.notna(p) else p)
    out["country_name"] = out["country_name"].fillna(cfg.PAIS_DESCONOCIDO)

    fechas_locales = parsear_fechas(out["invoice_date_raw"], cfg.FORMATOS_FECHA[fuente])
    out["invoice_ts_utc"] = convertir_a_utc(fechas_locales, zona_horaria)

    # Motivo de rechazo: se evalúa en orden y gana el primero que aplica.
    motivo = pd.Series(pd.NA, index=out.index, dtype="object")

    # 1. Campos obligatorios
    nulos = (
        out["invoice_no"].isna()
        | out["stock_code"].isna()
        | out["quantity"].isna()
        | out["unit_price"].isna()
    )
    motivo[nulos] = cfg.MOTIVO_CAMPO_NULO

    # 2. Fecha que no se pudo interpretar
    motivo[motivo.isna() & out["invoice_ts_utc"].isna()] = cfg.MOTIVO_FECHA_INVALIDA

    # 3. Códigos que no son producto (portes, comisiones, vales, ajustes manuales)
    es_producto = out["stock_code"].str.match(cfg.PATRON_CODIGO_PRODUCTO).fillna(False)
    motivo[motivo.isna() & ~es_producto] = cfg.MOTIVO_CODIGO_NO_PRODUCTO

    # 4. Una venta (cantidad > 0) necesita precio > 0
    venta_sin_precio = (out["quantity"] > 0) & (out["unit_price"] <= 0)
    motivo[motivo.isna() & venta_sin_precio.fillna(False)] = cfg.MOTIVO_PRECIO_NO_POSITIVO

    # 5. Una devolución puede valer 0 (ajuste de inventario) pero no tener precio negativo
    devolucion_negativa = (out["quantity"] <= 0) & (out["unit_price"] < 0)
    motivo[motivo.isna() & devolucion_negativa.fillna(False)] = (
        cfg.MOTIVO_PRECIO_NEGATIVO_DEVOLUCION
    )

    out["rejection_reason"] = motivo

    # 6. Duplicados dentro de la misma fuente: misma clave natural -> se conserva la primera
    validas = out["rejection_reason"].isna()
    out["row_hash"] = pd.NA
    out.loc[validas, "row_hash"] = calcular_row_hash(out.loc[validas])
    duplicada = validas & out["row_hash"].duplicated(keep="first") & out["row_hash"].notna()
    out.loc[duplicada, "rejection_reason"] = cfg.MOTIVO_DUPLICADO_EN_FUENTE

    sub(f"Motivos de rechazo en {fuente}: {out['rejection_reason'].value_counts().to_dict()}")
    return out


# =============================================================================
# 3. UNIFICACIÓN DE FUENTES
# =============================================================================


def resolver_duplicados_entre_fuentes(ventas, historico):
    """Une las fuentes válidas; si una línea está en ambas, gana el volcado diario."""
    hashes_ventas = set(ventas["row_hash"])
    repetida = historico["row_hash"].isin(hashes_ventas)
    sub(
        f"Líneas del histórico que ya están en ventas diarias: {formato_numero(repetida.sum())}"
        f" de {formato_numero(len(historico))}"
    )
    unificado = pd.concat([ventas, historico[~repetida]], ignore_index=True)
    return unificado, int(repetida.sum())


# =============================================================================
# 4. DIMENSIÓN PRODUCTO: NOMBRE CANÓNICO Y CATEGORÍA
# =============================================================================


def construir_productos(lineas, reglas_categoria, descripciones_fuente):
    """Un registro por stock_code con descripción canónica, variantes y categoría.

    El nombre canónico sale de las líneas válidas; las variantes se cuentan sobre todas
    las filas de las fuentes con código de producto (también las rechazadas), porque la
    pregunta de negocio es cómo llegan escritas las descripciones.
    """
    con_descripcion = lineas.dropna(subset=["description_norm"])

    # Nombre canónico = la descripción (en mayúsculas) más usada para el código;
    # en empate gana la más reciente y luego el orden alfabético (regla determinística).
    frecuencias = (
        con_descripcion.groupby(["stock_code", "description_norm"])
        .agg(veces=("row_hash", "size"), ultima_vez=("invoice_ts_utc", "max"))
        .reset_index()
        .sort_values(
            ["stock_code", "veces", "ultima_vez", "description_norm"],
            ascending=[True, False, False, True],
        )
    )
    canonica = frecuencias.drop_duplicates("stock_code").set_index("stock_code")["description_norm"]

    # Variantes = formas distintas de escribir la descripción tal como llegó (mayúsculas incluidas)
    variantes = (
        descripciones_fuente.dropna(subset=["description"])
        .groupby("stock_code")["description"]
        .nunique()
    )

    productos = pd.DataFrame({"stock_code": sorted(lineas["stock_code"].unique())})
    productos["product_description"] = (
        productos["stock_code"].map(canonica).fillna(cfg.DESCRIPCION_VACIA)
    )
    productos["description_variants"] = productos["stock_code"].map(variantes).fillna(0).astype(int)
    productos["category"] = asignar_categoria(productos["product_description"], reglas_categoria)
    return productos


def asignar_categoria(descripciones, reglas):
    """Asigna categoría por palabra clave; se aplica la regla de menor prioridad que coincida."""
    categoria = pd.Series(pd.NA, index=descripciones.index, dtype="object")
    for regla in reglas.itertuples():
        # \b = límite de palabra: "CARD" no coincide dentro de "CARDIGAN"
        patron = rf"\b{re.escape(regla.keyword)}\b"
        coincide = descripciones.str.contains(patron, regex=True, na=False)
        categoria[categoria.isna() & coincide] = regla.category
    return categoria.fillna(cfg.CATEGORIA_SIN_CLASIFICAR)


# =============================================================================
# 5. ORQUESTACIÓN DE LA TRANSFORMACIÓN
# =============================================================================

COLUMNAS_RAW = cfg.COLUMNAS_ESTANDAR


def _rechazos(df):
    """Arma el log de rechazos guardando el registro original como JSON."""
    rechazadas = df[df["rejection_reason"].notna()]
    raw = rechazadas[COLUMNAS_RAW].to_json(orient="records", lines=True, force_ascii=False)
    return pd.DataFrame(
        {
            "source_system": rechazadas["source_system"].to_numpy(),
            "source_row": rechazadas["source_row"].to_numpy(),
            "reason": rechazadas["rejection_reason"].to_numpy(),
            "raw_record": raw.splitlines() if len(rechazadas) else [],
        }
    )


def transformar_y_unificar(ventas_raw, historico_raw, reglas_categoria, mapeo_paises, zona):
    """Aplica todas las reglas y devuelve líneas válidas, rechazos, productos y métricas."""
    titulo("TRANSFORMACIÓN Y UNIFICACIÓN")

    ventas = normalizar_fuente(ventas_raw, zona, mapeo_paises)
    historico = normalizar_fuente(historico_raw, zona, mapeo_paises)

    rechazos = pd.concat([_rechazos(ventas), _rechazos(historico)], ignore_index=True)

    ventas_validas = ventas[ventas["rejection_reason"].isna()]
    historico_valido = historico[historico["rejection_reason"].isna()]
    lineas, duplicados_entre_fuentes = resolver_duplicados_entre_fuentes(
        ventas_validas, historico_valido
    )

    # Filas de ambas fuentes con un código de producto real (para contar variantes)
    descripciones_fuente = pd.concat(
        [
            df.loc[
                ~df["rejection_reason"].isin(
                    [cfg.MOTIVO_CAMPO_NULO, cfg.MOTIVO_CODIGO_NO_PRODUCTO]
                ),
                ["stock_code", "description"],
            ]
            for df in (ventas, historico)
        ],
        ignore_index=True,
    )
    productos = construir_productos(lineas, reglas_categoria, descripciones_fuente)
    sub(f"Productos únicos: {formato_numero(len(productos))}")
    sub(f"Categorías: {productos['category'].value_counts().to_dict()}")

    columnas_lineas = [
        "row_hash",
        "source_system",
        "invoice_no",
        "invoice_ts_utc",
        "stock_code",
        "customer_code",
        "country_name",
        "quantity",
        "unit_price",
    ]
    lineas = lineas[columnas_lineas].reset_index(drop=True)

    # Solo tipos nativos de Python: las métricas viajan por XCom (JSON)
    metricas = {
        "filas_leidas_ventas": len(ventas_raw),
        "filas_leidas_historico": len(historico_raw),
        "filas_validas": len(lineas),
        "filas_rechazadas": len(rechazos),
        "duplicados_entre_fuentes": duplicados_entre_fuentes,
        "rechazos_por_motivo": {k: int(v) for k, v in rechazos["reason"].value_counts().items()},
    }
    sub(f"Métricas: {metricas}")
    return {"lineas": lineas, "rechazos": rechazos, "productos": productos, "metricas": metricas}
