"""Rutas, nombres de Connections/Variables y reglas del pipeline."""

import os
from pathlib import Path

# =============================================================================
# RUTAS (dentro del contenedor todo vive bajo /opt/airflow)
# =============================================================================
DIRECTORIO_BASE = Path(os.environ.get("DATAMART_BASE_DIR", "/opt/airflow"))
DIRECTORIO_DATOS = DIRECTORIO_BASE / "data"
DIRECTORIO_TEMPORAL = DIRECTORIO_DATOS / "tmp"
DIRECTORIO_SEEDS = DIRECTORIO_BASE / "seeds"
DIRECTORIO_DDL = DIRECTORIO_BASE / "sql" / "ddl"

ARCHIVO_REGLAS_CATEGORIA = DIRECTORIO_SEEDS / "category_rules.csv"
ARCHIVO_MAPEO_PAISES = DIRECTORIO_SEEDS / "country_mapping.csv"

# =============================================================================
# AIRFLOW: CONNECTIONS Y VARIABLES
# =============================================================================
CONN_DWH = "dwh_postgres"
CONN_ARCHIVOS = "fs_datamart"

VAR_ARCHIVO_VENTAS = "datamart_archivo_ventas"
VAR_ARCHIVO_HISTORICO = "datamart_archivo_historico"
VAR_ZONA_HORARIA = "datamart_zona_horaria_origen"
VAR_MAX_RATIO_RECHAZOS = "datamart_max_ratio_rechazos"
VAR_MONEDA = "datamart_moneda"

# =============================================================================
# FUENTES
# =============================================================================
FUENTE_VENTAS = "ventas_diarias"  # data.csv (volcado diario, tiene prioridad)
FUENTE_HISTORICO = "historico"  # online_retail.csv (historial extendido de Kaggle)
FUENTES = [FUENTE_VENTAS, FUENTE_HISTORICO]

# data.csv y online_retail.csv no usan los mismos nombres de columna
ALIAS_COLUMNAS = {
    "invoiceno": "invoice_no",
    "invoice": "invoice_no",
    "stockcode": "stock_code",
    "description": "description",
    "quantity": "quantity",
    "invoicedate": "invoice_date_raw",
    "unitprice": "unit_price",
    "price": "unit_price",
    "customerid": "customer_id",
    "customer id": "customer_id",
    "country": "country",
}
COLUMNAS_ESTANDAR = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date_raw",
    "unit_price",
    "customer_id",
    "country",
]

# Formatos de fecha que se prueban en orden para cada fuente
FORMATOS_FECHA = {
    FUENTE_VENTAS: ["%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S"],
    FUENTE_HISTORICO: [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M",
    ],
}

# =============================================================================
# REGLAS DE NEGOCIO
# =============================================================================
# Los productos empiezan con 5 dígitos (85123A, 15056BL) o son sets DCGS.
# POST, DOT, M, D, C2, BANK CHARGES, AMAZONFEE, GIFT_0001... no son productos.
PATRON_CODIGO_PRODUCTO = r"^(\d{5}[A-Z0-9]{0,4}|DCGS[A-Z0-9]+)$"

CLIENTE_ANONIMO_ID = -1
CLIENTE_ANONIMO_CODIGO = "ANONIMO"
CATEGORIA_SIN_CLASIFICAR = "Sin clasificar"
DESCRIPCION_VACIA = "SIN DESCRIPCION"
PAIS_DESCONOCIDO = "Unknown"

# Motivos de rechazo (se guardan en audit.rejected_records)
MOTIVO_CAMPO_NULO = "campo_obligatorio_nulo"
MOTIVO_FECHA_INVALIDA = "fecha_invalida"
MOTIVO_CODIGO_NO_PRODUCTO = "codigo_no_producto"
MOTIVO_PRECIO_NO_POSITIVO = "precio_no_positivo"
MOTIVO_PRECIO_NEGATIVO_DEVOLUCION = "precio_negativo_en_devolucion"
MOTIVO_DUPLICADO_EN_FUENTE = "duplicado_en_fuente"  # misma clave natural repetida

# Tipos de devolución (quantity <= 0)
DEVOLUCION_CANCELACION = "cancelacion"  # factura con prefijo C
DEVOLUCION_AJUSTE = "ajuste"  # cantidad negativa sin factura C (daños, inventario)
