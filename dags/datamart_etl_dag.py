"""DAG diario de DataMart: lee los CSV de ventas, los limpia y carga el DWH en Postgres."""

import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Variable, dag, get_current_context, setup, task, teardown

# pandas y los módulos del ETL se importan dentro de cada tarea para que el DAG cargue rápido
from datamart import configuracion as cfg

ARGUMENTOS_POR_DEFECTO = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def _engine_dwh():
    """Engine de SQLAlchemy usando la Connection dwh_postgres."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=cfg.CONN_DWH).get_sqlalchemy_engine()


def _ruta(directorio, nombre):
    """Ruta del parquet intermedio de esta ejecución."""
    return str(Path(directorio) / f"{nombre}.parquet")


@dag(
    dag_id="datamart_etl_diario",
    description="ETL de ventas y devoluciones de DataMart hacia el repositorio analítico",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=ARGUMENTOS_POR_DEFECTO,
    tags=["datamart", "etl", "dwh"],
    doc_md=__doc__,
)
def datamart_etl_diario():
    # =========================================================================
    # PREPARACIÓN
    # =========================================================================
    @setup
    @task
    def preparar_directorio_temporal():
        """Crea la carpeta data/tmp/<run_id>/ para los parquet de este run."""
        run_id = get_current_context()["run_id"]
        directorio = cfg.DIRECTORIO_TEMPORAL / re.sub(r"[^A-Za-z0-9_-]", "_", run_id)
        directorio.mkdir(parents=True, exist_ok=True)
        return str(directorio)

    @task
    def crear_tablas_dwh():
        """Corre los DDL por si las tablas no existen."""
        from datamart import carga

        carga.ejecutar_ddl(_engine_dwh())

    esperar_archivo_ventas = FileSensor(
        task_id="esperar_archivo_ventas",
        fs_conn_id=cfg.CONN_ARCHIVOS,
        filepath="{{ var.value.datamart_archivo_ventas }}",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )
    esperar_archivo_historico = FileSensor(
        task_id="esperar_archivo_historico",
        fs_conn_id=cfg.CONN_ARCHIVOS,
        filepath="{{ var.value.datamart_archivo_historico }}",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )

    # =========================================================================
    # EXTRACCIÓN
    # =========================================================================
    @task
    def extraer_ventas_diarias(directorio):
        """Lee data.csv."""
        from datamart import extraccion

        archivo = cfg.DIRECTORIO_DATOS / Variable.get(cfg.VAR_ARCHIVO_VENTAS)
        return extraccion.extraer_fuente(
            archivo, cfg.FUENTE_VENTAS, _ruta(directorio, "extraido_ventas")
        )

    @task
    def extraer_historico(directorio):
        """Lee online_retail.csv."""
        from datamart import extraccion

        archivo = cfg.DIRECTORIO_DATOS / Variable.get(cfg.VAR_ARCHIVO_HISTORICO)
        return extraccion.extraer_fuente(
            archivo, cfg.FUENTE_HISTORICO, _ruta(directorio, "extraido_historico")
        )

    @task
    def extraer_reglas_categoria(directorio):
        """Lee las reglas de categoría de seeds/category_rules.csv."""
        from datamart import extraccion

        reglas = extraccion.leer_reglas_categoria()
        ruta = _ruta(directorio, "reglas_categoria")
        reglas.to_parquet(ruta, index=False)
        return ruta

    # =========================================================================
    # TRANSFORMACIÓN Y CALIDAD
    # =========================================================================
    @task
    def transformar_y_unificar(ventas, historico, ruta_reglas, directorio):
        """Limpia y une las dos fuentes."""
        import pandas as pd

        from datamart import extraccion, transformacion

        resultado = transformacion.transformar_y_unificar(
            pd.read_parquet(ventas["ruta"]),
            pd.read_parquet(historico["ruta"]),
            pd.read_parquet(ruta_reglas),
            extraccion.leer_mapeo_paises(),
            Variable.get(cfg.VAR_ZONA_HORARIA),
        )
        rutas = {}
        for nombre in ["lineas", "rechazos", "productos"]:
            rutas[nombre] = _ruta(directorio, nombre)
            resultado[nombre].to_parquet(rutas[nombre], index=False)
        return {"rutas": rutas, "metricas": resultado["metricas"]}

    @task
    def validar_calidad(transformado, directorio):
        """Separa ventas de devoluciones y valida antes de cargar."""
        import pandas as pd

        from datamart import calidad

        rutas = transformado["rutas"]
        metricas = transformado["metricas"]
        ventas, devoluciones = calidad.separar_ventas_devoluciones(
            pd.read_parquet(rutas["lineas"]), Variable.get(cfg.VAR_MONEDA)
        )
        ratio = calidad.validar_contrato(
            ventas,
            devoluciones,
            pd.read_parquet(rutas["productos"]),
            pd.read_parquet(rutas["rechazos"]),
            metricas,
            float(Variable.get(cfg.VAR_MAX_RATIO_RECHAZOS)),
        )
        rutas = {
            **rutas,
            "ventas": _ruta(directorio, "ventas"),
            "devoluciones": _ruta(directorio, "devoluciones"),
        }
        ventas.to_parquet(rutas["ventas"], index=False)
        devoluciones.to_parquet(rutas["devoluciones"], index=False)
        return {"rutas": rutas, "metricas": {**metricas, "ratio_rechazos": round(ratio, 4)}}

    # =========================================================================
    # CARGA
    # =========================================================================
    @task
    def cargar_staging(validado):
        """Carga productos, ventas y devoluciones a staging."""
        import pandas as pd

        from datamart import carga

        rutas = validado["rutas"]
        carga.cargar_staging(
            _engine_dwh(),
            pd.read_parquet(rutas["productos"]),
            pd.read_parquet(rutas["ventas"]),
            pd.read_parquet(rutas["devoluciones"]),
        )

    @task
    def cargar_rechazos(validado):
        """Guarda los rechazos en audit.rejected_records."""
        import pandas as pd

        from datamart import carga

        run_id = get_current_context()["run_id"]
        return carga.cargar_rechazos(
            _engine_dwh(), pd.read_parquet(validado["rutas"]["rechazos"]), run_id
        )

    @task
    def cargar_dimensiones():
        """Upsert de fechas, productos, clientes y países."""
        from datamart import carga

        return carga.cargar_dimensiones(_engine_dwh())

    @task
    def cargar_hecho_ventas():
        """Delete-insert de dwh.sales_fact."""
        from datamart import carga

        return carga.cargar_hecho_ventas(_engine_dwh())

    @task
    def cargar_hecho_devoluciones():
        """Delete-insert de dwh.returns_fact."""
        from datamart import carga

        return carga.cargar_hecho_devoluciones(_engine_dwh())

    @task
    def construir_revenue_diario():
        """Revenue bruto, devoluciones y neto por producto y día."""
        from datamart import carga

        return carga.construir_revenue_diario(_engine_dwh())

    @task
    def validar_carga(validado):
        """Revisa los conteos después de cargar y registra el run."""
        from datamart import carga

        contexto = get_current_context()
        return carga.validar_carga(
            _engine_dwh(),
            validado["metricas"],
            contexto["run_id"],
            # los runs manuales pueden venir sin logical_date
            contexto.get("logical_date"),
        )

    @teardown
    @task
    def limpiar_temporales(directorio):
        """Borra los parquet del run, aunque algo falle."""
        shutil.rmtree(directorio, ignore_errors=True)

    # =========================================================================
    # DEPENDENCIAS
    # =========================================================================
    directorio = preparar_directorio_temporal()
    tablas = crear_tablas_dwh()

    ventas = extraer_ventas_diarias(directorio)
    historico = extraer_historico(directorio)
    reglas = extraer_reglas_categoria(directorio)
    esperar_archivo_ventas >> ventas
    esperar_archivo_historico >> historico

    transformado = transformar_y_unificar(ventas, historico, reglas, directorio)
    validado = validar_calidad(transformado, directorio)

    staging = cargar_staging(validado)
    rechazos = cargar_rechazos(validado)
    tablas >> [staging, rechazos]

    dimensiones = cargar_dimensiones()
    hecho_ventas = cargar_hecho_ventas()
    hecho_devoluciones = cargar_hecho_devoluciones()
    agregado = construir_revenue_diario()
    validacion = validar_carga(validado)

    staging >> dimensiones >> [hecho_ventas, hecho_devoluciones] >> agregado
    [agregado, rechazos] >> validacion >> limpiar_temporales(directorio)


datamart_etl_diario()
