"""Verifica que el DAG cargue sin errores (requiere Airflow; se omite si no está instalado)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models.dagbag import DagBag  # noqa: E402

DIRECTORIO_DAGS = Path(__file__).resolve().parents[1] / "dags"


def test_dag_sin_errores_de_importacion():
    dagbag = DagBag(dag_folder=str(DIRECTORIO_DAGS), include_examples=False)
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag("datamart_etl_diario")
    assert dag is not None
    assert dag.default_args["retries"] >= 1
    assert {"extraer_ventas_diarias", "extraer_historico", "cargar_hecho_ventas"} <= set(
        dag.task_ids
    )
