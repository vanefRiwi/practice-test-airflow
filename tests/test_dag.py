"""Checks that the DAG loads without errors. Only runs if Airflow is installed."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


def test_dag_has_no_import_errors():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag("datamart_daily_etl")
    assert dag is not None
    assert dag.default_args["retries"] >= 1
    assert {"extract_daily_sales", "extract_history", "load_sales_fact"} <= set(dag.task_ids)
