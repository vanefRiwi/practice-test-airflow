"""DataMart daily DAG: reads the sales CSVs, cleans them and loads the Postgres DWH."""

import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Variable, dag, get_current_context, setup, task, teardown

# pandas and the ETL modules are imported inside each task so the DAG parses fast
from datamart import config as cfg

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def _dwh_engine():
    """SQLAlchemy engine built from the dwh_postgres Connection."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=cfg.CONN_DWH).get_sqlalchemy_engine()


def _path(directory, name):
    """Path of an intermediate parquet for this run."""
    return str(Path(directory) / f"{name}.parquet")


@dag(
    dag_id="datamart_daily_etl",
    description="DataMart sales and returns ETL into the analytical warehouse",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["datamart", "etl", "dwh"],
    doc_md=__doc__,
)
def datamart_daily_etl():
    # =========================================================================
    # SETUP
    # =========================================================================
    @setup
    @task
    def prepare_tmp_dir():
        """Creates data/tmp/<run_id>/ for this run's parquet files."""
        run_id = get_current_context()["run_id"]
        directory = cfg.TMP_DIR / re.sub(r"[^A-Za-z0-9_-]", "_", run_id)
        directory.mkdir(parents=True, exist_ok=True)
        return str(directory)

    @task
    def create_dwh_tables():
        """Runs the DDL in case the tables don't exist yet."""
        from datamart import load

        load.run_ddl(_dwh_engine())

    wait_for_sales_file = FileSensor(
        task_id="wait_for_sales_file",
        fs_conn_id=cfg.CONN_FILES,
        filepath="{{ var.value.datamart_sales_file }}",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )
    wait_for_history_file = FileSensor(
        task_id="wait_for_history_file",
        fs_conn_id=cfg.CONN_FILES,
        filepath="{{ var.value.datamart_history_file }}",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )

    # =========================================================================
    # EXTRACT
    # =========================================================================
    @task
    def extract_daily_sales(directory):
        """Reads data.csv."""
        from datamart import extract

        file = cfg.DATA_DIR / Variable.get(cfg.VAR_SALES_FILE)
        return extract.extract_source(file, cfg.SOURCE_SALES, _path(directory, "raw_sales"))

    @task
    def extract_history(directory):
        """Reads online_retail.csv."""
        from datamart import extract

        file = cfg.DATA_DIR / Variable.get(cfg.VAR_HISTORY_FILE)
        return extract.extract_source(file, cfg.SOURCE_HISTORY, _path(directory, "raw_history"))

    @task
    def extract_category_rules(directory):
        """Reads the category rules from seeds/category_rules.csv."""
        from datamart import extract

        rules = extract.read_category_rules()
        path = _path(directory, "category_rules")
        rules.to_parquet(path, index=False)
        return path

    # =========================================================================
    # TRANSFORM AND QUALITY
    # =========================================================================
    @task
    def transform_and_merge(sales, history, rules_path, directory):
        """Cleans and merges the two sources."""
        import pandas as pd

        from datamart import extract, transform

        result = transform.transform_and_merge(
            pd.read_parquet(sales["path"]),
            pd.read_parquet(history["path"]),
            pd.read_parquet(rules_path),
            extract.read_country_mapping(),
            Variable.get(cfg.VAR_TIMEZONE),
        )
        paths = {}
        for name in ["lines", "rejections", "products"]:
            paths[name] = _path(directory, name)
            result[name].to_parquet(paths[name], index=False)
        return {"paths": paths, "metrics": result["metrics"]}

    @task
    def check_quality(transformed, directory):
        """Splits sales from returns and validates before loading."""
        import pandas as pd

        from datamart import quality

        paths = transformed["paths"]
        metrics = transformed["metrics"]
        sales, returns = quality.split_sales_returns(
            pd.read_parquet(paths["lines"]), Variable.get(cfg.VAR_CURRENCY)
        )
        ratio = quality.check_contract(
            sales,
            returns,
            pd.read_parquet(paths["products"]),
            pd.read_parquet(paths["rejections"]),
            metrics,
            float(Variable.get(cfg.VAR_MAX_REJECT_RATIO)),
        )
        paths = {
            **paths,
            "sales": _path(directory, "sales"),
            "returns": _path(directory, "returns"),
        }
        sales.to_parquet(paths["sales"], index=False)
        returns.to_parquet(paths["returns"], index=False)
        return {"paths": paths, "metrics": {**metrics, "reject_ratio": round(ratio, 4)}}

    # =========================================================================
    # LOAD
    # =========================================================================
    @task
    def load_staging(checked):
        """Loads products, sales and returns into staging."""
        import pandas as pd

        from datamart import load

        paths = checked["paths"]
        load.load_staging(
            _dwh_engine(),
            pd.read_parquet(paths["products"]),
            pd.read_parquet(paths["sales"]),
            pd.read_parquet(paths["returns"]),
        )

    @task
    def load_rejections(checked):
        """Saves the rejections to audit.rejected_records."""
        import pandas as pd

        from datamart import load

        run_id = get_current_context()["run_id"]
        return load.load_rejections(
            _dwh_engine(), pd.read_parquet(checked["paths"]["rejections"]), run_id
        )

    @task
    def load_dimensions():
        """Upsert of dates, products, customers and countries."""
        from datamart import load

        return load.load_dimensions(_dwh_engine())

    @task
    def load_sales_fact():
        """Delete-insert of dwh.sales_fact."""
        from datamart import load

        return load.load_sales_fact(_dwh_engine())

    @task
    def load_returns_fact():
        """Delete-insert of dwh.returns_fact."""
        from datamart import load

        return load.load_returns_fact(_dwh_engine())

    @task
    def build_daily_revenue():
        """Gross revenue, returns and net revenue per product and day."""
        from datamart import load

        return load.build_daily_revenue(_dwh_engine())

    @task
    def validate_load(checked):
        """Checks the counts after loading and logs the run."""
        from datamart import load

        context = get_current_context()
        return load.validate_load(
            _dwh_engine(),
            checked["metrics"],
            context["run_id"],
            # manual runs can come without a logical_date
            context.get("logical_date"),
        )

    @teardown
    @task
    def cleanup_tmp(directory):
        """Deletes the run's parquet files, even if something failed."""
        shutil.rmtree(directory, ignore_errors=True)

    # =========================================================================
    # DEPENDENCIES
    # =========================================================================
    directory = prepare_tmp_dir()
    tables = create_dwh_tables()

    sales = extract_daily_sales(directory)
    history = extract_history(directory)
    rules = extract_category_rules(directory)
    wait_for_sales_file >> sales
    wait_for_history_file >> history

    transformed = transform_and_merge(sales, history, rules, directory)
    checked = check_quality(transformed, directory)

    staging = load_staging(checked)
    rejections = load_rejections(checked)
    tables >> [staging, rejections]

    dimensions = load_dimensions()
    sales_fact = load_sales_fact()
    returns_fact = load_returns_fact()
    daily_revenue = build_daily_revenue()
    validation = validate_load(checked)

    staging >> dimensions >> [sales_fact, returns_fact] >> daily_revenue
    [daily_revenue, rejections] >> validation >> cleanup_tmp(directory)


datamart_daily_etl()
