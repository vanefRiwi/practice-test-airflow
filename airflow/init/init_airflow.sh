#!/usr/bin/env bash
# =============================================================================
# Airflow bootstrap (airflow-init service)
# The image entrypoint already migrated the metadata DB and created the admin
# user before getting here (_AIRFLOW_DB_MIGRATE and _AIRFLOW_WWW_USER_CREATE).
# This script sets up the folders, the Connections and the Variables.
# Safe to re-run on every `docker compose up`.
# =============================================================================
set -euo pipefail

echo "1. Shared folders and permissions"
mkdir -p /opt/airflow/{logs,dags,plugins,config,include} /opt/airflow/data/{raw,tmp}
chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/{logs,dags,plugins,config} /opt/airflow/data/tmp

echo "2. Connection to the analytical warehouse (credentials from .env)"
airflow connections delete dwh_postgres > /dev/null 2>&1 || true
airflow connections add dwh_postgres \
    --conn-type postgres \
    --conn-host "${DWH_HOST}" \
    --conn-port "${DWH_PORT}" \
    --conn-login "${DWH_USER}" \
    --conn-password "${DWH_PASSWORD}" \
    --conn-schema "${DWH_DB}" \
    --conn-description "DataMart analytical warehouse (PostgreSQL)"

echo "3. Connection to the files folder (FileSensor)"
airflow connections delete fs_datamart > /dev/null 2>&1 || true
airflow connections add fs_datamart \
    --conn-type fs \
    --conn-extra '{"path": "/opt/airflow/data"}' \
    --conn-description "Shared folder with the source CSVs"

echo "4. Pipeline Variables"
airflow variables import /opt/airflow/init/variables.json

echo "5. Check (ids only, so passwords don't end up in the log)"
airflow connections list --output json \
    | python -c "import json, sys; print('Connections:', [c['conn_id'] for c in json.load(sys.stdin)])"
airflow variables list --output table
echo "Init done."
