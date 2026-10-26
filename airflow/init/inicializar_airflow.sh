#!/usr/bin/env bash
# =============================================================================
# Inicialización automática de Airflow (servicio airflow-init)
# El entrypoint de la imagen ya migró la base de metadatos y creó el usuario admin
# antes de llegar aquí (_AIRFLOW_DB_MIGRATE y _AIRFLOW_WWW_USER_CREATE).
# Este script deja listas las carpetas, las Connections y las Variables.
# Es idempotente: se puede ejecutar en cada `docker compose up`.
# =============================================================================
set -euo pipefail

echo "1. Carpetas compartidas y permisos"
mkdir -p /opt/airflow/{logs,dags,plugins,config,include} /opt/airflow/data/{raw,tmp}
chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/{logs,dags,plugins,config} /opt/airflow/data/tmp

echo "2. Connection al repositorio analítico (credenciales desde .env)"
airflow connections delete dwh_postgres > /dev/null 2>&1 || true
airflow connections add dwh_postgres \
    --conn-type postgres \
    --conn-host "${DWH_HOST}" \
    --conn-port "${DWH_PORT}" \
    --conn-login "${DWH_USER}" \
    --conn-password "${DWH_PASSWORD}" \
    --conn-schema "${DWH_DB}" \
    --conn-description "Repositorio analítico DataMart (PostgreSQL)"

echo "3. Connection al directorio de archivos (FileSensor)"
airflow connections delete fs_datamart > /dev/null 2>&1 || true
airflow connections add fs_datamart \
    --conn-type fs \
    --conn-extra '{"path": "/opt/airflow/data"}' \
    --conn-description "Carpeta compartida con los CSV de origen"

echo "4. Variables operativas del pipeline"
airflow variables import /opt/airflow/init/variables.json

echo "5. Verificación (solo ids, para no imprimir contraseñas en el log)"
airflow connections list --output json \
    | python -c "import json, sys; print('Connections:', [c['conn_id'] for c in json.load(sys.stdin)])"
airflow variables list --output table
echo "Inicialización completa."
