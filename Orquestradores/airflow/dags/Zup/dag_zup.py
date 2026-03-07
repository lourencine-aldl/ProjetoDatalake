# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
import sys
import os

# Importar notificador customizado
sys.path.insert(0, os.path.dirname(__file__))
from email_notifier import notify_success, notify_failure

# ✅ Timezone explícito Brasil
local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_zuq",
    description="Extração APIs ZUQ → Bronze completo (todos os scripts) + Parquet no MinIO",
    schedule="0 */10 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    tags=["zuq", "api", "integracao", "parquet", "minio"],
) as dag:

    run_bronze_full = BashOperator(
        task_id="run_zuq_bronze_full",
        bash_command=(
            "set -euo pipefail; "
            "bash /opt/airflow/dags/Zup/scripts/zuq/bronze/run_all_pipelines.sh 2>&1 | "
            "tee -a /opt/airflow/logs/zuq_bronze_full.log"
        ),
    )

    run_bronze_full


    