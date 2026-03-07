# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_bronze_zuq",
    description="Extração das APIs ZUQ - Bronze completo (todos os scripts)",
    schedule="0 */10 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["zuq", "api", "integracao", "bronze"],
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


    