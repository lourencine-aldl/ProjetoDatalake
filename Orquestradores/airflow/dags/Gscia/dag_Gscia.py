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
    dag_id="dag_Gscia",
    description="Extração de dados SQL Server (cndcondo, cndpagar) para camada RAW no MinIO",
    schedule="0 2 * * *",  # Executa diariamente às 2h da manhã
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gscia", "sqlserver", "raw-layer", "cndcondo", "cndpagar"],
) as dag:

    extract_cndcondo = BashOperator(
        task_id="extract_cndcondo_raw_layer",
        bash_command=(
            "set -euo pipefail; "
            "python3 /opt/airflow/dags/Gscia/scripts/raw_layer_cndcondo_env.py 2>&1 | "
            "tee -a /opt/airflow/logs/gscia_cndcondo_extraction.log"
        ),
    )

    extract_cndpagar = BashOperator(
        task_id="extract_cndpagar_raw_layer",
        bash_command=(
            "set -euo pipefail; "
            "python3 /opt/airflow/dags/Gscia/scripts/raw_layer_cndpagar.py 2>&1 | "
            "tee -a /opt/airflow/logs/gscia_cndpagar_extraction.log"
        ),
    )

    # Dependências: ambas as extrações em paralelo
    extract_cndcondo
    extract_cndpagar
