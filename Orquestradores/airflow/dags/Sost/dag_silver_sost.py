# -*- coding: utf-8 -*-
"""
DAG Silver Layer - SOST (MinIO -> PostgreSQL DW)

Executa sequencialmente as Silvers ordenadas pelo prefixo:
001_silver_*
004_silver_*
005_silver_*
008_silver_*
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Lista das silvers que você quer rodar
SILVER_SCRIPTS = [
    "005_silver_pcnfsaid.py",
    "002_silver_pcusuario.py",
    "006_silver_tab_faturamento_full.py",
    "008_silver_pcprodut.py",
]

# Ordena automaticamente pelo número inicial (001, 004, 005...)
SILVER_SCRIPTS = sorted(SILVER_SCRIPTS, key=lambda x: int(x.split("_")[0]))

with DAG(
    dag_id="dag_silver_sost",
    description="Carga Silver Layer - SOST (MinIO → PostgreSQL DW)",
    schedule=None,  # disparada pelo orquestrador
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),
    catchup=False,
    default_args=default_args,
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    tags=["sost", "silver", "dwh", "postgres", "minio"],
    max_active_runs=1,
) as dag:

    previous_task = None

    for script in SILVER_SCRIPTS:
        task = BashOperator(
            task_id=script.replace(".py", ""),
            bash_command=f"python3 /opt/airflow/dags/Sost/scripts/Datalake/Silver/{script}",
            pool="heavy_jobs",  # garante execução controlada
            pool_slots=1,
        )

        if previous_task:
            previous_task >> task

        previous_task = task