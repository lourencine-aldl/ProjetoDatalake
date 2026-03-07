# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta
import pendulum
import sys
import os

# Importar notificador customizado
sys.path.insert(0, os.path.dirname(__file__))
from email_notifier import notify_success, notify_failure

# ✅ Define timezone explícito
local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_bronze_sost",
    description="Extração Bronze Layer - SOST (PostgreSQL → MinIO)",

    # ✅ 02:00 horário São Paulo
    schedule="0 2 * * *",

    # ✅ Start date com timezone explícito
    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),

    catchup=False,
    default_args=default_args,
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    tags=["sost", "bronze", "datalake", "postgres", "minio"],
    max_active_runs=1,
) as dag:

    bronze_pcsupervisor = BashOperator(
        task_id="bronze_001_pcsupervisor",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/001_bronze_pcsupervisor.py",
    )

    bronze_pcusuario = BashOperator(
        task_id="bronze_002_pcusuario",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/002_bronze_pcusuario.py",
    )

    bronze_tab_cliente = BashOperator(
        task_id="bronze_003_tab_cliente",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/003_bronze_tab_cliente.py",
    )

    bronze_pcnfent = BashOperator(
        task_id="bronze_004_pcnfent",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/004_bronze_pcnfent.py",
    )

    bronze_pcnfsaid = BashOperator(
        task_id="bronze_005_pcnfsaid",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/005_bronze_pcnfsaid.py",
    )

    bronze_tab_faturamento = BashOperator(
        task_id="bronze_006_tab_faturamento_full",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/006_bronze_tab_faturamento_full.py",
        pool="heavy_jobs",
        pool_slots=1,
    )

    bronze_tab_movimentacao = BashOperator(
        task_id="bronze_007_tab_movimentacao",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/007_bronze_tab_movimentacao.py",
    )

    bronze_pcprodut = BashOperator(
        task_id="bronze_008_pcprodut",
        bash_command="python3 /opt/airflow/dags/Sost/scripts/Datalake/Bronze/008_bronze_pcprodut.py",
    )

    (
        bronze_pcsupervisor
        >> bronze_pcusuario
        >> bronze_tab_cliente
        >> bronze_pcnfent
        >> bronze_pcnfsaid
        >> bronze_tab_faturamento
        >> bronze_tab_movimentacao
        >> bronze_pcprodut
    )