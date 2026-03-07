# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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

with DAG(
    dag_id="dag_orchestrator_sost",
    description="Orquestração do Pipeline SOST - Bronze → Silver",

    # ✅ 01:00 horário São Paulo
    schedule="0 1 * * *",

    start_date=pendulum.datetime(2026, 2, 1, tz=local_tz),

    catchup=False,
    default_args=default_args,
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    tags=["sost", "orchestrator", "datalake"],
    max_active_runs=1,
) as dag:

    # ✅ trigger_run_id único e rastreável por camada
    # ✅ conf com template direto — renderizado pelo Airflow na DAG filha
    # ✅ reset_dag_run=True — evita falha em re-execuções manuais

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_sost",
        trigger_dag_id="dag_bronze_sost",
        trigger_run_id="bronze_{{ run_id }}",
        wait_for_completion=True,
        poke_interval=60,
        deferrable=True,
        reset_dag_run=True,
        conf={
            "pipeline_run_id": "{{ run_id }}",
        },
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_sost",
        trigger_dag_id="dag_silver_sost",
        trigger_run_id="silver_{{ run_id }}",
        wait_for_completion=True,
        poke_interval=60,
        deferrable=True,
        reset_dag_run=True,
        conf={
            "pipeline_run_id": "{{ run_id }}",
        },
    )


    # ✅ Pipeline: Bronze → Silver
    trigger_bronze >> trigger_silver