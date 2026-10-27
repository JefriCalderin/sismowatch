"""
### DAG de Transformación y Calidad Medallón - SismoWatch
Procesa la transición de Bronze -> Silver -> Gold:
1. Limpia, tipa, desanida coordenadas y sanitiza texto hacia `silver.earthquakes_cleaned`.
2. Aplica la compuerta de calidad de datos bloqueante (Pandera).
3. Detecta anomalías y enjambres con ML y publica en `gold.fact_earthquakes` y `gold.daily_metrics`.
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.include.spark_transformations import (
    process_bronze_to_silver,
    process_silver_to_gold,
)

logger = logging.getLogger(__name__)

def failure_alert(context):
    """Notificación ante fallas en tareas críticas de calidad."""
    ti = context.get("task_instance")
    logger.critical(f"[PIPELINE ERROR] Falló {ti.task_id} en ejecución {context.get('execution_date')}")

default_args = {
    "owner": "sismowatch_dataops",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": failure_alert,
}

def bronze_to_silver_callable(**kwargs):
    """Ejecuta la transformación y limpieza de datos crudos a Silver."""
    return process_bronze_to_silver()

def silver_to_gold_callable(**kwargs):
    """Aplica Data Quality Gate y publica datos curados y enriquecidos con ML en Gold."""
    run_id = kwargs.get("run_id", f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
    return process_silver_to_gold(pipeline_run_id=run_id)

with DAG(
    dag_id="transform_quality_dag",
    default_args=default_args,
    description="Transformación Medallón Bronze a Silver y Silver a Gold con Compuerta de Calidad y ML",
    schedule_interval=None, # Disparado por ingesta o ejecutable bajo demanda
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "silver", "gold", "quality", "ml", "medallion"],
    doc_md=__doc__,
) as dag:

    with TaskGroup("bronze_to_silver_group", tooltip="Limpieza, Tipado y Sanitización") as group_silver:
        transform_silver = PythonOperator(
            task_id="transform_clean_to_silver",
            python_callable=bronze_to_silver_callable,
        )

    with TaskGroup("silver_to_gold_group", tooltip="Compuerta de Calidad Bloqueante y Curación Gold") as group_gold:
        transform_gold = PythonOperator(
            task_id="quality_gate_and_gold_marts",
            python_callable=silver_to_gold_callable,
        )

    group_silver >> group_gold
