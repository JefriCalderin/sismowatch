"""
### DAG de Ingesta Continua - SismoWatch
Consulta el feed en tiempo casi real de USGS Earthquake Catalog cada 5 minutos
y almacena los datos de forma idempotente en la capa Bronze (bronze.earthquakes_raw).
Al finalizar con éxito, dispara la transformación Medallón.
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.include.extractors import fetch_usgs_feed, save_raw_to_bronze

logger = logging.getLogger(__name__)

def failure_alert(context):
    """Callback de notificación ante fallos en la ejecución de la tarea."""
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    exec_date = context.get("execution_date")
    logger.critical(
        f"[ALERTA DE SEGURIDAD/FALLO] DAG: {dag_id} | Tarea: {ti.task_id} "
        f"falló en ejecución {exec_date}. Excepción: {context.get('exception')}"
    )

default_args = {
    "owner": "sismowatch_dataops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": failure_alert,
}

def ingest_task_callable(**kwargs):
    """Extrae el feed de la última hora de USGS e inserta en Bronze."""
    data = fetch_usgs_feed()
    inserted_count = save_raw_to_bronze(data, source_feed="usgs_all_hour_feed")
    logger.info(f"Ingesta completada: {inserted_count} eventos en Bronze.")
    return inserted_count

with DAG(
    dag_id="ingest_earthquakes_dag",
    default_args=default_args,
    description="Ingesta periódica cada 5 min desde USGS API hacia Bronze (Medallón)",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "usgs", "bronze", "medallion", "realtime"],
    doc_md=__doc__,
) as dag:

    ingest_feed_to_bronze = PythonOperator(
        task_id="ingest_feed_to_bronze",
        python_callable=ingest_task_callable,
    )

    trigger_transformation = TriggerDagRunOperator(
        task_id="trigger_transformation",
        trigger_dag_id="transform_quality_dag",
        wait_for_completion=False,
    )

    ingest_feed_to_bronze >> trigger_transformation
