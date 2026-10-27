"""
### DAG de Backfill Histórico Parametrizado - SismoWatch
Permite cargar de forma controlada el historial sísmico de los últimos N días
desde el endpoint de consulta de USGS (FDSNWS), procesando de forma idempotente
toda la arquitectura Medallón (Bronze -> Silver -> Gold).
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.include.extractors import fetch_usgs_historical, save_raw_to_bronze
from airflow.include.spark_transformations import (
    process_bronze_to_silver,
    process_silver_to_gold,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "sismowatch_dataops",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}

def run_historical_backfill(**kwargs):
    """Ejecuta la ingesta histórica parametrizada y la transformación completa."""
    params = kwargs.get("params", {})
    days = int(params.get("days", 30))
    min_mag = float(params.get("min_magnitude", 2.5))

    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=days)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    logger.info(f"Iniciando Backfill: {days} días ({start_str} a {end_str}), Magnitud mínima: {min_mag}")
    data = fetch_usgs_historical(start_date=start_str, end_date=end_str, min_magnitude=min_mag)

    # 1. Guardar en Bronze
    inserted_bronze = save_raw_to_bronze(data, source_feed=f"usgs_backfill_{days}d")
    logger.info(f"Bronze actualizado: {inserted_bronze} eventos.")

    # 2. Transformar Bronze -> Silver
    silver_count = process_bronze_to_silver()
    logger.info(f"Silver procesado: {silver_count} eventos.")

    # 3. Validar Calidad y Transformar Silver -> Gold
    run_id = f"backfill_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    gold_count = process_silver_to_gold(pipeline_run_id=run_id)
    logger.info(f"Gold curado: {gold_count} eventos. Backfill finalizado con éxito.")

    return {
        "days": days,
        "bronze_records": inserted_bronze,
        "silver_records": silver_count,
        "gold_records": gold_count,
    }

with DAG(
    dag_id="backfill_earthquakes_dag",
    default_args=default_args,
    description="Backfill histórico parametrizado desde USGS (últimos N días)",
    schedule_interval=None, # Solo se ejecuta manualmente o vía API de administración
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["backfill", "history", "usgs", "medallion", "admin"],
    params={
        "days": Param(30, type="integer", minimum=1, maximum=90, description="Días de historial a consultar"),
        "min_magnitude": Param(2.5, type="number", minimum=1.0, maximum=8.0, description="Magnitud mínima a filtrar en USGS"),
    },
    doc_md=__doc__,
) as dag:

    backfill_task = PythonOperator(
        task_id="execute_usgs_historical_backfill",
        python_callable=run_historical_backfill,
    )
