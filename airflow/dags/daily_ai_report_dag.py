"""
### DAG Diario de Reporte Ejecutivo con IA - SismoWatch
Genera diariamente el informe ejecutivo para operaciones y directivos.
Controles implementados:
- Cero alucinación numérica: las cifras son calculadas formalmente en SQL/Python.
- Validación de consistencia matemática entre el texto generado y los datos calculados.
- Plan alterno (Fallback determinista) ante indisponibilidad del LLM.
- Auditoría completa de costos, latencia y tokens en `gold.ai_execution_logs`.
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.include.ai_client import generate_daily_ai_report

logger = logging.getLogger(__name__)

default_args = {
    "owner": "sismowatch_dataops",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}

def ai_report_callable(**kwargs):
    """Ejecuta la generación del reporte diario con IA y validaciones numéricas."""
    execution_date = kwargs.get("execution_date")
    target_date = (execution_date - timedelta(days=1)).date() if execution_date else None
    report = generate_daily_ai_report(target_date=target_date)
    logger.info(f"Reporte IA generado satisfactoriamente: {report.get('title')}")
    return report

with DAG(
    dag_id="daily_ai_report_dag",
    default_args=default_args,
    description="Generación diaria de informe ejecutivo sísmico con LLM auditado y validado",
    schedule_interval="5 0 * * *", # 00:05 UTC todos los días
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ai", "reporting", "llm", "gold", "executive"],
    doc_md=__doc__,
) as dag:

    generate_report_task = PythonOperator(
        task_id="generate_executive_ai_report",
        python_callable=ai_report_callable,
    )
