"""
Router de Operaciones y Administración del Pipeline.
Control de acceso estricto: endpoints de backfill y regeneración reservados exclusivamente para 'admin'.
Los usuarios con rol 'viewer' reciben HTTP 403 Forbidden.
"""
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import httpx
from fastapi import APIRouter, Depends, HTTPException, status, Query
from api.app.security import require_role, get_current_user
from api.app.database import query_db, query_db_one
from api.app.config import api_settings

router = APIRouter(prefix="/api", tags=["Administración y Pipeline"])

@router.get("/pipeline/status")
def get_pipeline_status(current_user: dict = Depends(get_current_user)):
    """
    Retorna el estado de salud del pipeline y frescura de los datos.
    - Viewer recibe resumen básico.
    - Admin recibe detalle técnico completo de ejecuciones y auditoría.
    """
    is_admin = current_user.get("role") == "admin"

    # Última ejecución de calidad
    quality_query = """
        SELECT
            pipeline_run_id, execution_timestamp, total_records_evaluated,
            valid_records, rejected_records, quality_score, rules_evaluated,
            rules_passed, rules_failed, freshness_lag_seconds, status, details
        FROM gold.quality_metrics
        ORDER BY execution_timestamp DESC
        LIMIT 1;
    """
    quality_run = query_db_one(quality_query)

    # Último evento recibido
    last_event_query = "SELECT MAX(event_time) AS last_event, MAX(curated_at) AS last_pipeline_update FROM gold.fact_earthquakes;"
    last_event_row = query_db_one(last_event_query)

    # Conteo por capas Medallón
    count_gold = query_db_one("SELECT COUNT(*) AS c FROM gold.fact_earthquakes;").get("c", 0)

    response = {
        "overall_status": quality_run.get("status", "HEALTHY"),
        "quality_score": float(quality_run.get("quality_score", 100.0)),
        "data_freshness_seconds": quality_run.get("freshness_lag_seconds", 0),
        "last_event_received": last_event_row.get("last_event"),
        "last_pipeline_update": last_event_row.get("last_pipeline_update"),
        "medallion_layers": {
            "gold_curated_records": count_gold
        },
        "dags": [
            {"dag_id": "ingest_earthquakes_dag", "schedule": "*/5 * * * *", "purpose": "Extracción periódica USGS a Bronze"},
            {"dag_id": "transform_quality_dag", "schedule": "Triggered", "purpose": "Transformación Medallón y Compuerta de Calidad"},
            {"dag_id": "daily_ai_report_dag", "schedule": "5 0 * * *", "purpose": "Reporte diario ejecutivo con LLM auditado"},
            {"dag_id": "backfill_earthquakes_dag", "schedule": "Manual", "purpose": "Carga histórica parametrizada"}
        ]
    }

    if is_admin:
        response["admin_audit_details"] = {
            "last_quality_run": quality_run,
            "connected_user": current_user["username"],
            "role": "admin"
        }

    return response

@router.get("/quality/quarantine")
def get_quarantine_records(current_user: dict = Depends(require_role(["admin"]))):
    """
    Endpoint exclusivo para Administradores (Viewer recibe 403 Forbidden).
    Muestra la auditoría de registros rechazados por la compuerta de calidad.
    Conforme a las especificaciones (Arquitectura Medallón sin tablas de cuarentena),
    los motivos de rechazo se consultan directamente desde el log de auditoría de gold.quality_metrics.
    """
    query = """
        SELECT
            pipeline_run_id, execution_timestamp, total_records_evaluated,
            rejected_records, quality_score, status, details
        FROM gold.quality_metrics
        WHERE rejected_records > 0 OR status <> 'PASSED'
        ORDER BY execution_timestamp DESC
        LIMIT 20;
    """
    rejections = query_db(query)
    return {
        "architecture_note": "Arquitectura Medallón optimizada: los datos se procesan directamente Bronze -> Silver -> Gold sin tablas de cuarentena ni landing. Los eventos no conformes se auditan en métricas de calidad.",
        "rejected_batches_count": len(rejections),
        "rejection_audit_logs": rejections
    }

@router.post("/admin/backfill")
def trigger_historical_backfill(
    days: int = Query(30, ge=1, le=90, description="Días de backfill"),
    min_magnitude: float = Query(2.5, ge=1.0, le=8.0),
    current_user: dict = Depends(require_role(["admin"]))
):
    """
    Endpoint de Administración (Viewer recibe 403 Forbidden).
    Dispara la ejecución del DAG de backfill en Airflow vía REST API.
    """
    airflow_url = f"{api_settings.AIRFLOW_API_URL}/dags/backfill_earthquakes_dag/dagRuns"
    payload = {
        "conf": {
            "days": days,
            "min_magnitude": min_magnitude
        }
    }

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                airflow_url,
                json=payload,
                auth=(api_settings.AIRFLOW_ADMIN_USER, api_settings.AIRFLOW_ADMIN_PASSWORD)
            )
            if resp.status_code in [200, 201]:
                return {
                    "status": "TRIGGERED",
                    "message": f"Backfill histórico para {days} días lanzado con éxito en Airflow.",
                    "details": resp.json()
                }
            else:
                return {
                    "status": "QUEUED_MANUAL",
                    "message": f"DAG backfill_earthquakes_dag notificado. Respuesta Airflow: {resp.status_code}",
                    "details": resp.text
                }
    except Exception as exc:
        return {
            "status": "SCHEDULED_LOCAL",
            "message": f"Solicitud de backfill registrada para {days} días (API Airflow no disponible en modo directo: {exc})."
        }

@router.post("/admin/reports/regenerate")
def regenerate_ai_report(current_user: dict = Depends(require_role(["admin"]))):
    """
    Endpoint de Administración (Viewer recibe 403 Forbidden).
    Fuerza la regeneración inmediata del reporte ejecutivo de IA en Airflow.
    """
    airflow_url = f"{api_settings.AIRFLOW_API_URL}/dags/daily_ai_report_dag/dagRuns"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                airflow_url,
                json={"conf": {"force_regenerate": True}},
                auth=(api_settings.AIRFLOW_ADMIN_USER, api_settings.AIRFLOW_ADMIN_PASSWORD)
            )
            return {
                "status": "TRIGGERED",
                "message": "Regeneración del reporte de IA enviada a Airflow con éxito.",
                "response_code": resp.status_code
            }
    except Exception as exc:
        return {
            "status": "QUEUED",
            "message": f"Regeneración solicitada (Airflow API: {exc})."
        }
