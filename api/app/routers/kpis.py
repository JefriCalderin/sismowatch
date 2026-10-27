"""
Router de Indicadores Clave de Rendimiento (KPIs).
Consume exclusivamente la capa Gold (gold.fact_earthquakes y gold.quality_metrics).
"""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from api.app.models import KPIResponse
from api.app.security import require_role
from api.app.database import query_db_one

router = APIRouter(prefix="/api", tags=["Dashboard KPIs"])

@router.get("/kpis", response_model=KPIResponse)
def get_kpis(current_user: dict = Depends(require_role(["viewer", "admin"]))):
    """
    Entrega los principales indicadores para el dashboard ejecutivo en tiempo real:
    - Eventos en las últimas 24 horas
    - Magnitud máxima y promedio
    - Alertas de tsunami y anomalías detectadas
    - Score de calidad de datos de la última ejecución
    - Hora de última actualización
    """
    # Consulta de KPIs de eventos en 24h
    kpi_query = """
        SELECT
            COUNT(*) AS events_24h,
            COALESCE(MAX(magnitude), 0.0) AS max_magnitude,
            COALESCE(ROUND(AVG(magnitude), 2), 0.0) AS avg_magnitude,
            COALESCE(SUM(CASE WHEN tsunami_alert THEN 1 ELSE 0 END), 0) AS tsunami_alerts,
            COALESCE(SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END), 0) AS anomalies_count,
            MAX(event_time) AS last_event_time,
            MAX(curated_at) AS last_update
        FROM gold.fact_earthquakes
        WHERE event_time >= (NOW() - INTERVAL '24 hours');
    """
    kpi_data = query_db_one(kpi_query)

    # Consulta de score de calidad más reciente
    quality_query = """
        SELECT quality_score, freshness_lag_seconds, execution_timestamp
        FROM gold.quality_metrics
        ORDER BY execution_timestamp DESC
        LIMIT 1;
    """
    quality_data = query_db_one(quality_query)

    score = float(quality_data.get("quality_score", 100.0))
    freshness = "Óptima" if quality_data.get("freshness_lag_seconds", 0) <= 7200 else "Con Retraso"

    last_up = kpi_data.get("last_update")
    last_up_str = last_up.isoformat() if last_up else datetime.now(timezone.utc).isoformat()

    return KPIResponse(
        events_24h=int(kpi_data.get("events_24h", 0)),
        max_magnitude=float(kpi_data.get("max_magnitude", 0.0)),
        avg_magnitude=float(kpi_data.get("avg_magnitude", 0.0)),
        tsunami_alerts=int(kpi_data.get("tsunami_alerts", 0)),
        anomalies_count=int(kpi_data.get("anomalies_count", 0)),
        quality_score=score,
        last_update=last_up_str,
        freshness_status=freshness
    )
