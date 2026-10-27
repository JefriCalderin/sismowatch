"""
Router de Estadísticas y Visualización para Dashboards.
Genera series temporales, distribución por categorías y concentración regional.
"""
from typing import List
from fastapi import APIRouter, Depends, Query
from api.app.models import TimeSeriesPoint, MagnitudeDistribution, RegionActivity
from api.app.security import require_role
from api.app.database import query_db

router = APIRouter(prefix="/api/stats", tags=["Estadísticas"])

@router.get("/timeseries", response_model=List[TimeSeriesPoint])
def get_timeseries(
    days: int = Query(7, ge=1, le=60, description="Días hacia atrás para serie de tiempo"),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Retorna la serie temporal horaria o diaria de eventos sísmicos y magnitud promedio.
    """
    query = """
        SELECT
            TO_CHAR(DATE_TRUNC('hour', event_time), 'YYYY-MM-DD HH24:00') AS time_bucket,
            COUNT(*) AS event_count,
            COALESCE(ROUND(AVG(magnitude), 2), 0.0) AS avg_magnitude,
            COALESCE(MAX(magnitude), 0.0) AS max_magnitude
        FROM gold.fact_earthquakes
        WHERE event_time >= (NOW() - INTERVAL '%s days')
        GROUP BY DATE_TRUNC('hour', event_time)
        ORDER BY DATE_TRUNC('hour', event_time) ASC;
    """
    rows = query_db(query, (days,))
    return [TimeSeriesPoint(**r) for r in rows]

@router.get("/distribution", response_model=List[MagnitudeDistribution])
def get_magnitude_distribution(current_user: dict = Depends(require_role(["viewer", "admin"]))):
    """
    Retorna la distribución de eventos según su clasificación de magnitud sísmica.
    """
    query = """
        SELECT
            magnitude_category AS category,
            COUNT(*) AS count
        FROM gold.fact_earthquakes
        GROUP BY magnitude_category
        ORDER BY count DESC;
    """
    rows = query_db(query)
    return [MagnitudeDistribution(**r) for r in rows]

@router.get("/regions", response_model=List[RegionActivity])
def get_top_regions(
    limit: int = Query(10, ge=1, le=50),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Retorna las regiones con mayor concentración de eventos sísmicos.
    """
    query = """
        SELECT
            region,
            COUNT(*) AS count
        FROM gold.fact_earthquakes
        WHERE region IS NOT NULL AND region <> ''
        GROUP BY region
        ORDER BY count DESC
        LIMIT %s;
    """
    rows = query_db(query, (limit,))
    return [RegionActivity(**r) for r in rows]
