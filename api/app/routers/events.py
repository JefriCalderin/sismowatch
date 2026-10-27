"""
Router de Eventos Sísmicos.
Permite consultar eventos curados de gold.fact_earthquakes con filtrado multicriterio y paginación.
"""
from typing import Optional
from fastapi import APIRouter, Depends, Query
from api.app.models import PaginatedEventsResponse, EarthquakeEvent
from api.app.security import require_role
from api.app.database import query_db, query_db_one

router = APIRouter(prefix="/api", tags=["Eventos Sísmicos"])

@router.get("/events", response_model=PaginatedEventsResponse)
def get_events(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(20, ge=1, le=200, description="Registros por página"),
    min_magnitude: Optional[float] = Query(None, description="Magnitud mínima"),
    max_magnitude: Optional[float] = Query(None, description="Magnitud máxima"),
    region: Optional[str] = Query(None, description="Filtro de región o texto"),
    is_anomaly: Optional[bool] = Query(None, description="Filtrar solo anomalías"),
    tsunami_only: Optional[bool] = Query(None, description="Filtrar eventos con tsunami"),
    start_date: Optional[str] = Query(None, description="Fecha inicial YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Fecha final YYYY-MM-DD"),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Retorna la lista paginada de eventos sísmicos curados en la capa Gold con filtros aplicados.
    """
    where_clauses = ["1=1"]
    params = []

    if min_magnitude is not None:
        where_clauses.append("magnitude >= %s")
        params.append(min_magnitude)

    if max_magnitude is not None:
        where_clauses.append("magnitude <= %s")
        params.append(max_magnitude)

    if region:
        where_clauses.append("(region ILIKE %s OR place ILIKE %s)")
        params.extend([f"%{region}%", f"%{region}%"])

    if is_anomaly is not None:
        where_clauses.append("is_anomaly = %s")
        params.append(is_anomaly)

    if tsunami_only:
        where_clauses.append("tsunami_alert = TRUE")

    if start_date:
        where_clauses.append("event_date >= %s")
        params.append(start_date)

    if end_date:
        where_clauses.append("event_date <= %s")
        params.append(end_date)

    where_sql = " AND ".join(where_clauses)

    # Conteo total para paginación (parámetros seguros con %s)
    count_query = f"SELECT COUNT(*) AS total FROM gold.fact_earthquakes WHERE {where_sql};"  # nosec B608
    total_row = query_db_one(count_query, tuple(params))
    total_count = int(total_row.get("total", 0))

    offset = (page - 1) * page_size
    query = f"""
        SELECT
            id, magnitude, place, region, event_time, updated_time,
            longitude, latitude, depth_km, tsunami_alert, significance,
            magnitude_category, depth_category, is_anomaly, anomaly_score,
            anomaly_reason, url
        FROM gold.fact_earthquakes
        WHERE {where_sql}
        ORDER BY event_time DESC
        LIMIT %s OFFSET %s;
    """  # nosec B608
    query_params = tuple(params + [page_size, offset])
    rows = query_db(query, query_params)

    events = [EarthquakeEvent(**row) for row in rows]
    return PaginatedEventsResponse(
        total=total_count,
        page=page,
        page_size=page_size,
        events=events
    )
