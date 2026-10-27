"""
Router de Eventos Sísmicos.
Permite consultar eventos curados de gold.fact_earthquakes con filtrado multicriterio y paginación,
así como consultas históricas directas a la API oficial del USGS (United States Geological Survey).
"""
import html
import re
import calendar
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import httpx
from fastapi import APIRouter, Depends, Query, HTTPException, status
from api.app.models import PaginatedEventsResponse, EarthquakeEvent
from api.app.security import require_role
from api.app.database import query_db, query_db_one

router = APIRouter(prefix="/api", tags=["Eventos Sísmicos"])

def sanitize_text(text: Optional[str]) -> str:
    """Sanitiza descripciones de ubicación previniendo XSS e inyecciones."""
    if not text:
        return "Ubicación no especificada"
    cleaned = html.escape(text.strip())
    cleaned = re.sub(r"[<>{}\[\];'\"]", "", cleaned)
    return cleaned[:250]

def extract_region(place: str) -> str:
    """Extrae la región o estado de una cadena descriptiva de ubicación."""
    if not place:
        return "Global"
    if " of " in place:
        parts = place.split(" of ", 1)
        if len(parts) > 1 and parts[1].strip():
            return parts[1].strip()[:100]
    if ", " in place:
        parts = place.split(", ")
        return parts[-1].strip()[:100]
    return place[:100]

def categorize_magnitude(mag: float) -> str:
    """Categorización sismológica estándar."""
    if mag < 3.0: return "Micro"
    if mag < 4.5: return "Menor"
    if mag < 6.0: return "Moderado"
    if mag < 7.5: return "Fuerte"
    return "Mayor"

def categorize_depth(depth: float) -> str:
    """Categorización sismológica por profundidad focal."""
    if depth <= 70.0: return "Superficial"
    if depth <= 300.0: return "Intermedio"
    return "Profundo"

@router.get("/events/usgs-query")
async def query_usgs_historical(
    search_type: str = Query("last_n_days", description="specific_day | custom_range | full_month | full_year | last_n_days"),
    target_date: Optional[str] = Query(None, description="Fecha YYYY-MM-DD para día específico"),
    start_date: Optional[str] = Query(None, description="Fecha YYYY-MM-DD para rango"),
    end_date: Optional[str] = Query(None, description="Fecha YYYY-MM-DD para rango"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Mes (1-12)"),
    year: Optional[int] = Query(None, ge=1900, le=2100, description="Año"),
    last_days: Optional[int] = Query(7, ge=1, le=3650, description="Últimos N días"),
    min_magnitude: float = Query(2.5, description="Magnitud mínima"),
    orderby: str = Query("time", description="time | time-asc | magnitude | magnitude-asc"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(20, ge=1, le=200, description="Registros por página"),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Consulta dinámica e histórica directamente contra el endpoint oficial del USGS:
    https://earthquake.usgs.gov/fdsnws/event/1/query
    
    Soporta los 5 tipos de consulta solicitados:
      1. Día específico (24 horas)
      2. Rango personalizado (Desde - Hasta)
      3. Mes completo (Mes y Año)
      4. Año completo (Año)
      5. Últimos N días (7, 30, 90, 365, etc.)
    """
    now_utc = datetime.now(timezone.utc)

    # 1. Resolver fechas de inicio y fin en formato ISO-8601 estricto para USGS
    try:
        if search_type == "specific_day":
            t_date = target_date or (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
            # Validar formato de fecha
            datetime.strptime(t_date, "%Y-%m-%d")
            starttime = f"{t_date}T00:00:00"
            endtime = f"{t_date}T23:59:59"

        elif search_type == "custom_range":
            s_date = start_date or (now_utc - timedelta(days=7)).strftime("%Y-%m-%d")
            e_date = end_date or now_utc.strftime("%Y-%m-%d")
            datetime.strptime(s_date, "%Y-%m-%d")
            datetime.strptime(e_date, "%Y-%m-%d")
            starttime = f"{s_date}T00:00:00"
            endtime = f"{e_date}T23:59:59"

        elif search_type == "full_month":
            y = year or now_utc.year
            m = month or now_utc.month
            last_day = calendar.monthrange(y, m)[1]
            starttime = f"{y:04d}-{m:02d}-01T00:00:00"
            endtime = f"{y:04d}-{m:02d}-{last_day:02d}T23:59:59"

        elif search_type == "full_year":
            y = year or now_utc.year
            starttime = f"{y:04d}-01-01T00:00:00"
            endtime = f"{y:04d}-12-31T23:59:59"

        elif search_type == "last_n_days":
            days = last_days or 7
            start_dt = now_utc - timedelta(days=days)
            starttime = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
            endtime = now_utc.strftime("%Y-%m-%dT%H:%M:%S")

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Tipo de búsqueda '{search_type}' no reconocido."
            )
    except ValueError as val_err:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Formato de fecha inválido. Se requiere YYYY-MM-DD: {val_err}"
        )

    # 2. Configurar parámetros para la API oficial de USGS
    usgs_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    usgs_params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime,
        "minmagnitude": min_magnitude,
        "orderby": "time" if "time" in orderby else "magnitude",
        "limit": 20000
    }

    # 3. Invocar API de USGS de forma asíncrona con reintentos y fallback resiliente a Gold
    data = None
    network_error_msg = None
    source_label = "USGS Official Earthquake Catalog (fdsnws/event/1/query)"

    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=25.0) as client:
                resp = await client.get(usgs_url, params=usgs_params)
                if resp.status_code == 200:
                    data = resp.json()
                    break
                elif resp.status_code != 200:
                    network_error_msg = f"Error retornado por la API del USGS ({resp.status_code}): {resp.text[:200]}"
                    break
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            network_error_msg = str(exc)
            if attempt == 0:
                import asyncio
                await asyncio.sleep(0.5)

    parsed_events = []
    map_markers = []

    if not data or "features" not in data:
        # Fallback resiliente: consultar catálogo Gold local
        source_label = "Catálogo Histórico Gold (Respaldo Local por Indisponibilidad de Red USGS)"
        db_sql = """
            SELECT id, magnitude, place, region, event_time, updated_time,
                   longitude, latitude, depth_km, tsunami_alert, significance,
                   magnitude_category, depth_category, is_anomaly, anomaly_score,
                   anomaly_reason, url
            FROM gold.fact_earthquakes
            WHERE event_time >= %s AND event_time <= %s AND magnitude >= %s
            ORDER BY event_time DESC
            LIMIT 5000;
        """
        db_rows = query_db(db_sql, (starttime, endtime, min_magnitude)) or []

        if db_rows:
            for r in db_rows:
                ev_time = r["event_time"].isoformat() if hasattr(r["event_time"], "isoformat") else str(r["event_time"])
                up_time = r["updated_at"].isoformat() if hasattr(r.get("updated_at"), "isoformat") else ev_time
                ev_dict = {
                    "id": str(r["id"]),
                    "magnitude": float(r["magnitude"]),
                    "place": str(r["place"]),
                    "region": str(r["region"] or "Global"),
                    "event_time": ev_time,
                    "updated_time": up_time,
                    "longitude": float(r.get("longitude") or 0.0),
                    "latitude": float(r.get("latitude") or 0.0),
                    "depth_km": float(r.get("depth_km") or 0.0),
                    "tsunami_alert": bool(r.get("tsunami_alert", False)),
                    "significance": int(r.get("significance") or 0),
                    "magnitude_category": str(r.get("magnitude_category") or "Desconocido"),
                    "depth_category": str(r.get("depth_category") or "Desconocido"),
                    "is_anomaly": bool(r.get("is_anomaly", False)),
                    "anomaly_score": float(r.get("anomaly_score") or 0.0),
                    "anomaly_reason": r.get("anomaly_reason"),
                    "url": r.get("url") or f"https://earthquake.usgs.gov/earthquakes/eventpage/{r['id']}"
                }
                parsed_events.append(ev_dict)
                if len(map_markers) < 1000:
                    map_markers.append({
                        "id": str(r["id"]),
                        "magnitude": float(r["magnitude"]),
                        "place": str(r["place"]),
                        "region": str(r["region"] or "Global"),
                        "latitude": float(r.get("latitude") or 0.0),
                        "longitude": float(r.get("longitude") or 0.0),
                        "depth_km": float(r.get("depth_km") or 0.0),
                        "time": ev_time,
                        "url": ev_dict["url"]
                    })
        elif network_error_msg:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error de conexión con los servidores del USGS: {network_error_msg}"
            )
    else:
        # 4. Procesar y sanitizar resultados de USGS
        features = data.get("features", [])
        for f in features:
            props = f.get("properties") or {}
            geom = f.get("geometry") or {}
            coords = geom.get("coordinates") or [0.0, 0.0, 0.0]

            t_ms = props.get("time") or 0
            u_ms = props.get("updated") or t_ms
            event_time = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)
            updated_time = datetime.fromtimestamp(u_ms / 1000.0, tz=timezone.utc)

            mag = float(props.get("mag") or 0.0)
            lon = float(coords[0]) if len(coords) > 0 else 0.0
            lat = float(coords[1]) if len(coords) > 1 else 0.0
            depth = float(coords[2]) if len(coords) > 2 else 0.0

            place_clean = sanitize_text(props.get("place"))
            region = extract_region(place_clean)
            event_id = f.get("id") or f"usgs_{t_ms}"

            event_dict = {
                "id": event_id,
                "magnitude": round(mag, 2),
                "place": place_clean,
                "region": region,
                "event_time": event_time.isoformat(),
                "updated_time": updated_time.isoformat(),
                "longitude": round(lon, 4),
                "latitude": round(lat, 4),
                "depth_km": round(depth, 2),
                "tsunami_alert": bool(props.get("tsunami", 0)),
                "significance": int(props.get("sig") or 0),
                "magnitude_category": categorize_magnitude(mag),
                "depth_category": categorize_depth(depth),
                "is_anomaly": False,
                "anomaly_score": 0.0,
                "anomaly_reason": None,
                "url": props.get("url") or f"https://earthquake.usgs.gov/earthquakes/eventpage/{event_id}"
            }
            parsed_events.append(event_dict)

            if len(map_markers) < 1000:
                map_markers.append({
                    "id": event_id,
                    "magnitude": round(mag, 2),
                    "place": place_clean,
                    "region": region,
                    "latitude": round(lat, 4),
                    "longitude": round(lon, 4),
                    "depth_km": round(depth, 2),
                    "time": event_time.isoformat(),
                    "url": event_dict["url"]
                })

    # 5. Ordenamiento según parámetro solicitado
    if orderby == "time-asc":
        parsed_events.sort(key=lambda x: x["event_time"])
    elif orderby == "magnitude":
        parsed_events.sort(key=lambda x: x["magnitude"], reverse=True)
    elif orderby == "magnitude-asc":
        parsed_events.sort(key=lambda x: x["magnitude"])
    else:  # time (más recientes primero)
        parsed_events.sort(key=lambda x: x["event_time"], reverse=True)

    total_count = len(parsed_events)
    offset = (page - 1) * page_size
    paged_events = parsed_events[offset : offset + page_size]

    return {
        "source": source_label,
        "search_type": search_type,
        "period": {
            "starttime": starttime,
            "endtime": endtime,
            "min_magnitude": min_magnitude,
            "orderby": orderby
        },
        "total": total_count,
        "page": page,
        "page_size": page_size,
        "events": paged_events,
        "map_markers": map_markers
    }

@router.get("/events/today")
def get_today_events(
    limit: int = Query(50, ge=1, le=100, description="Límite de eventos recientes de hoy"),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Retorna los eventos sísmicos registrados el día de hoy (UTC / últimas 24 horas)
    directamente desde la capa Gold para el monitoreo en tiempo real.
    """
    query = """
        SELECT
            id, magnitude, place, region, event_time, updated_time,
            longitude, latitude, depth_km, tsunami_alert, significance,
            magnitude_category, depth_category, is_anomaly, anomaly_score,
            anomaly_reason, url
        FROM gold.fact_earthquakes
        WHERE event_time >= CURRENT_DATE
           OR event_time >= (NOW() - INTERVAL '24 hours')
        ORDER BY event_time DESC
        LIMIT %s;
    """
    rows = query_db(query, (limit,))
    count_row = query_db_one("""
        SELECT COUNT(*) as total 
        FROM gold.fact_earthquakes 
        WHERE event_time >= CURRENT_DATE 
           OR event_time >= (NOW() - INTERVAL '24 hours');
    """)
    total = int(count_row.get("total", len(rows)))
    return {
        "source": "gold.fact_earthquakes (Tiempo Real / Hoy)",
        "total": total,
        "events": rows
    }

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
    order_by: Optional[str] = Query("time", description="Ordenamiento: 'time' (recientes) o 'magnitude' (mayor magnitud)"),
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
    order_sql = "ORDER BY magnitude DESC, event_time DESC" if order_by == "magnitude" else "ORDER BY event_time DESC"
    query = f"""
        SELECT
            id, magnitude, place, region, event_time, updated_time,
            longitude, latitude, depth_km, tsunami_alert, significance,
            magnitude_category, depth_category, is_anomaly, anomaly_score,
            anomaly_reason, url
        FROM gold.fact_earthquakes
        WHERE {where_sql}
        {order_sql}
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
