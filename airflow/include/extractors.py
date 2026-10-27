"""
Módulo de extracción de datos desde la API pública de USGS Earthquake Catalog.
Maneja reintentos con backoff exponencial, timeouts y almacenamiento idempotente en la capa Bronze.
"""
import json
import logging
import time
from typing import Dict, Any, List, Optional
import httpx
from airflow.include.config import settings
from airflow.include.db import execute_batch_upsert

logger = logging.getLogger(__name__)

def fetch_usgs_feed(feed_url: Optional[str] = None, max_retries: int = 3, timeout: float = 15.0) -> Dict[str, Any]:
    """
    Consulta el feed GeoJSON de USGS con política de reintentos crecientes.
    Si la API no responde tras agotar reintentos, genera un log explícito y lanza excepción controlada.
    """
    url = feed_url or settings.USGS_FEED_URL
    backoff = 2.0

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Consultando USGS feed (Intento {attempt}/{max_retries}): {url}")
            with httpx.Client(timeout=timeout) as client:
                response = client.get(url, headers={"User-Agent": "SismoWatch-DataPlatform/1.0"})
                response.raise_for_status()
                data = response.json()
                features = data.get("features", [])
                logger.info(f"Descarga exitosa: {len(features)} eventos obtenidos de USGS.")
                return data
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            logger.warning(f"Error al consultar USGS (Intento {attempt}/{max_retries}): {exc}")
            if attempt == max_retries:
                logger.error("Se agotaron los reintentos para consultar la API de USGS.")
                raise RuntimeError(f"Falla en conexión con USGS tras {max_retries} intentos: {exc}") from exc
            time.sleep(backoff)
            backoff *= 2.0

    return {"features": [], "metadata": {}}

def fetch_usgs_historical(start_date: str, end_date: str, min_magnitude: float = 2.5, timeout: float = 30.0) -> Dict[str, Any]:
    """
    Consulta histórica parametrizada al catálogo FDSNWS de USGS para backfill.
    """
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
        "minmagnitude": min_magnitude
    }
    url = settings.USGS_QUERY_URL
    logger.info(f"Ejecutando backfill histórico desde {start_date} hasta {end_date} (Min Mag: {min_magnitude})")

    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.get(url, params=params, headers={"User-Agent": "SismoWatch-DataPlatform/1.0"})
            response.raise_for_status()
            data = response.json()
            logger.info(f"Backfill histórico obtenido: {len(data.get('features', []))} eventos.")
            return data
    except Exception as exc:
        logger.error(f"Error consultando historial USGS: {exc}")
        raise RuntimeError(f"Fallo en backfill histórico USGS: {exc}") from exc

def save_raw_to_bronze(data: Dict[str, Any], source_feed: str = "usgs_feed") -> int:
    """
    Inserta o actualiza eventos en la capa Bronze (bronze.earthquakes_raw).
    Aplica rol de mínimo privilegio `role_ingest` y garantiza idempotencia (upsert por id).
    """
    features: List[Dict[str, Any]] = data.get("features", [])
    if not features:
        logger.info("No hay eventos sísmicos para insertar en Bronze.")
        return 0

    records_to_upsert = []
    for item in features:
        event_id = item.get("id")
        if not event_id:
            continue
        
        props = item.get("properties") or {}
        geom = item.get("geometry") or {}
        coords = geom.get("coordinates") or [None, None, None]

        lon = coords[0] if len(coords) > 0 else None
        lat = coords[1] if len(coords) > 1 else None
        depth = coords[2] if len(coords) > 2 else None

        records_to_upsert.append((
            event_id,
            props.get("mag"),
            props.get("place"),
            props.get("time"),
            props.get("updated"),
            props.get("tz"),
            props.get("url"),
            props.get("detail"),
            props.get("felt"),
            props.get("cdi"),
            props.get("mmi"),
            props.get("alert"),
            props.get("status"),
            props.get("tsunami"),
            props.get("sig"),
            props.get("net"),
            props.get("code"),
            props.get("ids"),
            props.get("sources"),
            props.get("types"),
            props.get("nst"),
            props.get("dmin"),
            props.get("rms"),
            props.get("gap"),
            props.get("magType"),
            props.get("type"),
            props.get("title"),
            geom.get("type"),
            lon,
            lat,
            depth,
            json.dumps(item),
            source_feed
        ))

    upsert_query = """
        INSERT INTO bronze.earthquakes_raw (
            id, properties_mag, properties_place, properties_time, properties_updated,
            properties_tz, properties_url, properties_detail, properties_felt, properties_cdi,
            properties_mmi, properties_alert, properties_status, properties_tsunami, properties_sig,
            properties_net, properties_code, properties_ids, properties_sources, properties_types,
            properties_nst, properties_dmin, properties_rms, properties_gap, properties_magType,
            properties_type, properties_title, geometry_type, longitude, latitude, depth_km,
            raw_payload, source_feed
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            properties_mag = EXCLUDED.properties_mag,
            properties_place = EXCLUDED.properties_place,
            properties_updated = EXCLUDED.properties_updated,
            properties_status = EXCLUDED.properties_status,
            properties_tsunami = EXCLUDED.properties_tsunami,
            properties_sig = EXCLUDED.properties_sig,
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
            depth_km = EXCLUDED.depth_km,
            raw_payload = EXCLUDED.raw_payload,
            ingestion_timestamp = CURRENT_TIMESTAMP;
    """

    # Ejecuta inserción con credenciales de role_ingest
    count = execute_batch_upsert(upsert_query, records_to_upsert, role="ingest")
    logger.info(f"Capa Bronze actualizada con éxito: {count} registros upserted.")

    # Sincronizar copia en MinIO Object Storage (bucket sismowatch-bronze)
    try:
        from airflow.include.storage_minio import upload_bronze_payload
        upload_bronze_payload(data, tag=source_feed)
    except Exception as minio_exc:
        logger.warning(f"Fallo no bloqueante al subir a MinIO Bronze: {minio_exc}")

    return count
