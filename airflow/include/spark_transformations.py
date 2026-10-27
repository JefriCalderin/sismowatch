"""
Transformaciones de Arquitectura Medallón (Bronze -> Silver -> Gold).
Implementa desanidado, tipado, sanitización anti-inyección, calidad bloqueante y agregaciones analíticas.
Diseñado para operar mediante PySpark con fallback automático a motor Pandas/SQL optimizado.
"""
import re
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List
import pandas as pd
import numpy as np
from airflow.include.config import settings
from airflow.include.db import execute_query, execute_batch_upsert
from airflow.include.quality import run_data_quality_checks
from airflow.include.anomaly_detection import detect_anomalies_and_swarms, save_anomaly_alerts

logger = logging.getLogger(__name__)

# Sanitizador de cadenas contra XSS e inyecciones a LLM
def sanitize_text(text: Optional[str]) -> str:
    """
    Sanitiza cadenas de texto para prevenir XSS y ataques de Prompt Injection en LLMs.
    Elimina secuencias peligrosas como 'ignore previous instructions', caracteres de escape y tags HTML.
    """
    if not text:
        return "Ubicación desconocida"
    # Eliminar etiquetas HTML/XML
    clean = re.sub(r"<[^>]+>", "", str(text))
    # Neutralizar intentos de Prompt Injection y SQL DDL peligrosos
    clean = re.sub(r"(?i)\b(ignore (all )?previous instructions|system prompt|system:|assistant:|user:|drop table|truncate table|delete from)\b", "[REDACTED_PROMPT_INJECTION]", clean)
    # Reemplazar múltiples espacios y caracteres de control
    clean = re.sub(r"[\r\n\t]+", " ", clean)
    clean = re.sub(r"[^\w\s,\.\-\(\)\/\[\]]", "", clean)
    return clean.strip() or "Ubicación no especificada"

def extract_region(place: str) -> str:
    """Extrae la región o país principal a partir de la descripción geográfica de USGS."""
    if not place:
        return "Global / Océano"
    clean_p = sanitize_text(place)
    # En USGS el formato suele ser "X km DIRECTION of City, State/Country"
    if "," in clean_p:
        parts = clean_p.split(",")
        return parts[-1].strip()
    if " of " in clean_p.lower():
        parts = re.split(r"\bof\b", clean_p, flags=re.IGNORECASE)
        return parts[-1].strip()
    return clean_p.strip()

def categorize_magnitude(mag: float) -> str:
    """Asigna categoría estandarizada de magnitud sísmica (escala Richter/MMS)."""
    if mag < 3.0:
        return "Micro"
    elif mag < 4.0:
        return "Menor"
    elif mag < 5.0:
        return "Ligero"
    elif mag < 6.0:
        return "Moderado"
    elif mag < 7.0:
        return "Fuerte"
    elif mag < 8.0:
        return "Mayor"
    else:
        return "Gran Terremoto"

def categorize_depth(depth_km: float) -> str:
    """Clasifica la profundidad focal sísmica según sismología estándar."""
    if depth_km <= 70.0:
        return "Superficial"
    elif depth_km <= 300.0:
        return "Intermedio"
    else:
        return "Profundo"

def process_bronze_to_silver() -> int:
    """
    Transforma registros crudos de bronze.earthquakes_raw a silver.earthquakes_cleaned.
    Realiza tipado estricto, desempaquetado de coordenadas, conversión temporal y sanitización.
    """
    logger.info("Iniciando transformación Medallón: Bronze -> Silver")
    
    # Extraer de Bronze registros pendientes o modificados recientemente
    select_query = """
        SELECT
            id, properties_mag, properties_place, properties_time, properties_updated,
            properties_tsunami, properties_sig, properties_status, properties_magType,
            properties_type, properties_url, longitude, latitude, depth_km
        FROM bronze.earthquakes_raw
        ORDER BY properties_time DESC;
    """
    rows = execute_query(select_query, role="transform", fetch=True)
    if not rows:
        logger.info("Bronze está vacío o no contiene registros.")
        return 0

    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]

    # Limpieza y tipado
    df["magnitude"] = pd.to_numeric(df["properties_mag"], errors="coerce").fillna(0.0)
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").fillna(0.0)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").fillna(0.0)
    df["depth_km"] = pd.to_numeric(df["depth_km"], errors="coerce").fillna(0.0)
    df["place"] = df["properties_place"].fillna("Ubicación desconocida").astype(str)
    df["sanitized_place"] = df["place"].apply(sanitize_text)
    df["region"] = df["sanitized_place"].apply(extract_region)

    # Conversión de milisegundos epoch a datetime UTC
    df["event_time"] = pd.to_datetime(df["properties_time"].fillna(0), unit="ms", utc=True)
    df["updated_time"] = pd.to_datetime(df["properties_updated"].fillna(0), unit="ms", utc=True)

    df["tsunami_alert"] = df["properties_tsunami"].fillna(0).astype(int) > 0
    df["significance"] = pd.to_numeric(df["properties_sig"], errors="coerce").fillna(0).astype(int)
    df["status"] = df["properties_status"].fillna("automatic").astype(str)
    df["magnitude_type"] = df["properties_magtype"].fillna("unknown").astype(str)
    df["event_type"] = df["properties_type"].fillna("earthquake").astype(str)
    df["url"] = df["properties_url"].fillna("").astype(str)

    df["magnitude_category"] = df["magnitude"].apply(categorize_magnitude)
    df["depth_category"] = df["depth_km"].apply(categorize_depth)
    df["is_valid"] = True

    # Preparar lote para Upsert en Silver
    records = []
    for _, r in df.iterrows():
        records.append((
            r["id"],
            float(r["magnitude"]),
            r["place"],
            r["sanitized_place"],
            r["event_time"].to_pydatetime(),
            r["updated_time"].to_pydatetime(),
            float(r["longitude"]),
            float(r["latitude"]),
            float(r["depth_km"]),
            bool(r["tsunami_alert"]),
            int(r["significance"]),
            r["status"],
            r["magnitude_type"],
            r["event_type"],
            r["url"],
            r["region"],
            r["magnitude_category"],
            r["depth_category"],
            bool(r["is_valid"])
        ))

    upsert_query = """
        INSERT INTO silver.earthquakes_cleaned (
            id, magnitude, place, sanitized_place, event_time, updated_time,
            longitude, latitude, depth_km, tsunami_alert, significance, status,
            magnitude_type, event_type, url, region, magnitude_category,
            depth_category, is_valid
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            magnitude = EXCLUDED.magnitude,
            place = EXCLUDED.place,
            sanitized_place = EXCLUDED.sanitized_place,
            event_time = EXCLUDED.event_time,
            updated_time = EXCLUDED.updated_time,
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
            depth_km = EXCLUDED.depth_km,
            tsunami_alert = EXCLUDED.tsunami_alert,
            significance = EXCLUDED.significance,
            status = EXCLUDED.status,
            region = EXCLUDED.region,
            magnitude_category = EXCLUDED.magnitude_category,
            depth_category = EXCLUDED.depth_category,
            silver_processed_at = CURRENT_TIMESTAMP;
    """

    count = execute_batch_upsert(upsert_query, records, role="transform")
    logger.info(f"Capa Silver actualizada exitosamente: {count} registros procesados.")

    # Sincronización en MinIO Object Storage (bucket sismowatch-silver)
    try:
        from airflow.include.storage_minio import upload_silver_dataset
        # Convertir fechas a strings para serialización JSON
        silver_records = []
        for _, row in df.iterrows():
            item = row.to_dict()
            for k, v in item.items():
                if hasattr(v, "isoformat"):
                    item[k] = v.isoformat()
            silver_records.append(item)
        upload_silver_dataset(silver_records)
    except Exception as minio_exc:
        logger.warning(f"Fallo no bloqueante sincronizando MinIO Silver: {minio_exc}")

    return count

def process_silver_to_gold(pipeline_run_id: str = "manual_run") -> int:
    """
    Transforma registros validados de silver.earthquakes_cleaned a gold.fact_earthquakes.
    Aplica compuerta de calidad estricta (Data Quality Gate con Pandera).
    Si no pasa la calidad, se bloquea la publicación.
    Detecta anomalías y enjambres con ML y genera agregaciones para gold.daily_metrics.
    """
    logger.info(f"Iniciando transformación Medallón: Silver -> Gold (Run ID: {pipeline_run_id})")

    select_query = """
        SELECT
            id, magnitude, place, sanitized_place, event_time, updated_time,
            longitude, latitude, depth_km, tsunami_alert, significance, status,
            magnitude_type, event_type, url, region, magnitude_category, depth_category
        FROM silver.earthquakes_cleaned;
    """
    rows = execute_query(select_query, role="transform", fetch=True)
    if not rows:
        logger.info("Silver está vacío. Nada que publicar en Gold.")
        return 0

    df = pd.DataFrame(rows)

    # 1. Ejecutar Compuerta de Calidad de Datos
    is_passed, valid_df, quality_report = run_data_quality_checks(df, pipeline_run_id)

    if not is_passed:
        err_msg = (
            f"COMPUERTA DE CALIDAD BLOQUEADA: Puntaje de calidad ({quality_report['score']}%) "
            f"es inferior al mínimo permitido ({settings.QUALITY_MIN_SCORE}%). "
            f"No se cargará ningún dato a Gold."
        )
        logger.critical(err_msg)
        raise RuntimeError(err_msg)

    if valid_df.empty:
        logger.warning("No hay registros válidos tras la validación de calidad.")
        return 0

    # 2. Detección de Anomalías con ML y Enjambres Sísmicos
    enriched_df, alerts = detect_anomalies_and_swarms(valid_df)

    # Guardar alertas en gold.anomaly_alerts
    save_anomaly_alerts(alerts)

    # Extraer componentes de fecha y hora para dimensionamiento
    enriched_df["event_date"] = pd.to_datetime(enriched_df["event_time"]).dt.date
    enriched_df["event_hour"] = pd.to_datetime(enriched_df["event_time"]).dt.hour

    # 3. Upsert en gold.fact_earthquakes
    records = []
    for _, r in enriched_df.iterrows():
        records.append((
            r["id"],
            float(r["magnitude"]),
            r["sanitized_place"],
            r["region"],
            pd.to_datetime(r["event_time"]).to_pydatetime(),
            pd.to_datetime(r["updated_time"]).to_pydatetime(),
            r["event_date"],
            int(r["event_hour"]),
            float(r["longitude"]),
            float(r["latitude"]),
            float(r["depth_km"]),
            bool(r["tsunami_alert"]),
            int(r["significance"]),
            r["magnitude_category"],
            r["depth_category"],
            bool(r["is_anomaly"]),
            float(r["anomaly_score"]),
            r["anomaly_reason"],
            r["url"]
        ))

    upsert_gold_query = """
        INSERT INTO gold.fact_earthquakes (
            id, magnitude, place, region, event_time, updated_time, event_date, event_hour,
            longitude, latitude, depth_km, tsunami_alert, significance, magnitude_category,
            depth_category, is_anomaly, anomaly_score, anomaly_reason, url
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            magnitude = EXCLUDED.magnitude,
            place = EXCLUDED.place,
            region = EXCLUDED.region,
            updated_time = EXCLUDED.updated_time,
            depth_km = EXCLUDED.depth_km,
            tsunami_alert = EXCLUDED.tsunami_alert,
            significance = EXCLUDED.significance,
            is_anomaly = EXCLUDED.is_anomaly,
            anomaly_score = EXCLUDED.anomaly_score,
            anomaly_reason = EXCLUDED.anomaly_reason,
            curated_at = CURRENT_TIMESTAMP;
    """
    gold_count = execute_batch_upsert(upsert_gold_query, records, role="transform")

    # 4. Actualizar Métricas Agregadas en gold.daily_metrics
    _refresh_daily_metrics()

    # Sincronización en MinIO Object Storage (bucket sismowatch-gold)
    try:
        from airflow.include.storage_minio import upload_gold_dataset
        gold_records = []
        for _, row in enriched_df.iterrows():
            item = row.to_dict()
            for k, v in item.items():
                if hasattr(v, "isoformat"):
                    item[k] = v.isoformat()
            gold_records.append(item)
        upload_gold_dataset(gold_records)
    except Exception as minio_exc:
        logger.warning(f"Fallo no bloqueante sincronizando MinIO Gold: {minio_exc}")

    logger.info(f"Capa Gold publicada exitosamente: {gold_count} registros curados.")
    return gold_count

def _refresh_daily_metrics() -> None:
    """Calcula y actualiza los agregados diarios en gold.daily_metrics."""
    query = """
        INSERT INTO gold.daily_metrics (
            metric_date, total_events, max_magnitude, avg_magnitude, min_magnitude,
            tsunami_events, anomalous_events, avg_depth_km, top_active_region, updated_at
        )
        SELECT
            event_date AS metric_date,
            COUNT(*) AS total_events,
            COALESCE(MAX(magnitude), 0.0) AS max_magnitude,
            COALESCE(ROUND(AVG(magnitude), 2), 0.0) AS avg_magnitude,
            COALESCE(MIN(magnitude), 0.0) AS min_magnitude,
            COALESCE(SUM(CASE WHEN tsunami_alert THEN 1 ELSE 0 END), 0) AS tsunami_events,
            COALESCE(SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END), 0) AS anomalous_events,
            COALESCE(ROUND(AVG(depth_km)::numeric, 2), 0.0) AS avg_depth_km,
            MODE() WITHIN GROUP (ORDER BY region) AS top_active_region,
            CURRENT_TIMESTAMP AS updated_at
        FROM gold.fact_earthquakes
        GROUP BY event_date
        ON CONFLICT (metric_date) DO UPDATE SET
            total_events = EXCLUDED.total_events,
            max_magnitude = EXCLUDED.max_magnitude,
            avg_magnitude = EXCLUDED.avg_magnitude,
            min_magnitude = EXCLUDED.min_magnitude,
            tsunami_events = EXCLUDED.tsunami_events,
            anomalous_events = EXCLUDED.anomalous_events,
            avg_depth_km = EXCLUDED.avg_depth_km,
            top_active_region = EXCLUDED.top_active_region,
            updated_at = CURRENT_TIMESTAMP;
    """
    execute_query(query, role="transform")
    logger.info("Métricas diarias en gold.daily_metrics actualizadas.")
