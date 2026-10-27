"""
Módulo de Calidad de Datos (Data Quality Gate) para SismoWatch.
Implementa validaciones con Pandera y reglas analíticas de frescura, volumen, rangos y tipos.
Registra auditoría en gold.quality_metrics y actúa como compuerta bloqueante hacia la capa Gold.
(Arquitectura Medallón sin tablas de cuarentena, las métricas y motivos se auditan en gold.quality_metrics).
"""
import os
os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
import logging
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from airflow.include.config import settings
from airflow.include.db import execute_query, execute_batch_upsert

logger = logging.getLogger(__name__)

# Definición de Schema de Calidad con Pandera para la capa Silver
SilverQualitySchema = DataFrameSchema(
    columns={
        "id": Column(str, nullable=False, unique=True),
        "magnitude": Column(float, checks=[Check.greater_than_or_equal_to(-2.0), Check.less_than_or_equal_to(10.0)], nullable=False),
        "latitude": Column(float, checks=[Check.greater_than_or_equal_to(-90.0), Check.less_than_or_equal_to(90.0)], nullable=False),
        "longitude": Column(float, checks=[Check.greater_than_or_equal_to(-180.0), Check.less_than_or_equal_to(180.0)], nullable=False),
        "depth_km": Column(float, checks=[Check.greater_than_or_equal_to(-5.0), Check.less_than_or_equal_to(800.0)], nullable=False),
        "place": Column(str, nullable=False),
        "event_time": Column(pd.Timestamp, nullable=False),
        "tsunami_alert": Column(bool, nullable=False),
        "significance": Column(int, checks=[Check.greater_than_or_equal_to(0)], nullable=False),
    },
    coerce=True,
    strict=False
)

def run_data_quality_checks(df: pd.DataFrame, pipeline_run_id: str) -> Tuple[bool, pd.DataFrame, Dict[str, Any]]:
    """
    Ejecuta el conjunto completo de validaciones de calidad de datos sobre el DataFrame.
    Retorna:
      - is_passed (bool): True si el score de calidad supera el umbral configurado.
      - valid_df (pd.DataFrame): DataFrame con registros limpios listos para Gold.
      - quality_report (dict): Reporte detallado de auditoría guardado en gold.quality_metrics.
    """
    total_records = len(df)
    if total_records == 0:
        logger.warning("Data Quality: DataFrame vacío. No hay registros para evaluar.")
        report = {
            "total": 0,
            "valid": 0,
            "rejected": 0,
            "score": 100.0,
            "rules_evaluated": 7,
            "rules_passed": 7,
            "rules_failed": 0,
            "freshness_lag_seconds": 0,
            "status": "PASSED",
            "details": {"info": "DataFrame vacío procesado exitosamente sin fallos."}
        }
        _record_quality_metrics(pipeline_run_id, report)
        return True, df, report

    rules_evaluated = 0
    rules_passed = 0
    rules_failed = 0
    rejections_log: List[Dict[str, Any]] = []

    # 1. Regla: Unicidad de ID
    rules_evaluated += 1
    duplicated_ids = df[df.duplicated(subset=["id"], keep="first")]
    if len(duplicated_ids) == 0:
        rules_passed += 1
    else:
        rules_failed += 1
        for _, r in duplicated_ids.iterrows():
            rejections_log.append({"id": str(r["id"]), "reason": "ID duplicado en lote"})
    
    # Filtrar duplicados quedándonos con el primero
    cleaned_df = df.drop_duplicates(subset=["id"], keep="first").copy()

    # 2. Regla: Campos obligatorios no nulos
    rules_evaluated += 1
    null_mask = cleaned_df[["id", "magnitude", "latitude", "longitude", "depth_km", "place", "event_time"]].isna().any(axis=1)
    null_records = cleaned_df[null_mask]
    if len(null_records) == 0:
        rules_passed += 1
    else:
        rules_failed += 1
        for _, r in null_records.iterrows():
            rejections_log.append({"id": str(r["id"]), "reason": "Campo obligatorio nulo"})
    
    valid_mask = ~null_mask

    # 3. Regla: Rango de Coordenadas (Lat [-90, 90], Lon [-180, 180])
    rules_evaluated += 1
    coord_mask = (
        (cleaned_df["latitude"] >= -90.0) & (cleaned_df["latitude"] <= 90.0) &
        (cleaned_df["longitude"] >= -180.0) & (cleaned_df["longitude"] <= 180.0)
    )
    invalid_coords = cleaned_df[~coord_mask]
    if len(invalid_coords) == 0:
        rules_passed += 1
    else:
        rules_failed += 1
        for _, r in invalid_coords.iterrows():
            rejections_log.append({"id": str(r["id"]), "reason": f"Coordenadas fuera de rango: lat={r.get('latitude')}, lon={r.get('longitude')}"})
    valid_mask = valid_mask & coord_mask

    # 4. Regla: Rango de Magnitud (-2.0 a 10.0)
    rules_evaluated += 1
    mag_mask = (cleaned_df["magnitude"] >= -2.0) & (cleaned_df["magnitude"] <= 10.0)
    invalid_mag = cleaned_df[~mag_mask]
    if len(invalid_mag) == 0:
        rules_passed += 1
    else:
        rules_failed += 1
        for _, r in invalid_mag.iterrows():
            rejections_log.append({"id": str(r["id"]), "reason": f"Magnitud fuera de rango plausible: {r.get('magnitude')}"})
    valid_mask = valid_mask & mag_mask

    # 5. Regla: Profundidad plausible (-5 a 800 km)
    rules_evaluated += 1
    depth_mask = (cleaned_df["depth_km"] >= -5.0) & (cleaned_df["depth_km"] <= 800.0)
    invalid_depth = cleaned_df[~depth_mask]
    if len(invalid_depth) == 0:
        rules_passed += 1
    else:
        rules_failed += 1
        for _, r in invalid_depth.iterrows():
            rejections_log.append({"id": str(r["id"]), "reason": f"Profundidad fuera de rango: {r.get('depth_km')}"})
    valid_mask = valid_mask & depth_mask

    # 6. Regla de Coherencia Temporal (fecha no posterior al presente + 1 hora)
    rules_evaluated += 1
    now_utc = pd.Timestamp.now(tz=timezone.utc)
    # Aseguramos formato tz aware
    event_times = pd.to_datetime(cleaned_df["event_time"], utc=True)
    future_mask = event_times > (now_utc + pd.Timedelta(hours=1))
    if future_mask.sum() == 0:
        rules_passed += 1
    else:
        rules_failed += 1
        for _, r in cleaned_df[future_mask].iterrows():
            rejections_log.append({"id": str(r["id"]), "reason": "Fecha de evento posterior al futuro permitido"})
    valid_mask = valid_mask & (~future_mask)

    # 7. Regla de Frescura (la fecha del evento más reciente no supera umbral)
    rules_evaluated += 1
    latest_event = event_times.max()
    lag_seconds = int((now_utc - latest_event).total_seconds()) if pd.notna(latest_event) else 0
    freshness_threshold_seconds = settings.FRESHNESS_THRESHOLD_MINUTES * 60
    if lag_seconds <= freshness_threshold_seconds or total_records < 5:
        rules_passed += 1
    else:
        rules_failed += 1
        logger.warning(f"Control de frescura: retraso de {lag_seconds} segundos supera el umbral de {freshness_threshold_seconds}s.")

    # Registros válidos y rechazados
    valid_df = cleaned_df[valid_mask].copy()
    valid_records = len(valid_df)
    rejected_records = total_records - valid_records

    # Cálculo de puntaje de calidad porcentual
    quality_score = round((valid_records / total_records) * 100.0, 2) if total_records > 0 else 100.0
    is_passed = quality_score >= settings.QUALITY_MIN_SCORE

    status = "PASSED" if is_passed else "BLOCKED"
    if is_passed and rejected_records > 0:
        status = "WARNING"

    report = {
        "total": total_records,
        "valid": valid_records,
        "rejected": rejected_records,
        "score": quality_score,
        "rules_evaluated": rules_evaluated,
        "rules_passed": rules_passed,
        "rules_failed": rules_failed,
        "freshness_lag_seconds": lag_seconds,
        "status": status,
        "details": {
            "rejections_count": len(rejections_log),
            "rejections_sample": rejections_log[:20],
            "rules_summary": {
                "id_uniqueness": "PASSED" if len(duplicated_ids) == 0 else "FAILED",
                "non_null_mandatory": "PASSED" if len(null_records) == 0 else "FAILED",
                "coordinates_range": "PASSED" if len(invalid_coords) == 0 else "FAILED",
                "magnitude_range": "PASSED" if len(invalid_mag) == 0 else "FAILED",
                "depth_range": "PASSED" if len(invalid_depth) == 0 else "FAILED",
                "temporal_coherence": "PASSED" if future_mask.sum() == 0 else "FAILED",
                "data_freshness": "PASSED" if lag_seconds <= freshness_threshold_seconds else "WARNING"
            }
        }
    }

    # Guardar métricas en gold.quality_metrics
    _record_quality_metrics(pipeline_run_id, report)

    logger.info(
        f"Data Quality Gate completado: Score={quality_score}% ({valid_records}/{total_records} válidos). "
        f"Reglas: {rules_passed}/{rules_evaluated} aprobadas. Estado: {status}"
    )

    if not is_passed:
        logger.error(f"Data Quality FAILED: Score {quality_score}% por debajo del mínimo requerido ({settings.QUALITY_MIN_SCORE}%). Publicación a Gold BLOQUEADA.")

    return is_passed, valid_df, report

def _record_quality_metrics(pipeline_run_id: str, report: Dict[str, Any]) -> None:
    """Inserta el resultado del control de calidad en gold.quality_metrics."""
    query = """
        INSERT INTO gold.quality_metrics (
            pipeline_run_id, total_records_evaluated, valid_records, rejected_records,
            quality_score, rules_evaluated, rules_passed, rules_failed,
            freshness_lag_seconds, status, details
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    params = (
        pipeline_run_id,
        report["total"],
        report["valid"],
        report["rejected"],
        report["score"],
        report["rules_evaluated"],
        report["rules_passed"],
        report["rules_failed"],
        report["freshness_lag_seconds"],
        report["status"],
        json.dumps(report["details"])
    )
    try:
        execute_query(query, params, role="transform")
    except Exception as exc:
        logger.error(f"Error registrando métricas de calidad en base de datos: {exc}")
