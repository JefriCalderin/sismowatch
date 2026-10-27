"""
Módulo de Integración con MinIO Object Storage (S3-Compatible Data Lakehouse).
Gestiona la persistencia de las 3 capas Medallón en buckets S3 dedicados:
  - sismowatch-bronze: Cargas crudas GeoJSON de USGS.
  - sismowatch-silver: Datos normalizados, deduplicados y validados por Pandera.
  - sismowatch-gold: Tablas de hechos curadas, métricas agregadas y anomalías ML.
  - sismowatch-reports: Informes ejecutivos generados por el agente IA (HTML y JSON).
  - sismowatch-backups: Respaldos programados del lago de datos.
"""
import io
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from minio import Minio
from minio.error import S3Error
from airflow.include.config import settings

logger = logging.getLogger(__name__)

# Configuración MinIO
MINIO_ENDPOINT = getattr(settings, "MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = getattr(settings, "MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = getattr(settings, "MINIO_ROOT_PASSWORD", "MinioAdminSismo2026!")
MINIO_SECURE = getattr(settings, "MINIO_SECURE", False)

DEFAULT_BUCKETS = [
    "sismowatch-bronze",
    "sismowatch-silver",
    "sismowatch-gold",
    "sismowatch-reports",
    "sismowatch-backups"
]

def get_minio_client() -> Optional[Minio]:
    """Crea una instancia cliente de MinIO segura."""
    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        return client
    except Exception as exc:
        logger.error(f"Error inicializando cliente MinIO ({MINIO_ENDPOINT}): {exc}")
        return None

def ensure_buckets_exist(buckets: Optional[List[str]] = None) -> bool:
    """Verifica y aprovisiona los buckets necesarios en MinIO."""
    client = get_minio_client()
    if not client:
        return False

    target_buckets = buckets or DEFAULT_BUCKETS
    for bucket in target_buckets:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"Bucket MinIO creado exitosamente: {bucket}")
        except Exception as exc:
            logger.warning(f"No se pudo verificar/crear bucket MinIO '{bucket}': {exc}")
            return False
    return True

def upload_bronze_payload(data: Dict[str, Any], tag: str = "usgs_feed") -> Optional[str]:
    """
    Sube el feed GeoJSON crudo a la capa Bronze en MinIO (bucket 'sismowatch-bronze').
    Estructurado en particiones temporales: year=YYYY/month=MM/day=DD/raw_events_{ts}.json
    """
    client = get_minio_client()
    if not client or not data:
        return None

    try:
        ensure_buckets_exist(["sismowatch-bronze"])
        now = datetime.now(timezone.utc)
        part_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        file_name = f"usgs_raw_{now.strftime('%Y%m%d_%H%M%S')}_{tag}.json"
        s3_key = f"{part_path}/{file_name}"

        payload_bytes = json.dumps(data, default=str, indent=2, ensure_ascii=False).encode("utf-8")
        client.put_object(
            bucket_name="sismowatch-bronze",
            object_name=s3_key,
            data=io.BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type="application/json"
        )
        logger.info(f"[MinIO Bronze] Raw GeoJSON almacenado en s3://sismowatch-bronze/{s3_key}")
        return f"s3://sismowatch-bronze/{s3_key}"
    except Exception as exc:
        logger.warning(f"Error subiendo feed a MinIO Bronze: {exc}")
        return None

def upload_silver_dataset(records: List[Dict[str, Any]], tag: str = "cleaned") -> Optional[str]:
    """
    Sube registros limpios y tipados a la capa Silver en MinIO (bucket 'sismowatch-silver').
    """
    client = get_minio_client()
    if not client or not records:
        return None

    try:
        ensure_buckets_exist(["sismowatch-silver"])
        now = datetime.now(timezone.utc)
        part_path = f"year={now.year}/month={now.month:02d}"
        file_name = f"earthquakes_silver_{now.strftime('%Y%m%d_%H%M%S')}_{tag}.json"
        s3_key = f"{part_path}/{file_name}"

        payload_bytes = json.dumps(records, default=str, indent=2, ensure_ascii=False).encode("utf-8")
        client.put_object(
            bucket_name="sismowatch-silver",
            object_name=s3_key,
            data=io.BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type="application/json"
        )
        logger.info(f"[MinIO Silver] Dataset limpio almacenado en s3://sismowatch-silver/{s3_key} ({len(records)} filas)")
        return f"s3://sismowatch-silver/{s3_key}"
    except Exception as exc:
        logger.warning(f"Error subiendo dataset a MinIO Silver: {exc}")
        return None

def upload_gold_dataset(fact_records: List[Dict[str, Any]], metrics: Optional[List[Dict[str, Any]]] = None) -> Dict[str, str]:
    """
    Sube tablas de hechos enriquecidas y métricas a la capa Gold en MinIO (bucket 'sismowatch-gold').
    """
    client = get_minio_client()
    result = {}
    if not client:
        return result

    try:
        ensure_buckets_exist(["sismowatch-gold"])
        now = datetime.now(timezone.utc)
        date_str = now.strftime('%Y-%m-%d')
        ts_str = now.strftime('%Y%m%d_%H%M%S')

        # 1. Subir fact table
        if fact_records:
            fact_key = f"facts/year={now.year}/month={now.month:02d}/fact_earthquakes_{ts_str}.json"
            f_bytes = json.dumps(fact_records, default=str, indent=2, ensure_ascii=False).encode("utf-8")
            client.put_object(
                bucket_name="sismowatch-gold",
                object_name=fact_key,
                data=io.BytesIO(f_bytes),
                length=len(f_bytes),
                content_type="application/json"
            )
            result["fact_s3"] = f"s3://sismowatch-gold/{fact_key}"

        # 2. Subir daily metrics
        if metrics:
            metrics_key = f"metrics/daily_metrics_{date_str}.json"
            m_bytes = json.dumps(metrics, default=str, indent=2, ensure_ascii=False).encode("utf-8")
            client.put_object(
                bucket_name="sismowatch-gold",
                object_name=metrics_key,
                data=io.BytesIO(m_bytes),
                length=len(m_bytes),
                content_type="application/json"
            )
            result["metrics_s3"] = f"s3://sismowatch-gold/{metrics_key}"

        logger.info(f"[MinIO Gold] Capa curada sincronizada en s3://sismowatch-gold")
    except Exception as exc:
        logger.warning(f"Error subiendo dataset a MinIO Gold: {exc}")

    return result

def upload_report_to_minio(report_date: str, html_content: str, report_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Sube los artefactos del reporte ejecutivo a MinIO en el bucket 'sismowatch-reports'.
    Guarda tanto la versión HTML imprimible como el JSON estructurado.
    """
    client = get_minio_client()
    if not client:
        return {"status": "SKIPPED", "error": "Cliente MinIO no disponible"}

    ensure_buckets_exist(["sismowatch-reports"])

    bucket_name = "sismowatch-reports"
    uploaded = {}

    try:
        # 1. Subir HTML
        html_bytes = html_content.encode("utf-8")
        html_key = f"reports/{report_date}/executive_report_{report_date}.html"
        client.put_object(
            bucket_name=bucket_name,
            object_name=html_key,
            data=io.BytesIO(html_bytes),
            length=len(html_bytes),
            content_type="text/html; charset=utf-8"
        )
        uploaded["html_path"] = f"s3://{bucket_name}/{html_key}"

        # 2. Subir JSON
        json_bytes = json.dumps(report_data, default=str, indent=2, ensure_ascii=False).encode("utf-8")
        json_key = f"reports/{report_date}/executive_report_{report_date}.json"
        client.put_object(
            bucket_name=bucket_name,
            object_name=json_key,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json"
        )
        uploaded["json_path"] = f"s3://{bucket_name}/{json_key}"
        uploaded["status"] = "SUCCESS"
        logger.info(f"Reporte del {report_date} subido con éxito a MinIO S3.")
    except Exception as exc:
        logger.error(f"Error subiendo reporte a MinIO: {exc}")
        uploaded["status"] = "ERROR"
        uploaded["error"] = str(exc)

    return uploaded
