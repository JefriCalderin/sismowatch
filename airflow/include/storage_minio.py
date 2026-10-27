"""
Módulo de Integración con MinIO Object Storage (S3-Compatible Data Lake).
Permite almacenar reportes ejecutivos (HTML, JSON), copias de seguridad de la capa Gold
y artefactos analíticos generados por los pipelines de Apache Airflow.
"""
import io
import json
import logging
from typing import Optional, Dict, Any, List
from minio import Minio
from minio.error import S3Error
from airflow.include.config import settings

logger = logging.getLogger(__name__)

# Configuración MinIO
MINIO_ENDPOINT = settings.MINIO_ENDPOINT if hasattr(settings, "MINIO_ENDPOINT") else "minio:9000"
MINIO_ACCESS_KEY = settings.MINIO_ROOT_USER if hasattr(settings, "MINIO_ROOT_USER") else "minioadmin"
MINIO_SECRET_KEY = settings.MINIO_ROOT_PASSWORD if hasattr(settings, "MINIO_ROOT_PASSWORD") else "MinioAdminSismo2026!"
MINIO_SECURE = getattr(settings, "MINIO_SECURE", False)

DEFAULT_BUCKETS = ["sismowatch-reports", "sismowatch-gold", "sismowatch-backups"]

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
