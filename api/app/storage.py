"""
Módulo de almacenamiento MinIO para la API de SismoWatch.
Permite consultar y descargar reportes y artefactos almacenados en el Data Lake S3.
"""
import io
import logging
from typing import Optional, List, Dict, Any
from minio import Minio
from api.app.config import api_settings

logger = logging.getLogger(__name__)

def get_minio_client() -> Optional[Minio]:
    """Retorna una instancia cliente de MinIO."""
    try:
        return Minio(
            endpoint=api_settings.MINIO_ENDPOINT,
            access_key=api_settings.MINIO_ROOT_USER,
            secret_key=api_settings.MINIO_ROOT_PASSWORD,
            secure=api_settings.MINIO_SECURE
        )
    except Exception as exc:
        logger.warning(f"No se pudo conectar a MinIO ({api_settings.MINIO_ENDPOINT}): {exc}")
        return None

def ensure_buckets():
    """Asegura que los buckets base existan."""
    client = get_minio_client()
    if not client:
        return
    for b in ["sismowatch-reports", "sismowatch-gold", "sismowatch-backups"]:
        try:
            if not client.bucket_exists(b):
                client.make_bucket(b)
        except Exception:
            pass
