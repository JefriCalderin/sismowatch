"""
Configuración centralizada para pipelines de Airflow y componentes de ingestión/transformación.
Carga variables de entorno de forma segura sin exponer credenciales en el código.
"""
import os
from typing import Dict, Any

class Settings:
    # Base de Datos
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "sismowatch")
    
    # Credenciales de roles con Mínimo Privilegio
    DB_INGEST_USER: str = os.getenv("DB_INGEST_USER", "role_ingest")
    DB_INGEST_PASSWORD: str = os.getenv("DB_INGEST_PASSWORD", "IngestSecret2026!")
    
    DB_TRANSFORM_USER: str = os.getenv("DB_TRANSFORM_USER", "role_transform")
    DB_TRANSFORM_PASSWORD: str = os.getenv("DB_TRANSFORM_PASSWORD", "TransformSecret2026!")
    
    DB_READ_USER: str = os.getenv("DB_READ_USER", "platform_user")
    DB_READ_PASSWORD: str = os.getenv("DB_READ_PASSWORD", "PlatformRead2026!")

    # Endpoints de API USGS
    USGS_FEED_URL: str = os.getenv(
        "USGS_FEED_URL",
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    )
    USGS_QUERY_URL: str = os.getenv(
        "USGS_QUERY_URL",
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
    )

    # Configuración de Calidad
    QUALITY_MIN_SCORE: float = float(os.getenv("QUALITY_MIN_SCORE", "90.0"))
    FRESHNESS_THRESHOLD_MINUTES: int = int(os.getenv("FRESHNESS_THRESHOLD_MINUTES", "120"))

    # Configuración de IA y LLM
    AI_PROVIDER: str = os.getenv("AI_PROVIDER", "gemini")
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    AI_MODEL_NAME: str = os.getenv("AI_MODEL_NAME", "gemini-2.0-flash-lite")

    # Spark
    SPARK_MASTER_URL: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    # MinIO Object Storage
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ROOT_USER: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_ROOT_PASSWORD: str = os.getenv("MINIO_ROOT_PASSWORD", "MinioAdminSismo2026!")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"

    @classmethod
    def get_db_uri(cls, role: str = "transform") -> str:
        """Obtiene la URI de conexión a PostgreSQL según el rol solicitado."""
        if role == "ingest":
            user, pwd = cls.DB_INGEST_USER, cls.DB_INGEST_PASSWORD
        elif role == "read":
            user, pwd = cls.DB_READ_USER, cls.DB_READ_PASSWORD
        else:
            user, pwd = cls.DB_TRANSFORM_USER, cls.DB_TRANSFORM_PASSWORD
        return f"postgresql://{user}:{pwd}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"

settings = Settings()
