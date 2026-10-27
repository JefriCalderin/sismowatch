"""
Configuración del Backend API de SismoWatch.
Carga variables de entorno para seguridad, base de datos y CORS.
"""
import os
from typing import List

class ApiSettings:
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "sismowatch")
    
    # Credenciales de lectura para la plataforma (Mínimo Privilegio: Solo Gold)
    DB_USER: str = os.getenv("DB_READ_USER", "platform_user")
    DB_PASSWORD: str = os.getenv("DB_READ_PASSWORD", "PlatformRead2026!")

    # Seguridad JWT
    JWT_SECRET: str = os.getenv("JWT_SECRET", "super_secret_jwt_key_sismowatch_2026_xYz987!Secured")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "120"))

    # Usuarios del Sistema (Inyectados por entorno, nunca en código)
    ADMIN_USERNAME: str = os.getenv("APP_ADMIN_USERNAME", "admin")
    ADMIN_PASSWORD: str = os.getenv("APP_ADMIN_PASSWORD", "AdminSismo2026!")
    VIEWER_USERNAME: str = os.getenv("APP_VIEWER_USERNAME", "viewer")
    VIEWER_PASSWORD: str = os.getenv("APP_VIEWER_PASSWORD", "ViewerSismo2026!")

    # CORS
    CORS_ORIGINS: List[str] = [
        origin.strip() for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",")
    ]

    # Airflow API para acciones de Administrador
    AIRFLOW_API_URL: str = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
    AIRFLOW_ADMIN_USER: str = os.getenv("AIRFLOW_ADMIN_USER", "admin")
    AIRFLOW_ADMIN_PASSWORD: str = os.getenv("AIRFLOW_ADMIN_PASSWORD", "SismoAdmin2026!")

    @property
    def db_uri(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

api_settings = ApiSettings()
