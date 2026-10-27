"""
Módulo de conexión a Base de Datos para el backend FastAPI.
Usa exclusivamente las credenciales de `platform_user` garantizando que
la plataforma solo tenga permisos SELECT sobre el esquema `gold`.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Generator, List, Dict, Any, Tuple
from api.app.config import api_settings

@contextmanager
def get_db_cursor() -> Generator[psycopg2.extensions.cursor, None, None]:
    """Provee un cursor con retorno en diccionarios para consultas de solo lectura en Gold."""
    conn = psycopg2.connect(
        host=api_settings.POSTGRES_HOST,
        port=api_settings.POSTGRES_PORT,
        dbname=api_settings.POSTGRES_DB,
        user=api_settings.DB_USER,
        password=api_settings.DB_PASSWORD,
        connect_timeout=5
    )
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()

def query_db(query: str, params: Tuple = ()) -> List[Dict[str, Any]]:
    """Ejecuta una consulta SQL en la capa Gold y retorna los registros."""
    with get_db_cursor() as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def query_db_one(query: str, params: Tuple = ()) -> Dict[str, Any]:
    """Ejecuta una consulta SQL y retorna un único registro o diccionario vacío."""
    with get_db_cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else {}
