"""
Manejador de conexiones a la base de datos PostgreSQL aplicando mínimo privilegio por rol.
"""
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from contextlib import contextmanager
from typing import Generator, List, Dict, Any, Tuple
from airflow.include.config import settings

@contextmanager
def get_db_connection(role: str = "transform") -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager para obtener conexiones seguras a PostgreSQL según el rol requerido.
    Roles:
      - 'ingest': acceso de escritura solo a bronze
      - 'transform': lectura de bronze, escritura en silver y gold
      - 'read': lectura exclusiva en gold
    """
    if role == "ingest":
        user = settings.DB_INGEST_USER
        password = settings.DB_INGEST_PASSWORD
    elif role == "read":
        user = settings.DB_READ_USER
        password = settings.DB_READ_PASSWORD
    else:
        user = settings.DB_TRANSFORM_USER
        password = settings.DB_TRANSFORM_PASSWORD

    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        dbname=settings.POSTGRES_DB,
        user=user,
        password=password,
        connect_timeout=10
    )
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def execute_query(query: str, params: Tuple = (), role: str = "transform", fetch: bool = False) -> List[Dict[str, Any]]:
    """Ejecuta una consulta SQL y retorna los registros en forma de diccionario."""
    with get_db_connection(role=role) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                return [dict(row) for row in cur.fetchall()]
            return []

def execute_batch_upsert(query: str, values: List[Tuple], role: str = "transform", page_size: int = 500) -> int:
    """Ejecuta inserciones o upserts masivos de alto rendimiento."""
    if not values:
        return 0
    with get_db_connection(role=role) as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, values, page_size=page_size)
    return len(values)
