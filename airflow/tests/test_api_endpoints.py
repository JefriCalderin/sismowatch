"""
Pruebas de Seguridad y Endpoints de la API.
Valida autenticación JWT, mínimo privilegio y control de acceso RBAC
(comprueba rechazo 401 ante falta de sesión y 403 cuando 'viewer' intenta acciones de 'admin').
"""
import pytest
from fastapi.testclient import TestClient
from api.app.main import app
from api.app.config import api_settings

client = TestClient(app)

def test_health_endpoint():
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "healthy"

def test_login_invalid_credentials():
    res = client.post("/auth/login", json={"username": "wrong", "password": "bad"})
    assert res.status_code == 401

def test_login_viewer_success():
    res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    assert res.status_code == 200
    data = res.json()
    assert "access_token" in data
    assert data["role"] == "viewer"

def test_login_admin_success():
    res = client.post("/auth/login", json={
        "username": api_settings.ADMIN_USERNAME,
        "password": api_settings.ADMIN_PASSWORD
    })
    assert res.status_code == 200
    data = res.json()
    assert "access_token" in data
    assert data["role"] == "admin"

def test_unauthenticated_request_blocked():
    """Sin sesión, ningún endpoint protegido es accesible (recibe 401)."""
    res = client.get("/api/kpis")
    assert res.status_code == 401

def test_viewer_cannot_execute_admin_backfill():
    """Un usuario con rol 'viewer' que intenta acción de admin recibe 403 Forbidden."""
    # 1. Obtener token de viewer
    login_res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    viewer_token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {viewer_token}"}

    # 2. Intentar backfill
    res = client.post("/api/admin/backfill", headers=headers)
    assert res.status_code == 403
    assert "Permiso denegado" in res.json()["detail"]

def test_viewer_cannot_access_quarantine_audit():
    """Un usuario 'viewer' no puede acceder a los registros de auditoría/cuarentena (recibe 403)."""
    login_res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    viewer_token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {viewer_token}"}

    res = client.get("/api/quality/quarantine", headers=headers)
    assert res.status_code == 403

def test_admin_can_access_quarantine_audit(monkeypatch):
    """Un usuario 'admin' tiene permiso para consultar auditoría de calidad."""
    monkeypatch.setattr("api.app.routers.admin.query_db", lambda q, p=(): [])
    
    login_res = client.post("/auth/login", json={
        "username": api_settings.ADMIN_USERNAME,
        "password": api_settings.ADMIN_PASSWORD
    })
    admin_token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {admin_token}"}

    res = client.get("/api/quality/quarantine", headers=headers)
    assert res.status_code == 200
    data = res.json()
    assert "architecture_note" in data

def test_export_with_query_token(monkeypatch):
    """
    Verifica que la descarga de archivos funcione pasando el token como parámetro ?token=...
    (Resuelve el error 401 reportado por el usuario al hacer clic en enlaces de descarga).
    """
    monkeypatch.setattr("api.app.routers.reports.query_db", lambda q, p=(): [])
    
    login_res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    token = login_res.json()["access_token"]
    
    # Petición directa SIN header Authorization, solo con ?token=
    res = client.get(f"/api/reports/export?format=csv&token={token}")
    assert res.status_code == 200
    assert "text/csv" in res.headers["content-type"]
    assert "attachment" in res.headers["content-disposition"]

def test_chat_ai_endpoint(monkeypatch):
    """
    Verifica que el nuevo endpoint de Chat IA (/api/chat) responda adecuadamente
    a preguntas de los usuarios utilizando los datos de la capa Gold.
    """
    monkeypatch.setattr("api.app.routers.chat.fetch_gold_context", lambda: {
        "kpis": {"events_24h": 10, "max_magnitude": 5.4, "avg_magnitude": 2.1, "tsunami_alerts": 0},
        "top_events": [{"place": "Central Chile", "magnitude": 5.4, "depth_km": 15.0, "tsunami_alert": False, "is_anomaly": False}],
        "recent_events": [],
        "last_report": {"title": "Reporte Sísmico", "executive_summary": "Actividad moderada.", "key_findings": ["Evento 5.4 en Chile"]},
        "quality": {"quality_score": 100.0, "status": "PASSED"},
        "anomalies": []
    })
    
    login_res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    res = client.post("/api/chat", json={
        "message": "¿Cuál fue el sismo de mayor magnitud registrado?",
        "history": []
    }, headers=headers)
    
    assert res.status_code == 200
    data = res.json()
    assert "reply" in data
    assert "gold.fact_earthquakes" in data["data_sources"]
    assert "5.4" in data["reply"] or "Chile" in data["reply"]

def test_usgs_historical_query_last_days(monkeypatch):
    """
    Verifica la consulta al catálogo oficial de USGS para los últimos N días.
    """
    sample_geojson = {
        "type": "FeatureCollection",
        "metadata": {"count": 2, "status": 200},
        "features": [
            {
                "id": "us7000test1",
                "properties": {"mag": 5.2, "place": "Near coast of Peru", "time": 1729876543000, "updated": 1729876600000, "tsunami": 0, "sig": 415},
                "geometry": {"type": "Point", "coordinates": [-76.2, -14.1, 35.0]}
            },
            {
                "id": "us7000test2",
                "properties": {"mag": 3.4, "place": "Central California", "time": 1729876000000, "updated": 1729876100000, "tsunami": 0, "sig": 180},
                "geometry": {"type": "Point", "coordinates": [-120.5, 36.2, 8.0]}
            }
        ]
    }
    
    class MockResponse:
        status_code = 200
        def json(self): return sample_geojson

    async def mock_get(self, url, **kwargs):
        return MockResponse()

    monkeypatch.setattr("httpx.AsyncClient.get", mock_get)

    login_res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    res = client.get("/api/events/usgs-query?search_type=last_n_days&last_days=7&min_magnitude=2.5", headers=headers)
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 2
    assert len(data["events"]) == 2
    assert data["events"][0]["id"] == "us7000test1"
    assert data["events"][0]["magnitude"] == 5.2
    assert "map_markers" in data
    assert len(data["map_markers"]) == 2

def test_usgs_historical_query_specific_day(monkeypatch):
    """
    Verifica la consulta de sismos para un día específico (24 horas) en formato ISO UTC.
    """
    sample_geojson = {
        "type": "FeatureCollection",
        "metadata": {"count": 1, "status": 200},
        "features": [{
            "id": "us7000daytest",
            "properties": {"mag": 4.6, "place": "Fox Islands, Aleutian Islands", "time": 1729850000000, "updated": 1729850100000, "tsunami": 0, "sig": 320},
            "geometry": {"type": "Point", "coordinates": [-168.5, 53.2, 45.0]}
        }]
    }

    class MockResponse:
        status_code = 200
        def json(self): return sample_geojson

    async def mock_get(self, url, **kwargs):
        # Verificar que starttime y endtime cubran las 24 horas del día
        params = kwargs.get("params", {})
        assert "2026-10-25T00:00:00" in params.get("starttime", "")
        assert "2026-10-25T23:59:59" in params.get("endtime", "")
        return MockResponse()

    monkeypatch.setattr("httpx.AsyncClient.get", mock_get)

    login_res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    res = client.get("/api/events/usgs-query?search_type=specific_day&target_date=2026-10-25&min_magnitude=4.0", headers=headers)
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 1
    assert data["events"][0]["magnitude"] == 4.6

def test_usgs_historical_query_invalid_search_type():
    """Valida rechazo 400 ante tipos de búsqueda no reconocidos."""
    login_res = client.post("/auth/login", json={
        "username": api_settings.VIEWER_USERNAME,
        "password": api_settings.VIEWER_PASSWORD
    })
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    res = client.get("/api/events/usgs-query?search_type=invalid_type", headers=headers)
    assert res.status_code == 400
