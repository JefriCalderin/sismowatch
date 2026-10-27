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
