"""
Pruebas de Detección de Anomalías Sísmicas y Validación Numérica de IA.
"""
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from airflow.include.anomaly_detection import haversine_distance, detect_anomalies_and_swarms
from airflow.include.ai_client import validate_numbers_integrity, generate_fallback_report

def test_haversine_distance():
    # Distancia conocida aproximada Bogotá - Medellín ~ 240 km
    d = haversine_distance(4.7110, -74.0721, 6.2442, -75.5812)
    assert 220 < d < 260

def test_detect_anomalies_and_swarms():
    now = pd.Timestamp.now(tz=timezone.utc)
    # Crear 3 eventos concentrados en la misma zona en menos de 2 horas (enjambre)
    df = pd.DataFrame([
        {
            "id": "swarm_1", "magnitude": 4.1, "latitude": 35.0, "longitude": -119.0,
            "depth_km": 8.0, "significance": 250, "tsunami_alert": False,
            "region": "California", "event_time": now
        },
        {
            "id": "swarm_2", "magnitude": 4.3, "latitude": 35.05, "longitude": -119.05,
            "depth_km": 9.0, "significance": 280, "tsunami_alert": False,
            "region": "California", "event_time": now + pd.Timedelta(minutes=30)
        },
        {
            "id": "swarm_3", "magnitude": 4.5, "latitude": 35.08, "longitude": -119.08,
            "depth_km": 7.5, "significance": 300, "tsunami_alert": False,
            "region": "California", "event_time": now + pd.Timedelta(minutes=60)
        }
    ])

    enriched, alerts = detect_anomalies_and_swarms(df)
    assert enriched["is_anomaly"].sum() >= 3
    assert len(alerts) >= 3
    assert any("Enjambre" in str(a["explanation"]) for a in alerts)

def test_validate_numbers_integrity_valid():
    metrics = {
        "max_magnitude": 6.2,
        "tsunami_events": 0,
        "total_events": 45
    }
    narrative = "Durante la jornada se registró una actividad moderada con sismo máximo de magnitud 6.2 en la región."
    is_valid, discrepancies = validate_numbers_integrity(narrative, metrics)
    assert is_valid is True
    assert len(discrepancies) == 0

def test_validate_numbers_integrity_hallucination():
    metrics = {
        "max_magnitude": 5.1,
        "tsunami_events": 0,
        "total_events": 20
    }
    # Alucinación: el LLM inventó una magnitud 7.8 y afirmó alerta de tsunami
    narrative = "Un devastador sismo de magnitud 7.8 activó alerta de tsunami en la costa."
    is_valid, discrepancies = validate_numbers_integrity(narrative, metrics)
    assert is_valid is False
    assert len(discrepancies) >= 1

def test_generate_fallback_report():
    metrics = {
        "report_date": "2026-10-27",
        "total_events": 120,
        "max_magnitude": 6.7,
        "max_mag_location": "Coast of Honshu, Japan",
        "avg_magnitude": 3.8,
        "tsunami_events": 1,
        "anomalous_events": 4,
        "top_active_region": "Japan"
    }
    report = generate_fallback_report(metrics)
    assert "Modo de Contingencia" in report["title"]
    assert report["risk_assessment"] == "Crítico"
    assert len(report["key_findings"]) >= 4
    assert len(report["operational_recommendations"]) >= 2
