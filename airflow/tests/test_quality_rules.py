"""
Pruebas de Reglas de Calidad de Datos (Data Quality Gate).
Valida casos conformes (válidos) y casos de rechazo (coordenadas fuera de rango,
magnitudes imposibles, IDs duplicados y nulos obligatorios).
"""
import pytest
import pandas as pd
from datetime import datetime, timezone
import airflow.include.quality as quality
from airflow.include.quality import run_data_quality_checks

def test_data_quality_valid_records(monkeypatch):
    """Caso válido: todos los registros cumplen el esquema y las reglas de rango."""
    monkeypatch.setattr(quality, "_record_quality_metrics", lambda run_id, report: None)

    now = pd.Timestamp.now(tz=timezone.utc)
    valid_data = pd.DataFrame([
        {
            "id": "us7000test1",
            "magnitude": 4.5,
            "latitude": 34.05,
            "longitude": -118.25,
            "depth_km": 12.0,
            "place": "10 km S of Los Angeles, CA",
            "event_time": now,
            "tsunami_alert": False,
            "significance": 250
        },
        {
            "id": "us7000test2",
            "magnitude": 5.8,
            "latitude": -33.45,
            "longitude": -70.66,
            "depth_km": 45.0,
            "place": "Central Chile",
            "event_time": now,
            "tsunami_alert": True,
            "significance": 600
        }
    ])

    is_passed, valid_df, report = run_data_quality_checks(valid_data, pipeline_run_id="test_run_valid")
    assert is_passed is True
    assert report["score"] == 100.0
    assert report["rejected"] == 0
    assert len(valid_df) == 2

def test_data_quality_invalid_records(monkeypatch):
    """Caso inválido: registros con latitud imposible, magnitud corrupta y duplicados."""
    monkeypatch.setattr(quality, "_record_quality_metrics", lambda run_id, report: None)

    now = pd.Timestamp.now(tz=timezone.utc)
    corrupted_data = pd.DataFrame([
        {
            "id": "us_valid_1",
            "magnitude": 3.2,
            "latitude": 20.0,
            "longitude": -100.0,
            "depth_km": 10.0,
            "place": "Mexico",
            "event_time": now,
            "tsunami_alert": False,
            "significance": 100
        },
        {
            "id": "us_invalid_lat",
            "magnitude": 4.0,
            "latitude": 150.0, # Fuera de rango (-90 a 90)
            "longitude": 0.0,
            "depth_km": 20.0,
            "place": "Invalid Lat",
            "event_time": now,
            "tsunami_alert": False,
            "significance": 100
        },
        {
            "id": "us_invalid_mag",
            "magnitude": 25.0, # Magnitud físicamente imposible
            "latitude": 0.0,
            "longitude": 0.0,
            "depth_km": 20.0,
            "place": "Invalid Mag",
            "event_time": now,
            "tsunami_alert": False,
            "significance": 100
        },
        {
            "id": "us_valid_1", # ID Duplicado
            "magnitude": 3.2,
            "latitude": 20.0,
            "longitude": -100.0,
            "depth_km": 10.0,
            "place": "Mexico",
            "event_time": now,
            "tsunami_alert": False,
            "significance": 100
        }
    ])

    is_passed, valid_df, report = run_data_quality_checks(corrupted_data, pipeline_run_id="test_run_corrupt")
    # Solo 1 de 4 registros es completamente válido -> score 25% < 90% umbral mínimo
    assert is_passed is False
    assert report["status"] == "BLOCKED"
    assert report["rejected"] > 0
    assert len(valid_df) == 1
    assert valid_df.iloc[0]["id"] == "us_valid_1"
