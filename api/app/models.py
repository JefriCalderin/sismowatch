"""
Modelos Pydantic para serialización y validación de entradas y respuestas de la API.
"""
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class LoginRequest(BaseModel):
    username: str = Field(..., example="admin")
    password: str = Field(..., example="AdminSismo2026!")

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str

class KPIResponse(BaseModel):
    events_24h: int
    max_magnitude: float
    avg_magnitude: float
    tsunami_alerts: int
    anomalies_count: int
    quality_score: float
    last_update: Optional[str]
    freshness_status: str

class EarthquakeEvent(BaseModel):
    id: str
    magnitude: float
    place: str
    region: str
    event_time: datetime
    updated_time: datetime
    longitude: float
    latitude: float
    depth_km: float
    tsunami_alert: bool
    significance: int
    magnitude_category: str
    depth_category: str
    is_anomaly: bool
    anomaly_score: float
    anomaly_reason: Optional[str] = None
    url: Optional[str] = None

class PaginatedEventsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    events: List[EarthquakeEvent]

class TimeSeriesPoint(BaseModel):
    time_bucket: str
    event_count: int
    avg_magnitude: float
    max_magnitude: float

class MagnitudeDistribution(BaseModel):
    category: str
    count: int

class RegionActivity(BaseModel):
    region: str
    count: int

class AIReportResponse(BaseModel):
    id: int
    report_date: date
    title: str
    executive_summary: str
    key_findings: List[str]
    calculated_metrics: Dict[str, Any]
    model_name: str
    generation_status: str
    created_at: datetime

class PipelineStatusResponse(BaseModel):
    overall_status: str
    data_freshness_seconds: Optional[int]
    last_event_received: Optional[str]
    quality_score: float
    dags_status: List[Dict[str, Any]]
    medallion_layers: Dict[str, int]
    role_info: Optional[str] = None
