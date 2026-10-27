-- ==============================================================================
-- 04_gold_tables.sql
-- Capa Gold (Curated Business Marts & Analytics)
-- Tablas dimensionales, hechos, reportes de IA, métricas de calidad y anomalías
-- ==============================================================================

-- 1. Tabla de Hechos de Eventos Sísmicos (Consumo para API y Plataforma)
CREATE TABLE IF NOT EXISTS gold.fact_earthquakes (
    id VARCHAR(64) PRIMARY KEY,
    magnitude NUMERIC(4,2) NOT NULL,
    place TEXT NOT NULL,
    region VARCHAR(128) NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_time TIMESTAMP WITH TIME ZONE NOT NULL,
    event_date DATE NOT NULL,
    event_hour INT NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION NOT NULL,
    tsunami_alert BOOLEAN NOT NULL DEFAULT FALSE,
    significance INT NOT NULL,
    magnitude_category VARCHAR(32) NOT NULL,
    depth_category VARCHAR(32) NOT NULL,
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_score NUMERIC(6,3) DEFAULT 0.0,
    anomaly_reason TEXT,
    url TEXT,
    curated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gold_fact_event_time ON gold.fact_earthquakes(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_gold_fact_date ON gold.fact_earthquakes(event_date);
CREATE INDEX IF NOT EXISTS idx_gold_fact_mag ON gold.fact_earthquakes(magnitude);
CREATE INDEX IF NOT EXISTS idx_gold_fact_region ON gold.fact_earthquakes(region);
CREATE INDEX IF NOT EXISTS idx_gold_fact_anomaly ON gold.fact_earthquakes(is_anomaly);

-- 2. Métricas Diarias Agregadas
CREATE TABLE IF NOT EXISTS gold.daily_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE UNIQUE NOT NULL,
    total_events INT NOT NULL DEFAULT 0,
    max_magnitude NUMERIC(4,2) NOT NULL DEFAULT 0.0,
    avg_magnitude NUMERIC(4,2) NOT NULL DEFAULT 0.0,
    min_magnitude NUMERIC(4,2) NOT NULL DEFAULT 0.0,
    tsunami_events INT NOT NULL DEFAULT 0,
    anomalous_events INT NOT NULL DEFAULT 0,
    avg_depth_km NUMERIC(6,2) NOT NULL DEFAULT 0.0,
    top_active_region VARCHAR(128),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gold_daily_metrics_date ON gold.daily_metrics(metric_date DESC);

-- 3. Métricas de Calidad de Datos (Ejecución bloqueante en Airflow)
CREATE TABLE IF NOT EXISTS gold.quality_metrics (
    id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(128) NOT NULL,
    execution_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_records_evaluated INT NOT NULL DEFAULT 0,
    valid_records INT NOT NULL DEFAULT 0,
    rejected_records INT NOT NULL DEFAULT 0,
    quality_score NUMERIC(5,2) NOT NULL DEFAULT 100.0,
    rules_evaluated INT NOT NULL DEFAULT 0,
    rules_passed INT NOT NULL DEFAULT 0,
    rules_failed INT NOT NULL DEFAULT 0,
    freshness_lag_seconds INT,
    status VARCHAR(32) NOT NULL, -- 'PASSED', 'WARNING', 'BLOCKED'
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_gold_quality_ts ON gold.quality_metrics(execution_timestamp DESC);

-- 4. Reportes Ejecutivos de IA (Narrativa + Cifras Exactas Validadas)
CREATE TABLE IF NOT EXISTS gold.ai_reports (
    id SERIAL PRIMARY KEY,
    report_date DATE UNIQUE NOT NULL,
    title TEXT NOT NULL,
    executive_summary TEXT NOT NULL,
    key_findings JSONB NOT NULL,
    calculated_metrics JSONB NOT NULL,
    model_name VARCHAR(64) NOT NULL,
    generation_status VARCHAR(32) NOT NULL, -- 'SUCCESS', 'FALLBACK'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gold_ai_reports_date ON gold.ai_reports(report_date DESC);

-- 5. Logs de Auditoría y Ejecución de IA (Tokens, Latencia, Costos)
CREATE TABLE IF NOT EXISTS gold.ai_execution_logs (
    id SERIAL PRIMARY KEY,
    execution_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    task_name VARCHAR(64) NOT NULL,
    model_name VARCHAR(64) NOT NULL,
    prompt_tokens INT DEFAULT 0,
    completion_tokens INT DEFAULT 0,
    total_tokens INT DEFAULT 0,
    latency_ms NUMERIC(10,2) NOT NULL,
    estimated_cost_usd NUMERIC(10,6) DEFAULT 0.0,
    status VARCHAR(32) NOT NULL,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_gold_ai_logs_ts ON gold.ai_execution_logs(execution_timestamp DESC);

-- 6. Alertas de Detección de Anomalías (ML / Estadística)
CREATE TABLE IF NOT EXISTS gold.anomaly_alerts (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    algorithm VARCHAR(64) NOT NULL,
    anomaly_score NUMERIC(6,3) NOT NULL,
    severity VARCHAR(32) NOT NULL, -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    explanation TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gold_anomalies_ts ON gold.anomaly_alerts(detected_at DESC);

-- 7. Vista en Tiempo Real para KPIs de las últimas 24 Horas
CREATE OR REPLACE VIEW gold.view_realtime_kpis AS
SELECT
    COUNT(*) AS events_24h,
    COALESCE(MAX(magnitude), 0.0) AS max_magnitude_24h,
    COALESCE(ROUND(AVG(magnitude), 2), 0.0) AS avg_magnitude_24h,
    COALESCE(SUM(CASE WHEN tsunami_alert THEN 1 ELSE 0 END), 0) AS tsunami_alerts_24h,
    COALESCE(SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END), 0) AS anomalies_24h,
    MAX(event_time) AS last_event_time,
    MAX(curated_at) AS last_pipeline_update
FROM gold.fact_earthquakes
WHERE event_time >= (NOW() - INTERVAL '24 hours');
