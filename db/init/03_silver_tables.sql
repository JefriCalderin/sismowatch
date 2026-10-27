-- ==============================================================================
-- 03_silver_tables.sql
-- Capa Silver (Staged, Cleaned & Validated)
-- Datos limpios, tipados, coordenadas desanidadas y texto sanitizado
-- ==============================================================================

CREATE TABLE IF NOT EXISTS silver.earthquakes_cleaned (
    id VARCHAR(64) PRIMARY KEY,
    magnitude NUMERIC(4,2) NOT NULL,
    place TEXT NOT NULL,
    sanitized_place TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_time TIMESTAMP WITH TIME ZONE NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION NOT NULL,
    tsunami_alert BOOLEAN NOT NULL DEFAULT FALSE,
    significance INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    magnitude_type VARCHAR(32),
    event_type VARCHAR(64),
    url TEXT,
    region VARCHAR(128) NOT NULL,
    magnitude_category VARCHAR(32) NOT NULL,
    depth_category VARCHAR(32) NOT NULL,
    is_valid BOOLEAN DEFAULT TRUE,
    silver_processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices analíticos de la capa Silver
CREATE INDEX IF NOT EXISTS idx_silver_event_time ON silver.earthquakes_cleaned(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_silver_magnitude ON silver.earthquakes_cleaned(magnitude);
CREATE INDEX IF NOT EXISTS idx_silver_region ON silver.earthquakes_cleaned(region);
