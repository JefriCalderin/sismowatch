-- ==============================================================================
-- 02_bronze_tables.sql
-- Capa Bronze (Raw Ingestion)
-- Almacena los eventos sísmicos exactamente como se reciben del API de USGS
-- ==============================================================================

CREATE TABLE IF NOT EXISTS bronze.earthquakes_raw (
    id VARCHAR(64) PRIMARY KEY,
    properties_mag NUMERIC(5,2),
    properties_place TEXT,
    properties_time BIGINT,
    properties_updated BIGINT,
    properties_tz INT,
    properties_url TEXT,
    properties_detail TEXT,
    properties_felt INT,
    properties_cdi NUMERIC(4,2),
    properties_mmi NUMERIC(4,2),
    properties_alert VARCHAR(32),
    properties_status VARCHAR(32),
    properties_tsunami INT,
    properties_sig INT,
    properties_net VARCHAR(32),
    properties_code VARCHAR(64),
    properties_ids TEXT,
    properties_sources TEXT,
    properties_types TEXT,
    properties_nst INT,
    properties_dmin NUMERIC(6,3),
    properties_rms NUMERIC(6,3),
    properties_gap NUMERIC(6,2),
    properties_magType VARCHAR(32),
    properties_type VARCHAR(64),
    properties_title TEXT,
    geometry_type VARCHAR(32),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    depth_km DOUBLE PRECISION,
    raw_payload JSONB,
    source_feed VARCHAR(255),
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices para optimizar consultas de actualización e ingesta idempotente
CREATE INDEX IF NOT EXISTS idx_bronze_time ON bronze.earthquakes_raw(properties_time);
CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_ts ON bronze.earthquakes_raw(ingestion_timestamp);
