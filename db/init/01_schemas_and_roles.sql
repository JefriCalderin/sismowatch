-- ==============================================================================
-- 01_schemas_and_roles.sql
-- Creación de Schemas Medallón (Bronze, Silver, Gold) y Roles de Mínimo Privilegio
-- Arquitectura Medallón sin Landing ni Cuarentena
-- ==============================================================================

-- 1. Creación de Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. Creación de Roles de Mínimo Privilegio (con contraseñas si no existen)
DO $$
BEGIN
    -- Rol de Ingesta: Solo escribe en Bronze
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'role_ingest') THEN
        CREATE ROLE role_ingest WITH LOGIN PASSWORD 'IngestSecret2026!';
    END IF;

    -- Rol de Transformación: Lee Bronze, escribe Silver y Gold
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'role_transform') THEN
        CREATE ROLE role_transform WITH LOGIN PASSWORD 'TransformSecret2026!';
    END IF;

    -- Rol de Lectura / Plataforma: SOLO lee Gold. Cero acceso a Bronze y Silver
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'platform_user') THEN
        CREATE ROLE platform_user WITH LOGIN PASSWORD 'PlatformRead2026!';
    END IF;
END $$;

-- 3. Permisos para role_ingest (Solo Bronze)
GRANT USAGE ON SCHEMA bronze TO role_ingest;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bronze TO role_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT, INSERT, UPDATE ON TABLES TO role_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT USAGE, SELECT ON SEQUENCES TO role_ingest;
REVOKE ALL ON SCHEMA silver FROM role_ingest;
REVOKE ALL ON SCHEMA gold FROM role_ingest;

-- 4. Permisos para role_transform (Lee Bronze, opera Silver y Gold)
GRANT USAGE ON SCHEMA bronze TO role_transform;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO role_transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO role_transform;

GRANT USAGE, CREATE ON SCHEMA silver TO role_transform;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO role_transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT USAGE, SELECT ON SEQUENCES TO role_transform;

GRANT USAGE, CREATE ON SCHEMA gold TO role_transform;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO role_transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE, SELECT ON SEQUENCES TO role_transform;

-- 5. Permisos para platform_user (Mínimo Privilegio: SOLO Lectura en Gold)
GRANT USAGE ON SCHEMA gold TO platform_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO platform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO platform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT USAGE, SELECT ON SEQUENCES TO platform_user;

-- Garantizar que platform_user NO tenga acceso a Bronze ni Silver
REVOKE ALL ON SCHEMA bronze FROM platform_user;
REVOKE ALL ON SCHEMA silver FROM platform_user;
