-- YugabyteDB Database Setup Script
-- Run this script using: ysqlsh -h localhost -p 5433 -U postgres -f setup-yugabytedb.sql
-- Or connect first: ysqlsh -h localhost -p 5433 -U postgres
-- Default password for postgres user is usually empty (press Enter) or 'yugabyte'

-- Create user if not exists (for YugabyteDB)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'yugabyte') THEN
        CREATE USER yugabyte WITH PASSWORD 'yugabyte';
        RAISE NOTICE 'User yugabyte created';
    ELSE
        RAISE NOTICE 'User yugabyte already exists';
    END IF;
END
$$;

-- Create database if not exists
SELECT 'CREATE DATABASE bedrock_enriched_content_db OWNER yugabyte'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'bedrock_enriched_content_db')\gexec

-- Connect to the new database
\c bedrock_enriched_content_db

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE bedrock_enriched_content_db TO yugabyte;
GRANT ALL PRIVILEGES ON SCHEMA public TO yugabyte;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO yugabyte;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO yugabyte;

-- Optional (recommended if you run with ddl-auto=validate/none):
-- Persist per-item hashes per (sourceUri, version) for safe delta computation fallback.
CREATE TABLE IF NOT EXISTS item_version_hashes (
    source_uri   TEXT        NOT NULL,
    version      INTEGER     NOT NULL,
    source_path  TEXT        NOT NULL,
    item_type    TEXT        NOT NULL,
    usage_path   TEXT        NOT NULL,
    content_hash TEXT        NOT NULL,
    context_hash TEXT        NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_uri, version, source_path, item_type, usage_path)
);

CREATE INDEX IF NOT EXISTS idx_item_version_hashes_source_version
    ON item_version_hashes (source_uri, version);

-- Verify
\du
\l

SELECT 'Database and user setup complete!' AS status;






