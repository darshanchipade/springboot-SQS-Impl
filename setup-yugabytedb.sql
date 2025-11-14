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

-- Verify
\du
\l

SELECT 'Database and user setup complete!' AS status;






