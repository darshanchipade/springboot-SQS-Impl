-- Create user if not exists
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'yugabyte') THEN
        CREATE USER yugabyte WITH PASSWORD 'yugabyte';
    END IF;
END
\$\$;

-- Create database if not exists
SELECT 'CREATE DATABASE bedrock_enriched_content_db OWNER yugabyte'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'bedrock_enriched_content_db')\gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE bedrock_enriched_content_db TO yugabyte;





