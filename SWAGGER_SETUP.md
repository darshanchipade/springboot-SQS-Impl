# Swagger/OpenAPI Setup Guide

This guide explains how to set up and use Swagger UI with your Spring Boot application.

## Option 1: Docker YugabyteDB Setup (Recommended)

### Prerequisites
- Docker Desktop installed and running

### Steps

1. **Start YugabyteDB using Docker Compose:**
   ```powershell
   docker-compose up -d
   ```

2. **Wait for YugabyteDB to start (about 30-60 seconds):**
   ```powershell
   docker-compose ps
   ```
   Wait until the status shows "healthy"

3. **Create the database and user:**
   ```powershell
   # Copy SQL file into container
   docker cp setup-yugabytedb.sql springboot-yugabytedb:/tmp/
   
   # Run the setup script
   docker exec -it springboot-yugabytedb ysqlsh -h localhost -p 5433 -U postgres -f /tmp/setup-yugabytedb.sql
   ```
   (Default postgres password is usually empty - just press Enter)

4. **Start your Spring Boot application:**
   ```powershell
   mvn spring-boot:run
   ```

5. **Access Swagger UI:**
   - Open your browser and navigate to: `http://localhost:8080/swagger-ui.html`
   - Or use: `http://localhost:8080/swagger-ui/index.html`

6. **Stop YugabyteDB when done:**
   ```powershell
   docker-compose down
   ```

### Database Connection Details
- **Host:** localhost
- **Port:** 5433
- **Database:** bedrock_enriched_content_db
- **Username:** yugabyte
- **Password:** yugabyte

### YugabyteDB Management UI
- **Master UI:** http://localhost:7000
- **TServer UI:** http://localhost:9000

## Option 2: View Static OpenAPI Documentation

If you just want to view the API documentation without running the application:

1. **Open the static HTML viewer:**
   - Open `swagger-viewer.html` in your web browser
   - This will load the OpenAPI specification from `openapi.json`

2. **Or use an online Swagger Editor:**
   - Go to https://editor.swagger.io/
   - Copy the contents of `openapi.json`
   - Paste it into the editor

## API Endpoints Summary

### Chatbot Endpoints (`/api/chatbot`)
- `POST /api/chatbot/query` - AI-driven chatbot query
- `POST /api/chatbot/ai-search` - Explicit AI search endpoint  
- `POST /api/chatbot/query-legacy` - Legacy chatbot query

### Data Extraction Endpoints (`/api`)
- `GET /api/extract-cleanse-enrich-and-store?sourceUri={uri}` - Extract, cleanse, enrich and store data
- `POST /api/ingest-json-payload` - Ingest JSON payload
- `GET /api/cleansed-data-status/{id}` - Get cleansed data status

### Search Endpoints (`/api`)
- `GET /api/refine?query={query}` - Get refinement chips for a query
- `POST /api/search` - Vector search endpoint

## Access Points

When the application is running:
- **Swagger UI:** http://localhost:8080/swagger-ui.html
- **OpenAPI JSON:** http://localhost:8080/v3/api-docs
- **OpenAPI YAML:** http://localhost:8080/v3/api-docs.yaml

## Troubleshooting

### Database Connection Issues
If you see "Connection to localhost:5433 refused":
1. Make sure Docker is running
2. Run `docker-compose up -d` to start the database
3. Wait a few seconds for the database to fully start
4. Check if the database is running: `docker ps`

### Swagger UI Not Loading
1. Check that the application started successfully (look for "Started SpringbootApplication")
2. Verify the port is 8080 (check `application.properties`)
3. Try accessing the OpenAPI JSON directly: http://localhost:8080/v3/api-docs
4. Check browser console for errors (F12)

### Java Version Issues
- Ensure you have Java 17 or higher installed
- Verify with: `java -version`
- The project is configured for Java 17 (see `pom.xml`)

