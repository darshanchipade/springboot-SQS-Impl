# YugabyteDB Setup Guide

This guide helps you set up YugabyteDB for your Spring Boot application.

## Quick Start with Docker Compose (Recommended)

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
   
   **Option A: Using Docker exec**
   ```powershell
   docker exec -it springboot-yugabytedb ysqlsh -h localhost -p 5433 -U postgres -f /tmp/setup-yugabytedb.sql
   ```
   
   **Option B: Copy SQL file and run manually**
   ```powershell
   # Copy the SQL file into the container
   docker cp setup-yugabytedb.sql springboot-yugabytedb:/tmp/
   
   # Connect to YugabyteDB
   docker exec -it springboot-yugabytedb ysqlsh -h localhost -p 5433 -U postgres
   ```
   
   Then paste the contents of `setup-yugabytedb.sql` or run:
   ```sql
   \i /tmp/setup-yugabytedb.sql
   ```

4. **Verify the setup:**
   ```powershell
   docker exec -it springboot-yugabytedb ysqlsh -h localhost -p 5433 -U yugabyte -d bedrock_enriched_content_db
   ```
   
   You should be able to connect. Type `\q` to exit.

5. **Start your Spring Boot application:**
   ```powershell
   mvn spring-boot:run
   ```

6. **Access Swagger UI:**
   - Open: `http://localhost:8080/swagger-ui.html`

## Alternative: Install YugabyteDB Locally

If you prefer to install YugabyteDB locally:

1. **Download YugabyteDB:**
   - Visit: https://www.yugabyte.com/download/
   - Download the Windows installer or use the ZIP package

2. **Extract and set up:**
   - Extract to a location like `C:\yugabyte` or `%USERPROFILE%\yugabyte`
   - Add the `bin` directory to your PATH

3. **Start YugabyteDB:**
   ```powershell
   # Navigate to YugabyteDB directory
   cd C:\yugabyte
   
   # Start YugabyteDB
   .\bin\yugabyted start
   ```

4. **Run the setup script:**
   ```powershell
   .\bin\ysqlsh -h localhost -p 5433 -U postgres -f setup-yugabytedb.sql
   ```

## Database Connection Details

- **Host:** localhost
- **Port:** 5433
- **Database:** bedrock_enriched_content_db
- **Username:** yugabyte
- **Password:** yugabyte

## YugabyteDB Management UI

When running with Docker Compose, you can access:
- **Master UI:** http://localhost:7000
- **TServer UI:** http://localhost:9000

## Troubleshooting

### Port 5433 Already in Use
If port 5433 is already in use:
```powershell
# Check what's using the port
netstat -ano | findstr :5433

# Or stop any existing YugabyteDB instances
docker stop springboot-yugabytedb
```

### Docker Container Not Starting
```powershell
# Check logs
docker-compose logs yugabytedb

# Restart the container
docker-compose restart yugabytedb
```

### Connection Refused
1. Verify YugabyteDB is running:
   ```powershell
   docker ps | Select-String yugabyte
   ```

2. Check if port 5433 is listening:
   ```powershell
   netstat -an | findstr 5433
   ```

3. Wait a bit longer - YugabyteDB can take 30-60 seconds to fully start

## Stop YugabyteDB

When you're done:
```powershell
docker-compose down
```

To stop and remove volumes (WARNING: This deletes all data):
```powershell
docker-compose down -v
```






