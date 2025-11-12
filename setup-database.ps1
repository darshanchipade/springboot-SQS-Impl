# PostgreSQL Database Setup Script
# This script helps you set up the database for the Spring Boot application

Write-Host "=== PostgreSQL Database Setup ===" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check if PostgreSQL is installed
Write-Host "Step 1: Checking PostgreSQL installation..." -ForegroundColor Yellow
$pgPath = Get-ChildItem "C:\Program Files\PostgreSQL" -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $pgPath) {
    Write-Host "ERROR: PostgreSQL not found in C:\Program Files\PostgreSQL" -ForegroundColor Red
    Write-Host "Please install PostgreSQL or use Docker Compose (docker-compose.yml)" -ForegroundColor Yellow
    exit 1
}

$pgVersion = $pgPath.Name
$pgBinPath = Join-Path $pgPath.FullName "bin"
Write-Host "Found PostgreSQL version: $pgVersion" -ForegroundColor Green

# Step 2: Check if PostgreSQL service is running
Write-Host ""
Write-Host "Step 2: Checking PostgreSQL service..." -ForegroundColor Yellow
$pgService = Get-Service | Where-Object {$_.DisplayName -like "*PostgreSQL*" -or $_.Name -like "*postgres*"} | Select-Object -First 1

if ($pgService) {
    Write-Host "Found service: $($pgService.DisplayName) ($($pgService.Name))" -ForegroundColor Green
    if ($pgService.Status -eq "Running") {
        Write-Host "Service is already running" -ForegroundColor Green
    } else {
        Write-Host "Starting service..." -ForegroundColor Yellow
        Start-Service -Name $pgService.Name
        Start-Sleep -Seconds 3
        Write-Host "Service started" -ForegroundColor Green
    }
} else {
    Write-Host "WARNING: PostgreSQL service not found. You may need to start it manually." -ForegroundColor Yellow
    Write-Host "Try: Get-Service | Where-Object {`$_.DisplayName -like '*PostgreSQL*'}" -ForegroundColor Yellow
}

# Step 3: Find psql executable
Write-Host ""
Write-Host "Step 3: Finding psql executable..." -ForegroundColor Yellow
$psqlPath = Join-Path $pgBinPath "psql.exe"
if (-not (Test-Path $psqlPath)) {
    Write-Host "ERROR: psql.exe not found at $psqlPath" -ForegroundColor Red
    exit 1
}
Write-Host "Found psql at: $psqlPath" -ForegroundColor Green

# Step 4: Check PostgreSQL configuration for port
Write-Host ""
Write-Host "Step 4: Checking PostgreSQL port configuration..." -ForegroundColor Yellow
$pgDataPath = Join-Path $pgPath.FullName "data"
$pgConfPath = Join-Path $pgDataPath "postgresql.conf"

if (Test-Path $pgConfPath) {
    $portLine = Select-String -Path $pgConfPath -Pattern "^port\s*=" | Select-Object -First 1
    if ($portLine) {
        Write-Host "Current port configuration: $($portLine.Line)" -ForegroundColor Cyan
        if ($portLine.Line -match "port\s*=\s*(\d+)") {
            $currentPort = $matches[1]
            if ($currentPort -ne "5433") {
                Write-Host "WARNING: PostgreSQL is configured for port $currentPort, but application expects port 5433" -ForegroundColor Yellow
                Write-Host "You may need to:" -ForegroundColor Yellow
                Write-Host "  1. Edit $pgConfPath and change port to 5433" -ForegroundColor Yellow
                Write-Host "  2. Restart PostgreSQL service" -ForegroundColor Yellow
                Write-Host "  3. Or update application.properties to use port $currentPort" -ForegroundColor Yellow
            }
        }
    }
}

# Step 5: Connect to PostgreSQL and create database/user
Write-Host ""
Write-Host "Step 5: Setting up database and user..." -ForegroundColor Yellow
Write-Host "Attempting to connect to PostgreSQL (you may be prompted for password)..." -ForegroundColor Cyan
Write-Host ""

# Try to connect with default postgres user
$sqlCommands = @"
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
"@

Write-Host "SQL Commands to run:" -ForegroundColor Cyan
Write-Host $sqlCommands -ForegroundColor Gray
Write-Host ""

Write-Host "Please run these commands manually using psql:" -ForegroundColor Yellow
Write-Host "  $psqlPath -U postgres -p 5433" -ForegroundColor Cyan
Write-Host ""
Write-Host "Or run:" -ForegroundColor Yellow
Write-Host "  $psqlPath -U postgres -p 5433 -f setup-database.sql" -ForegroundColor Cyan
Write-Host ""

# Create SQL file
$sqlFile = "setup-database.sql"
$sqlCommands | Out-File -FilePath $sqlFile -Encoding UTF8
Write-Host "Created SQL file: $sqlFile" -ForegroundColor Green
Write-Host ""

# Step 6: Test connection
Write-Host "Step 6: Testing connection..." -ForegroundColor Yellow
Write-Host "To test the connection, run:" -ForegroundColor Cyan
Write-Host "  $psqlPath -U yugabyte -d bedrock_enriched_content_db -p 5433 -h localhost" -ForegroundColor Cyan
Write-Host ""

Write-Host "=== Setup Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Run the SQL commands above to create the database and user" -ForegroundColor White
Write-Host "2. Start your Spring Boot application: mvn spring-boot:run" -ForegroundColor White
Write-Host "3. Access Swagger UI: http://localhost:8080/swagger-ui.html" -ForegroundColor White







