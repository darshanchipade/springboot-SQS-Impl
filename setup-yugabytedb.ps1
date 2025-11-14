# YugabyteDB Setup Script
# This script helps you set up YugabyteDB for the Spring Boot application

Write-Host "=== YugabyteDB Setup ===" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check if YugabyteDB is installed
Write-Host "Step 1: Checking YugabyteDB installation..." -ForegroundColor Yellow

# Check common installation locations
$yugabytePaths = @(
    "$env:USERPROFILE\yugabyte",
    "$env:USERPROFILE\yugabyte-db",
    "C:\yugabyte",
    "C:\Program Files\Yugabyte",
    "$env:LOCALAPPDATA\Yugabyte"
)

$yugabyteHome = $null
foreach ($path in $yugabytePaths) {
    if (Test-Path $path) {
        $yugabyteHome = $path
        Write-Host "Found YugabyteDB at: $yugabyteHome" -ForegroundColor Green
        break
    }
}

if (-not $yugabyteHome) {
    Write-Host "YugabyteDB not found in common locations." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Installation options:" -ForegroundColor Cyan
    Write-Host "1. Download from: https://www.yugabyte.com/download/" -ForegroundColor White
    Write-Host "2. Use Docker: docker run -d -p 5433:5433 yugabytedb/yugabyte:latest" -ForegroundColor White
    Write-Host "3. Or use the docker-compose.yml file provided" -ForegroundColor White
    Write-Host ""
    
    # Check if docker-compose.yml exists
    if (Test-Path "docker-compose.yml") {
        Write-Host "Found docker-compose.yml - you can use Docker Compose:" -ForegroundColor Green
        Write-Host "  docker-compose up -d" -ForegroundColor Cyan
        Write-Host ""
    }
    
    exit 1
}

# Step 2: Check if YugabyteDB service is running
Write-Host ""
Write-Host "Step 2: Checking YugabyteDB service..." -ForegroundColor Yellow
$yugabyteService = Get-Service | Where-Object {
    $_.Name -like "*yugabyte*" -or 
    $_.DisplayName -like "*yugabyte*" -or
    $_.DisplayName -like "*Yugabyte*"
} | Select-Object -First 1

if ($yugabyteService) {
    Write-Host "Found service: $($yugabyteService.DisplayName) ($($yugabyteService.Name))" -ForegroundColor Green
    if ($yugabyteService.Status -eq "Running") {
        Write-Host "Service is already running" -ForegroundColor Green
    } else {
        Write-Host "Starting service..." -ForegroundColor Yellow
        Start-Service -Name $yugabyteService.Name
        Start-Sleep -Seconds 5
        Write-Host "Service started" -ForegroundColor Green
    }
} else {
    Write-Host "No Windows service found. YugabyteDB may be running as a process or via Docker." -ForegroundColor Yellow
}

# Step 3: Check if YugabyteDB is listening on port 5433
Write-Host ""
Write-Host "Step 3: Checking if YugabyteDB is listening on port 5433..." -ForegroundColor Yellow
$port5433 = Get-NetTCPConnection -LocalPort 5433 -ErrorAction SilentlyContinue
if ($port5433) {
    Write-Host "Port 5433 is in use - YugabyteDB may already be running" -ForegroundColor Green
} else {
    Write-Host "Port 5433 is not in use - YugabyteDB may need to be started" -ForegroundColor Yellow
    Write-Host "If you have YugabyteDB installed, start it manually" -ForegroundColor Yellow
}

# Step 4: Find ysqlsh or psql executable
Write-Host ""
Write-Host "Step 4: Finding ysqlsh/psql executable..." -ForegroundColor Yellow

$binPaths = @(
    Join-Path $yugabyteHome "bin",
    Join-Path $yugabyteHome "postgres\bin",
    Join-Path $yugabyteHome "tserver\bin"
)

$ysqlshPath = $null
foreach ($binPath in $binPaths) {
    $testPath = Join-Path $binPath "ysqlsh.exe"
    if (Test-Path $testPath) {
        $ysqlshPath = $testPath
        Write-Host "Found ysqlsh at: $ysqlshPath" -ForegroundColor Green
        break
    }
}

if (-not $ysqlshPath) {
    # Try psql as fallback
    $psqlPath = Get-Command psql -ErrorAction SilentlyContinue
    if ($psqlPath) {
        $ysqlshPath = $psqlPath.Source
        Write-Host "Using psql at: $ysqlshPath" -ForegroundColor Green
    } else {
        Write-Host "WARNING: Could not find ysqlsh or psql executable" -ForegroundColor Yellow
    }
}

# Step 5: Create database and user
Write-Host ""
Write-Host "Step 5: Setting up database and user..." -ForegroundColor Yellow
Write-Host ""

if ($ysqlshPath) {
    Write-Host "To create the database and user, run:" -ForegroundColor Cyan
    Write-Host "  $ysqlshPath -h localhost -p 5433 -U postgres -f setup-yugabytedb.sql" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Or connect interactively:" -ForegroundColor Cyan
    Write-Host "  $ysqlshPath -h localhost -p 5433 -U postgres" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host "Please run the SQL commands from setup-yugabytedb.sql manually" -ForegroundColor Yellow
}

Write-Host "=== Setup Instructions ===" -ForegroundColor Green
Write-Host ""
Write-Host "1. Start YugabyteDB (if not already running)" -ForegroundColor White
Write-Host "2. Run the SQL setup script: setup-yugabytedb.sql" -ForegroundColor White
Write-Host "3. Start your Spring Boot application: mvn spring-boot:run" -ForegroundColor White
Write-Host "4. Access Swagger UI: http://localhost:8080/swagger-ui.html" -ForegroundColor White






