# Script to start Docker Desktop and wait for it to be ready

Write-Host "Starting Docker Desktop..." -ForegroundColor Cyan

$dockerPath = "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
if (-not (Test-Path $dockerPath)) {
    $dockerPath = "${env:ProgramFiles(x86)}\Docker\Docker\Docker Desktop.exe"
}

if (-not (Test-Path $dockerPath)) {
    Write-Host "ERROR: Docker Desktop not found!" -ForegroundColor Red
    Write-Host "Please install Docker Desktop from: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}

# Check if Docker Desktop is already running
$dockerProcess = Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue
if ($dockerProcess) {
    Write-Host "Docker Desktop appears to be running" -ForegroundColor Green
} else {
    Write-Host "Starting Docker Desktop..." -ForegroundColor Yellow
    Start-Process $dockerPath
    Write-Host "Waiting for Docker Desktop to start (this may take 30-60 seconds)..." -ForegroundColor Yellow
}

# Wait for Docker to be ready
$maxAttempts = 60
$attempt = 0
$dockerReady = $false

while ($attempt -lt $maxAttempts -and -not $dockerReady) {
    Start-Sleep -Seconds 2
    $attempt++
    
    try {
        $result = docker ps 2>&1
        if ($LASTEXITCODE -eq 0) {
            $dockerReady = $true
            Write-Host "Docker is ready!" -ForegroundColor Green
        } else {
            Write-Host "." -NoNewline -ForegroundColor Gray
        }
    } catch {
        Write-Host "." -NoNewline -ForegroundColor Gray
    }
}

if (-not $dockerReady) {
    Write-Host ""
    Write-Host "Docker Desktop may still be starting. Please wait a bit longer and try again." -ForegroundColor Yellow
    Write-Host "You can check Docker Desktop status in the system tray." -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "Docker is ready! You can now run:" -ForegroundColor Green
    Write-Host "  docker-compose up -d" -ForegroundColor Cyan
}






