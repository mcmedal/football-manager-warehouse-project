# ==============================================================================
# FM Analytics Web Application - Windows Quick Startup Script
# ==============================================================================
# Purpose:
#   Checks that the SQL Server Docker container is running, ensures Node.js
#   is available, installs dependencies if needed, and starts the Next.js app.
#
# Usage (in PowerShell):
#   .\run_webapp.ps1
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "⚽ Starting FM Analytics Web App (FBref & Opta Scouting Engine)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# 1. Verify Docker container is up
$runningContainers = docker ps --format '{{.Names}}' 2>$null
if ($runningContainers -notcontains "sql_server") {
    Write-Host ">>> [INFO] SQL Server container is not running. Starting via docker compose..." -ForegroundColor Yellow
    docker compose up -d sql-server | Out-Null
    Write-Host ">>> Waiting for SQL Server to become healthy..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
} else {
    Write-Host ">>> [OK] SQL Server container ('sql_server') is running." -ForegroundColor Green
}

# 2. Check Node environment
$nodeCmd = Get-Command node -ErrorAction SilentlyContinue
if (-not $nodeCmd) {
    Write-Host ">>> [ERROR] Node.js is not found in your system PATH." -ForegroundColor Red
    Write-Host "Please download and install Node.js (LTS version) from:" -ForegroundColor Yellow
    Write-Host "https://nodejs.org/" -ForegroundColor White
    Write-Host "After installing, close and reopen PowerShell, then run .\run_webapp.ps1 again." -ForegroundColor Yellow
    exit 1
}

$nodeVersion = node -v
$npmVersion = npm -v
Write-Host ">>> [OK] Using Node $nodeVersion and npm $npmVersion" -ForegroundColor Green

# 3. Check node_modules in webapp
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$webappDir = Join-Path $scriptDir "webapp"

if (-not (Test-Path (Join-Path $webappDir "node_modules"))) {
    Write-Host ">>> [INFO] Installing webapp dependencies (first time only)..." -ForegroundColor Cyan
    Push-Location $webappDir
    try {
        npm install
    } finally {
        Pop-Location
    }
}

# 4. Check .env.local
$envLocalPath = Join-Path $webappDir ".env.local"
if (-not (Test-Path $envLocalPath)) {
    Write-Host ">>> [INFO] Creating .env.local with default Docker SQL credentials..." -ForegroundColor Cyan
    @"
MSSQL_USER=sa
MSSQL_PASSWORD=McMedal@14
MSSQL_SERVER=localhost
MSSQL_PORT=1433
MSSQL_DATABASE=DW
"@ | Out-File -FilePath $envLocalPath -Encoding utf8
}

# 5. Start Next.js Development Server
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "🚀 Booting Next.js Analytics App on http://localhost:3000 ..." -ForegroundColor Green
Write-Host "   - Scouting Hub:         http://localhost:3000/" -ForegroundColor White
Write-Host "   - TradingView Screener: http://localhost:3000/screener" -ForegroundColor White
Write-Host "   - Head-to-Head:         http://localhost:3000/compare" -ForegroundColor White
Write-Host "   - Tactical Archetypes:  http://localhost:3000/archetypes" -ForegroundColor White
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop the web application." -ForegroundColor Yellow
Write-Host ""

Push-Location $webappDir
try {
    npm run dev
} finally {
    Pop-Location
}
