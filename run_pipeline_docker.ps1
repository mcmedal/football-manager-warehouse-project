# ======================================================================
# Football Manager Data Warehouse - Windows / Docker Pipeline Runner
# ======================================================================
# Purpose:
#   Orchestrates SQL Server inside Docker on Windows:
#     1. Checks Docker Desktop is active
#     2. Starts SQL Server container via docker compose
#     3. Synchronizes FM data exports into the container
#     4. Deploys database DDL, stored procedures, and views in order
#     5. Executes the master warehouse ETL pipeline (Bronze -> Silver -> Gold)
#     6. Runs automated data integrity and similarity test suites
#
# Usage (in PowerShell):
#   .\run_pipeline_docker.ps1
#   .\run_pipeline_docker.ps1 -Season "2024/25"
# ======================================================================

param (
    [string]$Season = "2023/24",
    [string]$ContainerName = "sql_server",
    [string]$SaPassword = "McMedal@14",
    [string]$Port = "1433"
)

$ErrorActionPreference = "Stop"

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  ⚽ FM24 DATA WAREHOUSE: WINDOWS & DOCKER ETL RUNNER" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Container Name : $ContainerName" -ForegroundColor Yellow
Write-Host "Target Season  : $Season" -ForegroundColor Yellow
Write-Host "SQL Port       : $Port" -ForegroundColor Yellow
Write-Host ""

# -------------------------------------------------------------------
# 1. Check Docker Desktop
# -------------------------------------------------------------------
try {
    docker info > $null 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Docker not responding" }
} catch {
    Write-Host "[ERROR] Docker Desktop is not running or not installed." -ForegroundColor Red
    Write-Host "Please start Docker Desktop from your Start Menu, wait until the icon in your taskbar says 'Engine running', and try again." -ForegroundColor Yellow
    Write-Host "Download Docker Desktop for Windows: https://www.docker.com/products/docker-desktop/" -ForegroundColor White
    exit 1
}

# -------------------------------------------------------------------
# 2. Check & Start SQL Server Container
# -------------------------------------------------------------------
$runningContainers = docker ps --format '{{.Names}}'
if ($runningContainers -notcontains $ContainerName) {
    $allContainers = docker ps -a --format '{{.Names}}'
    if ($allContainers -contains $ContainerName) {
        Write-Host ">>> Starting existing container '$ContainerName'..." -ForegroundColor Cyan
        docker start $ContainerName | Out-Null
    } else {
        Write-Host ">>> Starting SQL Server container with docker compose..." -ForegroundColor Cyan
        docker compose up -d | Out-Null
    }
}

# -------------------------------------------------------------------
# 3. Wait for SQL Server to be ready
# -------------------------------------------------------------------
Write-Host ">>> Waiting for SQL Server engine to be healthy and ready..." -ForegroundColor Cyan
$maxRetries = 30
$retryCount = 0
$isReady = $false

# Detect sqlcmd path inside container
$sqlCmdBin = "/opt/mssql-tools18/bin/sqlcmd"
docker exec $ContainerName test -x /opt/mssql-tools18/bin/sqlcmd 2>$null
if ($LASTEXITCODE -ne 0) {
    $sqlCmdBin = "/opt/mssql-tools/bin/sqlcmd"
}

function Run-Sql($query, $db = "master") {
    docker exec $ContainerName $sqlCmdBin -S localhost -U sa -P $SaPassword -C -d $db -Q $query
}

function Run-SqlFile($filepath, $db = "master") {
    docker exec $ContainerName $sqlCmdBin -S localhost -U sa -P $SaPassword -C -d $db -i $filepath
}

while ($retryCount -lt $maxRetries) {
    try {
        docker exec $ContainerName $sqlCmdBin -S localhost -U sa -P $SaPassword -C -d master -Q "SELECT 1;" > $null 2>&1
        if ($LASTEXITCODE -eq 0) {
            $isReady = $true
            break
        }
    } catch {}
    $retryCount++
    Start-Sleep -Seconds 1
}

if (-not $isReady) {
    Write-Host "[ERROR] SQL Server did not become ready within 30 seconds." -ForegroundColor Red
    exit 1
}
Write-Host "[OK] SQL Server is active and accepting connections." -ForegroundColor Green

# -------------------------------------------------------------------
# 4. Synchronize FM Data Exports into Container
# -------------------------------------------------------------------
Write-Host ">>> Synchronizing documents/ and sql scripts/ to container..." -ForegroundColor Cyan
docker exec -u 0 $ContainerName mkdir -p /data /scripts > $null 2>&1
docker cp documents/. "${ContainerName}:/data/" > $null 2>&1
docker cp "sql scripts/." "${ContainerName}:/scripts/" > $null 2>&1
docker exec -u 0 $ContainerName chown -R mssql:mssql /data /scripts > $null 2>&1
Write-Host "[OK] Files synchronized to /data and /scripts in container." -ForegroundColor Green

# -------------------------------------------------------------------
# 5. Deploy Medallion Warehouse Schema
# -------------------------------------------------------------------
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  DEPLOYING MEDALLION SCHEMA & PROCEDURES" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

$steps = @(
    @{ Name = "1/9: Database & Schemas (UTF-8)"; File = "/scripts/init_database.sql"; DB = "master" },
    @{ Name = "2/9: Bronze Layer DDL"; File = "/scripts/bronze/ddl-bronze.sql"; DB = "DW" },
    @{ Name = "3/9: Bronze Ingestion Procedure"; File = "/scripts/bronze/data-ingestion bronze.sql"; DB = "DW" },
    @{ Name = "4/9: Silver Layer DDL"; File = "/scripts/silver/ddl-silver.sql"; DB = "DW" },
    @{ Name = "5/9: Silver Cleaning Procedure"; File = "/scripts/silver/proc-silver.sql"; DB = "DW" },
    @{ Name = "6/9: Gold Dimensional DDL"; File = "/scripts/gold/ddl-gold.sql"; DB = "DW" },
    @{ Name = "7/9: Gold Loading Procedures"; File = "/scripts/gold/proc-gold.sql"; DB = "DW" },
    @{ Name = "8/9: Gold Views & Similarity Engines"; File = "/scripts/gold/views-gold.sql"; DB = "DW" },
    @{ Name = "9/9: Master Pipeline Orchestrator"; File = "/scripts/orchestration/run_pipeline.sql"; DB = "DW" }
)

foreach ($step in $steps) {
    Write-Host ">>> Deploying [$($step.Name)]..." -ForegroundColor Cyan
    Run-SqlFile $step.File $step.DB | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "    [ERROR] Failed to compile $($step.Name)" -ForegroundColor Red
        exit 1
    }
    Write-Host "    [SUCCESS] $($step.Name) compiled." -ForegroundColor Green
}

# -------------------------------------------------------------------
# 6. Execute Master Pipeline
# -------------------------------------------------------------------
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  EXECUTING MASTER ETL PIPELINE (SEASON: $Season)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

$execCmd = "EXEC dbo.usp_run_warehouse_pipeline @Season = '$Season', @DataPath = '/data/', @RunTests = 1;"
docker exec $ContainerName $sqlCmdBin -S localhost -U sa -P $SaPassword -C -d DW -Q $execCmd

# -------------------------------------------------------------------
# 7. Run Automated Test Suites
# -------------------------------------------------------------------
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  RUNNING AUTOMATED TEST SUITES" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

$testSuites = @(
    @{ Name = "Data Warehouse Integrity & Sanity Suite"; File = "/scripts/tests/test_data_integrity.sql" },
    @{ Name = "Positional Replacement Similarity Engines Suite"; File = "/scripts/tests/test_vw_similar_positions.sql" },
    @{ Name = "Central Defender Analytical Reports Suite"; File = "/scripts/tests/test_vw_squad_central_defender_reports.sql" }
)

$allTestsPassed = $true

foreach ($suite in $testSuites) {
    Write-Host ">>> Running Test Suite: $($suite.Name)..." -ForegroundColor Cyan
    $testOutput = Run-SqlFile $suite.File "DW"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    [PASS] $($suite.Name)" -ForegroundColor Green
    } else {
        Write-Host "    [FAIL] $($suite.Name)" -ForegroundColor Red
        Write-Host $testOutput
        $allTestsPassed = $false
    }
}

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
if ($allTestsPassed) {
    Write-Host "  🎉 ALL PIPELINE TASKS AND TEST SUITES PASSED ON WINDOWS/DOCKER!" -ForegroundColor Green
    Write-Host ""
    Write-Host "  👉 Next Step: Launch the Web App by running:" -ForegroundColor Yellow
    Write-Host "     .\run_webapp.ps1" -ForegroundColor White
} else {
    Write-Host "  ❌ ONE OR MORE TEST SUITES FAILED. Check output above." -ForegroundColor Red
    exit 1
}
Write-Host "======================================================================" -ForegroundColor Cyan
