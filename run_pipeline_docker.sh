#!/usr/bin/env bash
# ======================================================================
# Football Manager Data Warehouse - Linux / Docker Pipeline Runner
# ======================================================================
# Purpose:
#   Orchestrates SQL Server inside Docker on Linux:
#     1. Ensures SQL Server container is up and accepting connections
#     2. Deploys DDL, stored procedures, and views in Medallion order
#     3. Executes the master warehouse ETL pipeline (Bronze -> Silver -> Gold)
#     4. Runs the complete automated test suite
#
# Usage:
#   ./run_pipeline_docker.sh
#   SEASON="2024/25" ./run_pipeline_docker.sh
# ======================================================================

set -euo pipefail

# ANSI Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${PROJECT_ROOT}"

# Load .env if present
if [[ -f .env ]]; then
    set -a
    source .env
    set +a
fi

CONTAINER_NAME="${CONTAINER_NAME:-sql_server}"
SA_PASSWORD="${MSSQL_SA_PASSWORD:-McMedal@14}"
PORT="${MSSQL_PORT:-1433}"
TARGET_SEASON="${SEASON:-2023/24}"

echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}  ⚽ FM24 DATA WAREHOUSE: LINUX & DOCKER ETL RUNNER  ${NC}"
echo -e "${BLUE}======================================================================${NC}"
echo -e "Container Name : ${YELLOW}${CONTAINER_NAME}${NC}"
echo -e "Target Season  : ${YELLOW}${TARGET_SEASON}${NC}"
echo -e "SQL Port       : ${YELLOW}${PORT}${NC}"
echo ""

# -------------------------------------------------------------------
# 1. Check Docker Daemon & Container
# -------------------------------------------------------------------
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}[ERROR] Docker daemon is not running or current user lacks permissions.${NC}"
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}[INFO] Container '${CONTAINER_NAME}' is not running.${NC}"
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${BLUE}>>> Starting existing container '${CONTAINER_NAME}'...${NC}"
        docker start "${CONTAINER_NAME}"
    else
        echo -e "${BLUE}>>> Starting SQL Server container with docker compose...${NC}"
        docker compose up -d
    fi
fi

# -------------------------------------------------------------------
# 2. Wait for SQL Server to be ready
# -------------------------------------------------------------------
echo -e "${BLUE}>>> Waiting for SQL Server engine to be healthy and ready...${NC}"
MAX_RETRIES=30
RETRY_COUNT=0
SQLCMD_BIN=""

# Detect sqlcmd path inside container (v18 vs legacy)
if docker exec "${CONTAINER_NAME}" test -x /opt/mssql-tools18/bin/sqlcmd; then
    SQLCMD_BIN="/opt/mssql-tools18/bin/sqlcmd"
elif docker exec "${CONTAINER_NAME}" test -x /opt/mssql-tools/bin/sqlcmd; then
    SQLCMD_BIN="/opt/mssql-tools/bin/sqlcmd"
else
    echo -e "${RED}[ERROR] sqlcmd not found in container.${NC}"
    exit 1
fi

run_sql() {
    local query="$1"
    local db="${2:-master}"
    docker exec "${CONTAINER_NAME}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -d "${db}" -Q "${query}"
}

run_sql_file() {
    local filepath="$1"
    local db="${2:-master}"
    docker exec "${CONTAINER_NAME}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -d "${db}" -i "${filepath}"
}

until run_sql "SELECT 1;" >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [[ ${RETRY_COUNT} -ge ${MAX_RETRIES} ]]; then
        echo -e "${RED}[ERROR] SQL Server did not become ready within 30 seconds.${NC}"
        exit 1
    fi
    sleep 1
done
echo -e "${GREEN}[OK] SQL Server is active and accepting connections.${NC}"

# -------------------------------------------------------------------
# 3. Prepare and Synchronize FM Data Exports inside Container
# -------------------------------------------------------------------
echo -e "${BLUE}>>> Preparing and synchronizing FM data exports...${NC}"
if command -v python3 >/dev/null 2>&1; then
    python3 "python scripts/prepare_ingestion.py" --target-dir documents/ 2>/dev/null || true
else
    echo -e "${YELLOW}[INFO] python3 not detected; using pre-packaged dataset in documents/.${NC}"
fi

echo -e "${BLUE}>>> Verifying volume mounts / container directories...${NC}"
# If /data or /scripts are not mounted as volumes, ensure they exist and copy files
docker exec -u 0 "${CONTAINER_NAME}" mkdir -p /data /scripts >/dev/null 2>&1 || true
docker cp documents/. "${CONTAINER_NAME}:/data/" >/dev/null 2>&1
docker cp "sql scripts/." "${CONTAINER_NAME}:/scripts/" >/dev/null 2>&1
docker exec -u 0 "${CONTAINER_NAME}" chown -R mssql:mssql /data /scripts >/dev/null 2>&1 || true
echo -e "${GREEN}[OK] Files synchronized to /data and /scripts in container.${NC}"

# -------------------------------------------------------------------
# 4. Deploy Medallion Warehouse Schema
# -------------------------------------------------------------------
echo ""
echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}  DEPLOYING MEDALLION SCHEMA & PROCEDURES  ${NC}"
echo -e "${BLUE}======================================================================${NC}"

steps=(
    "1/9: Database & Schemas (UTF-8)|/scripts/init_database.sql|master"
    "2/9: Bronze Layer DDL|/scripts/bronze/ddl-bronze.sql|DW"
    "3/9: Bronze Ingestion Procedure|/scripts/bronze/data-ingestion bronze.sql|DW"
    "4/9: Silver Layer DDL|/scripts/silver/ddl-silver.sql|DW"
    "5/9: Silver Cleaning Procedure|/scripts/silver/proc-silver.sql|DW"
    "6/9: Gold Dimensional DDL|/scripts/gold/ddl-gold.sql|DW"
    "7/9: Gold Loading Procedures|/scripts/gold/proc-gold.sql|DW"
    "8/9: Gold Views & Similarity Engines|/scripts/gold/views-gold.sql|DW"
    "9/9: Master Pipeline Orchestrator|/scripts/orchestration/run_pipeline.sql|DW"
)

for step in "${steps[@]}"; do
    IFS="|" read -r step_name script_path db_context <<< "${step}"
    echo -e "${BLUE}>>> Deploying [${step_name}]...${NC}"
    run_sql_file "${script_path}" "${db_context}" >/dev/null
    echo -e "${GREEN}    [SUCCESS] ${step_name} compiled.${NC}"
done

# -------------------------------------------------------------------
# 5. Execute Master Pipeline
# -------------------------------------------------------------------
echo ""
echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}  EXECUTING MASTER ETL PIPELINE (SEASON: ${TARGET_SEASON})  ${NC}"
echo -e "${BLUE}======================================================================${NC}"

EXEC_CMD="EXEC dbo.usp_run_warehouse_pipeline @Season = '${TARGET_SEASON}', @DataPath = '/data/', @RunTests = 1;"
docker exec "${CONTAINER_NAME}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -d DW -Q "${EXEC_CMD}"

# -------------------------------------------------------------------
# 6. Execute Automated Test Suites
# -------------------------------------------------------------------
echo ""
echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}  RUNNING AUTOMATED TEST SUITES  ${NC}"
echo -e "${BLUE}======================================================================${NC}"

test_suites=(
    "Data Warehouse Integrity & Sanity Suite|/scripts/tests/test_data_integrity.sql"
    "Positional Replacement Similarity Engines Suite|/scripts/tests/test_vw_similar_positions.sql"
    "Central Defender Analytical Reports Suite|/scripts/tests/test_vw_squad_central_defender_reports.sql"
)

ALL_TESTS_PASSED=true

for suite in "${test_suites[@]}"; do
    IFS="|" read -r suite_name suite_file <<< "${suite}"
    echo -e "${BLUE}>>> Running Test Suite: ${suite_name}...${NC}"
    if output=$(run_sql_file "${suite_file}" "DW"); then
        echo -e "${GREEN}    [PASS] ${suite_name}${NC}"
    else
        echo -e "${RED}    [FAIL] ${suite_name}${NC}"
        echo "${output}"
        ALL_TESTS_PASSED=false
    fi
done

echo ""
echo -e "${BLUE}======================================================================${NC}"
if [ "${ALL_TESTS_PASSED}" = true ]; then
    echo -e "${GREEN}  🎉 ALL PIPELINE TASKS AND TEST SUITES PASSED ON LINUX/DOCKER!  ${NC}"
else
    echo -e "${RED}  ❌ ONE OR MORE TEST SUITES FAILED. Check output above.  ${NC}"
    exit 1
fi
echo -e "${BLUE}======================================================================${NC}"
