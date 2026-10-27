#!/usr/bin/env bash
# ======================================================================
# Football Manager Data Warehouse - Data Preparation & Container Sync
# ======================================================================
# Purpose:
#   Prepares raw Football Manager exports for warehouse ingestion:
#     1. Converts HTML exports to clean UTF-8 CSVs
#     2. Normalizes goalkeeper and outfield player schemas
#     3. Synchronizes standardized CSVs from documents/ to SQL Server container (/data/)
#     4. (Optional) Directly runs dbo.usp_run_warehouse_pipeline for the target season
#
# Usage:
#   ./prepare_data.sh
#   ./prepare_data.sh --season "2024/25"
#   SEASON="2024/25" ./prepare_data.sh
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
TARGET_SEASON="${SEASON:-}"
SOURCE_DIR="${FM_DATA_DIR:-}"

# Parse optional arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--season)
            TARGET_SEASON="$2"
            shift 2
            ;;
        --src|--source-dir)
            SOURCE_DIR="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: ./prepare_data.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -s, --season <SEASON>       Automatically execute dbo.usp_run_warehouse_pipeline for the given season (e.g. '2024/25')"
            echo "  --src, --source-dir <PATH>  Specify path to directory containing raw FM24 exports"
            echo "  -h, --help                  Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}[ERROR] Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}  ⚽ FM24 DATA PREPARATION & CONTAINER SYNCHRONIZATION  ${NC}"
echo -e "${BLUE}======================================================================${NC}"

# 1. Run Python Normalization & HTML -> CSV Conversion
echo -e "${BLUE}>>> Converting HTML exports and normalizing schemas...${NC}"
PREPARE_ARGS=("--target-dir" "documents/")
if [[ -n "${TARGET_SEASON}" ]]; then
    PREPARE_ARGS+=("--season" "${TARGET_SEASON}")
fi
if [[ -n "${SOURCE_DIR}" ]]; then
    PREPARE_ARGS+=("--source-dir" "${SOURCE_DIR}")
fi
python3 "python scripts/prepare_ingestion.py" "${PREPARE_ARGS[@]}"

# 2. Check Docker Container Status & Sync Files
if docker info >/dev/null 2>&1; then
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${BLUE}>>> Syncing documents/ and sql scripts/ to container '${CONTAINER_NAME}'...${NC}"
        docker exec -u 0 "${CONTAINER_NAME}" mkdir -p /data /scripts >/dev/null 2>&1 || true
        # Remove stale partition files from container to match current export exactly
        docker exec -u 0 "${CONTAINER_NAME}" bash -c "rm -f /data/interested_out_players*.csv" >/dev/null 2>&1 || true
        docker cp documents/. "${CONTAINER_NAME}:/data/" >/dev/null 2>&1
        docker cp "sql scripts/." "${CONTAINER_NAME}:/scripts/" >/dev/null 2>&1
        docker exec -u 0 "${CONTAINER_NAME}" chown -R mssql:mssql /data /scripts >/dev/null 2>&1 || true
        echo -e "${GREEN}[OK] Files synchronized to /data/ and /scripts/ inside container '${CONTAINER_NAME}'.${NC}"

        # Ensure latest stored procedures (with CREATE OR ALTER) are compiled non-destructively
        echo -e "${BLUE}>>> Refreshing stored procedures non-destructively (CREATE OR ALTER)...${NC}"
        for sp_script in "/scripts/silver/proc-silver.sql" "/scripts/gold/proc-gold.sql" "/scripts/gold/views-gold.sql" "/scripts/orchestration/run_pipeline.sql"; do
            docker exec "${CONTAINER_NAME}" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -C -d DW -i "${sp_script}" >/dev/null 2>&1 || true
        done
        echo -e "${GREEN}[OK] Stored procedures compiled.${NC}"
    else
        echo -e "${YELLOW}[WARNING] Container '${CONTAINER_NAME}' is not currently running.${NC}"
        echo -e "${YELLOW}          Files are ready in documents/ on host. Start container and re-run to sync.${NC}"
    fi
else
    echo -e "${YELLOW}[INFO] Docker daemon not running or not in Docker environment.${NC}"
    echo -e "${GREEN}[OK] Standardized CSV files are prepared in 'documents/'.${NC}"
fi

# 3. If --season was specified, execute the pipeline without schema reset
if [[ -n "${TARGET_SEASON}" ]]; then
    echo ""
    echo -e "${BLUE}======================================================================${NC}"
    echo -e "${BLUE}  EXECUTING MASTER ETL PIPELINE (SEASON: ${TARGET_SEASON})  ${NC}"
    echo -e "${BLUE}======================================================================${NC}"

    # Detect sqlcmd path inside container (v18 vs legacy)
    SQLCMD_BIN=""
    if docker exec "${CONTAINER_NAME}" test -x /opt/mssql-tools18/bin/sqlcmd 2>/dev/null; then
        SQLCMD_BIN="/opt/mssql-tools18/bin/sqlcmd"
    elif docker exec "${CONTAINER_NAME}" test -x /opt/mssql-tools/bin/sqlcmd 2>/dev/null; then
        SQLCMD_BIN="/opt/mssql-tools/bin/sqlcmd"
    else
        echo -e "${RED}[ERROR] sqlcmd not found in container.${NC}"
        exit 1
    fi

    EXEC_CMD="EXEC dbo.usp_run_warehouse_pipeline @Season = '${TARGET_SEASON}', @DataPath = '/data/', @RunTests = 1;"
    docker exec "${CONTAINER_NAME}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -d DW -Q "${EXEC_CMD}"

    echo ""
    echo -e "${GREEN}======================================================================${NC}"
    echo -e "${GREEN}  🎉 SEASON '${TARGET_SEASON}' SUCCESSFULLY INGESTED!  ${NC}"
    echo -e "${GREEN}======================================================================${NC}"
else
    echo ""
    echo -e "${GREEN}======================================================================${NC}"
    echo -e "${GREEN}  🎉 DATA IS READY FOR WAREHOUSE INGESTION!  ${NC}"
    echo -e "${GREEN}======================================================================${NC}"
    echo -e "You can now run the pipeline in SSMS, Azure Data Studio, or sqlcmd:"
    echo ""
    echo -e "    ${YELLOW}EXEC dbo.usp_run_warehouse_pipeline ${NC}"
    echo -e "    ${YELLOW}    @Season = '2024/25', ${NC}"
    echo -e "    ${YELLOW}    @RunTests = 1;${NC}"
    echo ""
    echo -e "Or execute it directly via this script:"
    echo -e "    ${YELLOW}./prepare_data.sh --season 2024/25${NC}"
    echo -e "${GREEN}======================================================================${NC}"
fi
