#!/usr/bin/env bash
# ==============================================================================
# FM Analytics Web Application - Quick Startup Script
# ==============================================================================
# Purpose:
#   Checks that the SQL Server Docker container is running, ensures the Node
#   environment is configured, and boots up the Next.js analytics web app.
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$SCRIPT_DIR/webapp"

echo "======================================================================"
echo "⚽ Starting FM Analytics Web App (FBref & Opta Scouting Engine)"
echo "======================================================================"

# 1. Verify Docker container is up
if ! docker ps --format '{{.Names}}' | grep -q "^sql_server$"; then
    echo ">>> [INFO] SQL Server container is not running. Starting via docker-compose..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d sql-server
    echo ">>> Waiting for SQL Server to become healthy..."
    sleep 5
else
    echo ">>> [OK] SQL Server container ('sql_server') is running."
fi

# 2. Check Node environment
export PATH="$HOME/.local/bin:$PATH"
if ! command -v node >/dev/null 2>&1; then
    echo ">>> [ERROR] Node.js is not found in PATH."
    exit 1
fi

echo ">>> [OK] Using Node $(node -v) and npm $(npm -v)"

# 3. Check node_modules in webapp
if [ ! -d "$WEBAPP_DIR/node_modules" ]; then
    echo ">>> [INFO] Installing webapp dependencies..."
    (cd "$WEBAPP_DIR" && npm install)
fi

# 4. Check .env.local
if [ ! -f "$WEBAPP_DIR/.env.local" ]; then
    echo ">>> [INFO] Creating .env.local with default Docker SQL credentials..."
    cat <<EOF > "$WEBAPP_DIR/.env.local"
MSSQL_USER=sa
MSSQL_PASSWORD=McMedal@14
MSSQL_SERVER=localhost
MSSQL_PORT=1433
MSSQL_DATABASE=DW
EOF
fi

# 5. Start Next.js Development Server
echo "======================================================================"
echo "🚀 Booting Next.js Analytics App on http://localhost:3000 ..."
echo "   - Scouting Hub:        http://localhost:3000/"
echo "   - TradingView Screener: http://localhost:3000/screener"
echo "   - Head-to-Head:        http://localhost:3000/compare"
echo "   - Tactical Archetypes: http://localhost:3000/archetypes"
echo "======================================================================"
echo "Press Ctrl+C to stop the web application."
echo ""

cd "$WEBAPP_DIR"
exec npm run dev
