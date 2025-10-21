#!/usr/bin/env bash
# reset_postgres_arch.sh — ultra-minimal hard-reset for Arch Linux PostgreSQL
# Based on the Mac/Homebrew version structure
#
# IMPORTANT: This script must be run AS the postgres user and assumes PostgreSQL is already stopped.
# This script contains NO sudo commands - completely runs as postgres user.
#
# Usage:
#   1. Stop PostgreSQL first:
#      sudo systemctl stop postgresql
#
#   2. Run this script as postgres user:
#      sudo -u postgres ./reset_postgres_arch.sh
#
#   3. Start PostgreSQL when done:
#      sudo systemctl start postgresql
#
set -euo pipefail

# ─────────── Configuration ───────────
DATA_DIR="/var/lib/postgres/data"
PG_USER="postgres"
PG_DB="buckaroo_db"
PG_PASS="secret"
PORT=5432
LOG_FILE="${DATA_DIR}/server.log"

# Check if port is already in use
if ss -tuln | grep -q ":${PORT} "; then
    echo "ERROR: Port ${PORT} is already in use!"
    echo "Please ensure PostgreSQL is stopped before running this script:"
    echo "  sudo systemctl stop postgresql"
    echo "  sudo pkill -9 -u postgres postgres"
    echo ""
    echo "Currently listening on port ${PORT}:"
    ss -tulnp | grep ":${PORT} " || true
    exit 1
fi

echo "Removing old cluster at ${DATA_DIR}..."
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
chmod 700 "${DATA_DIR}"

echo "Initialising new cluster..."
initdb -D "${DATA_DIR}" \
    --username="${PG_USER}" \
    --auth-local=trust --auth-host=trust

echo "Starting temporary server on port ${PORT}..."
if ! pg_ctl -D "${DATA_DIR}" -l "${LOG_FILE}" -o "-p ${PORT} -k ${DATA_DIR}" start; then
    echo "Postgres failed to start. Last 40 log lines:"
    tail -n 40 "${LOG_FILE}"
    exit 1
fi

# Wait until server is ready
echo "Waiting for server readiness..."
# Try for 10 seconds, then give up
COUNTER=0
until pg_isready -q -h localhost -p "${PORT}" 2>/dev/null; do
    sleep 0.5
    COUNTER=$((COUNTER + 1))
    if [ $COUNTER -gt 20 ]; then
        echo "Server failed to become ready after 10 seconds. Log output:"
        tail -n 50 "${LOG_FILE}"
        exit 1
    fi
done

echo "Setting password for '${PG_USER}'..."
psql -h localhost -U "${PG_USER}" -p "${PORT}" -d postgres \
    -c "ALTER ROLE ${PG_USER} WITH PASSWORD '${PG_PASS}';"

echo "Creating database '${PG_DB}'..."
createdb -h localhost -U "${PG_USER}" -p "${PORT}" "${PG_DB}" 2>/dev/null || true

echo "Stopping temporary server..."
pg_ctl -D "${DATA_DIR}" stop -m fast

echo "Done! Cluster reset and shut down."
echo
echo "To start Postgres again, run:"
echo "  sudo systemctl start postgresql"
