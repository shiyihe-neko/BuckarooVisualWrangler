#!/bin/bash
# setup_buckaroo_db.sh — Create Buckaroo database and user (idempotent)
#
# Purpose:
#   Sets up the buckaroo_db database and buckaroo_user for the application.
#   Safe to run multiple times - checks if resources exist before creating.
#   ✓ Works on both Linux and Mac!
#
# Prerequisites:
#   - PostgreSQL must be running
#     Linux: sudo systemctl start postgresql
#     Mac:   brew services start postgresql@14
#   - postgres superuser must have password set to 'secret'
#
# Usage:
#   ./setup_buckaroo_db.sh
#
# Typical workflow (Linux):
#   1. Reset PostgreSQL: sudo -u postgres ./reset_postgres_arch.sh
#   2. Start PostgreSQL: sudo systemctl start postgresql
#   3. Run this script: ./setup_buckaroo_db.sh
#
# Typical workflow (Mac):
#   1. Reset PostgreSQL: ./new_instructions/reset_postgres.sh
#   2. Start PostgreSQL: brew services start postgresql@14
#   3. Run this script: ./setup_buckaroo_db.sh

set -euo pipefail

# ─────────── Configuration ───────────
HOST="localhost"
PORT="5432"
SUPERUSER="postgres"
SUPERPASS="secret"
DB_NAME="buckaroo_db"
NEW_USER="buckaroo_user"
NEW_PASS="secret"

# ─────────── Create Application User ───────────
echo "Creating user '${NEW_USER}' if it doesn't exist..."
PGPASSWORD="$SUPERPASS" psql -h "$HOST" -p "$PORT" -U "$SUPERUSER" -d postgres <<EOF
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$NEW_USER') THEN
      CREATE USER $NEW_USER WITH PASSWORD '$NEW_PASS';
      RAISE NOTICE 'User ${NEW_USER} created';
   ELSE
      RAISE NOTICE 'User ${NEW_USER} already exists';
   END IF;
END
\$\$;
EOF

# ─────────── Create Application Database ───────────
echo "Creating database '${DB_NAME}' if it doesn't exist..."
# Check if database exists
if PGPASSWORD="$SUPERPASS" psql -h "$HOST" -p "$PORT" -U "$SUPERUSER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1; then
  echo "Database '${DB_NAME}' already exists"
else
  PGPASSWORD="$SUPERPASS" psql -h "$HOST" -p "$PORT" -U "$SUPERUSER" -d postgres -c "CREATE DATABASE $DB_NAME OWNER $NEW_USER;"
  echo "Database '${DB_NAME}' created"
fi

echo ""
echo "✓ Setup complete!"
echo "  Database: ${DB_NAME}"
echo "  User: ${NEW_USER}"
echo "  Connection string: postgresql://${NEW_USER}:${NEW_PASS}@${HOST}:${PORT}/${DB_NAME}"
