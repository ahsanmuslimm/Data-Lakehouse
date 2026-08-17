#!/bin/bash
# ============================================================================
# Schema Initialization Container Entrypoint
# ============================================================================
# Purpose: Wait for PostgreSQL to be fully ready, then execute schema
#          initialization SQL script. Exits with 0 on success.
#
# Requirements: 8.8
# ============================================================================

set -e  # Exit on any error

# Ensure required environment variables are set
if [ -z "$POSTGRES_HOST" ] || [ -z "$POSTGRES_USER" ] || [ -z "$POSTGRES_DB" ]; then
    echo "ERROR: Required environment variables not set (POSTGRES_HOST, POSTGRES_USER, POSTGRES_DB)"
    exit 1
fi

# ============================================================================
# WAIT FOR POSTGRESQL
# ============================================================================

echo "Waiting for PostgreSQL at $POSTGRES_HOST:$POSTGRES_PORT to be ready..."

# Set connection timeout and retry parameters
MAX_RETRIES=30
RETRY_INTERVAL=2
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_RETRIES ]; do
    ATTEMPT=$((ATTEMPT + 1))
    
    if PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$POSTGRES_HOST" \
        -p "${POSTGRES_PORT:-5432}" \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -c "SELECT 1" > /dev/null 2>&1; then
        
        echo "✓ PostgreSQL is ready after $ATTEMPT attempts"
        break
    fi
    
    if [ $ATTEMPT -lt $MAX_RETRIES ]; then
        echo "  Attempt $ATTEMPT/$MAX_RETRIES: PostgreSQL not ready, waiting ${RETRY_INTERVAL}s..."
        sleep $RETRY_INTERVAL
    else
        echo "ERROR: PostgreSQL failed to become ready after $MAX_RETRIES attempts ($(($MAX_RETRIES * $RETRY_INTERVAL)) seconds)"
        exit 1
    fi
done

# ============================================================================
# EXECUTE SCHEMA INITIALIZATION SCRIPT
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INIT_SQL_PATH="$SCRIPT_DIR/init_schemas.sql"

if [ ! -f "$INIT_SQL_PATH" ]; then
    echo "ERROR: Schema initialization script not found at $INIT_SQL_PATH"
    exit 1
fi

echo ""
echo "Executing schema initialization from $INIT_SQL_PATH..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Execute the SQL script
if PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$POSTGRES_HOST" \
    -p "${POSTGRES_PORT:-5432}" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -f "$INIT_SQL_PATH"; then
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✓ Schema initialization completed successfully"
    exit 0
else
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "ERROR: Schema initialization failed"
    exit 1
fi
