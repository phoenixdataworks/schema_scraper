#!/bin/bash
# Integration test script for schema-scraper
# Spins up test databases and runs the scraper against each

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/output"
SQLITE_DB="${SCRIPT_DIR}/sqlite/test.db"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    cd "${SCRIPT_DIR}"
    docker compose down -v 2>/dev/null || true
    rm -rf "${OUTPUT_DIR}"
    rm -f "${SQLITE_DB}"
}

# Trap to cleanup on exit
trap cleanup EXIT

# Parse arguments
SKIP_DOCKER=false
ONLY_DB=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-docker)
            SKIP_DOCKER=true
            shift
            ;;
        --only)
            ONLY_DB="$2"
            shift 2
            ;;
        --no-cleanup)
            trap - EXIT
            shift
            ;;
        *)
            echo "Usage: $0 [--skip-docker] [--only <db_type>] [--no-cleanup]"
            exit 1
            ;;
    esac
done

# Start fresh
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

cd "${SCRIPT_DIR}"

# =============================================================================
# Start Docker containers
# =============================================================================
if [ "$SKIP_DOCKER" = false ]; then
    log_info "Starting Docker containers..."
    docker compose up -d

    log_info "Waiting for databases to be ready..."

    # Wait for PostgreSQL
    if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "postgresql" ]; then
        log_info "Waiting for PostgreSQL..."
        for i in {1..30}; do
            if docker compose exec -T postgresql pg_isready -U testuser -d testdb >/dev/null 2>&1; then
                log_info "PostgreSQL is ready!"
                break
            fi
            sleep 2
        done
    fi

    # Wait for MySQL
    if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "mysql" ]; then
        log_info "Waiting for MySQL..."
        for i in {1..30}; do
            if docker compose exec -T mysql mysqladmin ping -h localhost -u root -prootpass >/dev/null 2>&1; then
                log_info "MySQL is ready!"
                break
            fi
            sleep 2
        done
    fi

    # Wait for MSSQL
    if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "mssql" ]; then
        log_info "Waiting for MS SQL Server..."
        for i in {1..60}; do
            if docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "TestPassword123!" -C -Q "SELECT 1" >/dev/null 2>&1; then
                log_info "MS SQL Server is ready!"
                # Run init script
                log_info "Initializing MS SQL Server database..."
                docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "TestPassword123!" -C -i /init.sql
                break
            fi
            sleep 3
        done
    fi

    # Wait for Oracle
    if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "oracle" ]; then
        log_info "Waiting for Oracle (this may take a while)..."
        for i in {1..120}; do
            if docker compose exec -T oracle healthcheck.sh >/dev/null 2>&1; then
                log_info "Oracle is ready!"
                sleep 5  # Extra time for init script to complete
                break
            fi
            sleep 5
        done
    fi
fi

# =============================================================================
# Create SQLite database
# =============================================================================
if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "sqlite" ]; then
    log_info "Creating SQLite database..."
    rm -f "${SQLITE_DB}"
    sqlite3 "${SQLITE_DB}" < "${SCRIPT_DIR}/sqlite/init.sql"
fi

# =============================================================================
# Run schema-scraper tests
# =============================================================================
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_DBS=""

run_scraper() {
    local db_type=$1
    local db_name=$2
    shift 2
    local extra_args=("$@")

    log_info "Testing ${db_type}..."

    if schema-scraper scrape -t "${db_type}" "${extra_args[@]}" -o "${OUTPUT_DIR}" -vv; then
        log_info "${db_type}: SUCCESS"
        TESTS_PASSED=$((TESTS_PASSED + 1))

        # Count generated files
        local file_count=$(find "${OUTPUT_DIR}/${db_name}" -name "*.md" 2>/dev/null | wc -l | tr -d ' ')
        log_info "${db_type}: Generated ${file_count} markdown files"
    else
        log_error "${db_type}: FAILED"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        FAILED_DBS="${FAILED_DBS} ${db_type}"
    fi
}

# SQLite
if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "sqlite" ]; then
    run_scraper "sqlite" "test" -d "${SQLITE_DB}"
fi

# PostgreSQL
if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "postgresql" ]; then
    run_scraper "postgresql" "testdb" -h localhost -P 5432 -d testdb -u testuser -p testpass \
        --schemas sales --schemas inventory --schemas public
fi

# MySQL
if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "mysql" ]; then
    run_scraper "mysql" "testdb" -h localhost -P 3306 -d testdb -u testuser -p testpass
fi

# MS SQL Server
if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "mssql" ]; then
    run_scraper "mssql" "TestDB" -h localhost -P 1433 -d TestDB -u sa -p "TestPassword123!" \
        --schemas Sales --schemas Inventory --schemas dbo
fi

# Oracle
if [ -z "$ONLY_DB" ] || [ "$ONLY_DB" = "oracle" ]; then
    run_scraper "oracle" "FREEPDB1" -h localhost -P 1521 -d FREEPDB1 -u testuser -p testpass \
        --service-name FREEPDB1
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
echo "              TEST SUMMARY"
echo "=============================================="
echo -e "Passed: ${GREEN}${TESTS_PASSED}${NC}"
echo -e "Failed: ${RED}${TESTS_FAILED}${NC}"

if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "Failed databases:${RED}${FAILED_DBS}${NC}"
    exit 1
else
    log_info "All tests passed!"
    echo ""
    log_info "Generated documentation in: ${OUTPUT_DIR}"
    echo ""
    # Show directory structure
    if command -v tree &> /dev/null; then
        tree -L 2 "${OUTPUT_DIR}"
    else
        find "${OUTPUT_DIR}" -type d -maxdepth 2
    fi
fi
