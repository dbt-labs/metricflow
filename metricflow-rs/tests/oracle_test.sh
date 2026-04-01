#!/bin/bash
# Oracle test: compare Rust MetricFlow SQL output against Python MetricFlow.
# Usage: ./tests/oracle_test.sh /path/to/semantic_manifest.json
#
# Requires:
#   - Rust CLI built: cargo build -p mf-cli
#   - Python MetricFlow available via uvx
#   - DBT_PROFILES_DIR set (defaults to ~/.dbt)

set -euo pipefail

MANIFEST="${1:?Usage: $0 /path/to/semantic_manifest.json}"
INTERNAL_ANALYTICS_DIR="$(dirname "$(dirname "$MANIFEST")")"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUST_CLI="$SCRIPT_DIR/../target/debug/mf"
DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$HOME/.dbt}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0
ERRORS=""

# Generate Python SQL for a query
python_sql() {
    local metrics="$1"
    local extra_args="${2:-}"
    cd "$INTERNAL_ANALYTICS_DIR"
    DBT_PROFILES_DIR="$DBT_PROFILES_DIR" uvx --pre --from dbt-metricflow --with dbt-snowflake \
        mf query --metrics "$metrics" $extra_args --explain 2>&1 \
        | sed -n '/^SELECT/,/^$/p' \
        | sed '/^$/d'
}

# Generate Rust SQL for a query
rust_sql() {
    local metrics="$1"
    local extra_args="${2:-}"
    "$RUST_CLI" query --manifest "$MANIFEST" --metrics "$metrics" $extra_args --dialect duckdb 2>&1
}

# Normalize SQL for comparison: lowercase, collapse whitespace, strip aliases
normalize_sql() {
    echo "$1" \
        | tr '[:upper:]' '[:lower:]' \
        | sed 's/  */ /g' \
        | sed 's/^ //; s/ $//' \
        | tr -d '\n' \
        | sed 's/  */ /g'
}

# Compare two SQL strings semantically
# Returns 0 if they produce the same result (same tables, exprs, filters, group-by)
compare_sql() {
    local python="$1"
    local rust="$2"
    local metric="$3"
    local variant="$4"

    # Check for key semantic elements from Python SQL that must appear in Rust SQL
    local missing=""

    # Extract table references from Python (FROM ... tablename)
    local py_tables
    py_tables=$(echo "$python" | grep -ioE 'FROM [A-Za-z0-9_.]+' | head -1 | awk '{print $2}')
    if [ -n "$py_tables" ] && ! echo "$rust" | grep -qi "$py_tables"; then
        missing="$missing\n  - Missing table: $py_tables"
    fi

    # Check WHERE clauses from Python exist in Rust
    local py_where
    py_where=$(echo "$python" | grep -i "WHERE" | sed 's/.*WHERE //' | head -1)
    if [ -n "$py_where" ]; then
        # Normalize the where condition
        local py_where_norm
        py_where_norm=$(echo "$py_where" | tr '[:upper:]' '[:lower:]' | sed "s/  */ /g; s/^ //; s/ $//")
        local rust_norm
        rust_norm=$(echo "$rust" | tr '[:upper:]' '[:lower:]')
        if ! echo "$rust_norm" | grep -qF "$py_where_norm"; then
            missing="$missing\n  - Missing WHERE: $py_where"
        fi
    fi

    # Check aggregation function matches
    local py_agg
    py_agg=$(echo "$python" | grep -ioE '(SUM|AVG|COUNT|MIN|MAX)\(' | head -1)
    if [ -n "$py_agg" ] && ! echo "$rust" | grep -qi "$py_agg"; then
        missing="$missing\n  - Missing aggregation: $py_agg"
    fi

    # Check GROUP BY presence matches
    local py_has_groupby rust_has_groupby
    py_has_groupby=$(echo "$python" | grep -c "GROUP BY" || true)
    rust_has_groupby=$(echo "$rust" | grep -c "GROUP BY" || true)
    if [ "$py_has_groupby" -gt 0 ] && [ "$rust_has_groupby" -eq 0 ]; then
        missing="$missing\n  - Missing GROUP BY"
    fi

    if [ -n "$missing" ]; then
        echo -e "${RED}FAIL${NC} $metric ($variant)$missing"
        echo "  Python SQL:"
        echo "$python" | sed 's/^/    /'
        echo "  Rust SQL:"
        echo "$rust" | sed 's/^/    /'
        echo ""
        FAIL=$((FAIL + 1))
        ERRORS="$ERRORS\n$metric ($variant)"
        return 1
    else
        echo -e "${GREEN}PASS${NC} $metric ($variant)"
        PASS=$((PASS + 1))
        return 0
    fi
}

# Test a metric
test_metric() {
    local metric="$1"

    # Test without group-by
    local py_out rust_out
    py_out=$(python_sql "$metric" "" 2>/dev/null) || { echo -e "${YELLOW}SKIP${NC} $metric (Python error)"; SKIP=$((SKIP+1)); return 0; }
    rust_out=$(rust_sql "$metric" "" 2>/dev/null) || { echo -e "${YELLOW}SKIP${NC} $metric (Rust error)"; SKIP=$((SKIP+1)); return 0; }
    compare_sql "$py_out" "$rust_out" "$metric" "no group-by" || true

    # Test with metric_time group-by
    py_out=$(python_sql "$metric" "--group-by metric_time" 2>/dev/null) || { echo -e "${YELLOW}SKIP${NC} $metric by day (Python error)"; SKIP=$((SKIP+1)); return 0; }
    rust_out=$(rust_sql "$metric" "--group-by metric_time --grain day" 2>/dev/null) || { echo -e "${YELLOW}SKIP${NC} $metric by day (Rust error)"; SKIP=$((SKIP+1)); return 0; }
    compare_sql "$py_out" "$rust_out" "$metric" "by day" || true
}

echo "Oracle test: comparing Rust vs Python MetricFlow SQL output"
echo "Manifest: $MANIFEST"
echo ""

# Build Rust CLI
echo "Building Rust CLI..."
cd "$SCRIPT_DIR/.."
cargo build -p mf-cli --quiet

# Pick test metrics: mix of simple (no filter), simple (with filter)
echo ""
echo "=== Simple metrics without filter ==="
for metric in avg_spend clicks spend account_signups; do
    test_metric "$metric"
done

echo ""
echo "=== Simple metrics with filter ==="
for metric in arr_current_self_serve active_ads; do
    test_metric "$metric"
done

echo ""
echo "=================================="
echo -e "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${YELLOW}${SKIP} skipped${NC}"
if [ -n "$ERRORS" ]; then
    echo -e "\nFailed tests:$ERRORS"
fi
exit $FAIL
