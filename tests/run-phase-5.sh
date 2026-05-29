#!/bin/bash
set -e

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 5: Reconciliation Integration & Longitudinal Constitutional Testing
# ═══════════════════════════════════════════════════════════════════════════════
#
# Test layers:
#   5A — Reconciliation Engine Gap Tests (GAP-1 through GAP-10)
#   5B — Concurrent Ecosystem Testing (5-domain, 5-min concurrent operation)
#   5C — Catastrophic Fault Recovery (worker massacre, Redis restart, corruption)
#   5D — 1-Hour Longitudinal Constitutional Soak (with reconciliation cycles)
#
# Usage:
#   ./run-phase-5.sh                 # Run 5A + 5B + 5C (skip 5D by default)
#   ./run-phase-5.sh --all           # Run all four (5A + 5B + 5C + 5D)
#   ./run-phase-5.sh --5a-only       # Run only gap tests
#   ./run-phase-5.sh --5b-only       # Run only concurrent ecosystem
#   ./run-phase-5.sh --5c-only       # Run only catastrophic fault recovery
#   ./run-phase-5.sh --5d-only       # Run only 1-hour soak
#   ./run-phase-5.sh --keep-up       # Leave Docker stack running after tests
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"

KEEP_UP=false
RUN_5A=true
RUN_5B=true
RUN_5C=true
RUN_5D=false  # 1-hour soak is opt-in — enable with --all or --5d-only

while [[ $# -gt 0 ]]; do
  case $1 in
    --keep-up) KEEP_UP=true; shift ;;
    --all) RUN_5A=true; RUN_5B=true; RUN_5C=true; RUN_5D=true; shift ;;
    --5a-only) RUN_5B=false; RUN_5C=false; RUN_5D=false; shift ;;
    --5b-only) RUN_5A=false; RUN_5C=false; RUN_5D=false; shift ;;
    --5c-only) RUN_5A=false; RUN_5B=false; RUN_5D=false; shift ;;
    --5d-only) RUN_5A=false; RUN_5B=false; RUN_5C=false; RUN_5D=true; shift ;;
    --5d) RUN_5D=true; shift ;;
    *) echo "Unknown option: $1"; echo "Usage: run-phase-5.sh [--all] [--5a-only|--5b-only|--5c-only|--5d-only] [--5d] [--keep-up]"; exit 1 ;;
  esac
done

cd "$PROJECT_ROOT"

# ── Start Docker services ─────────────────────────────────────────────────────
docker-compose -f "$COMPOSE_FILE" up -d

echo "Waiting for services to be healthy..."
for i in $(seq 1 30); do
  REDIS_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-redis redis-cli ping 2>/dev/null || echo "FAIL")
  PG_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-postgres pg_isready -U testuser -d testgovernance 2>/dev/null || echo "FAIL")
  if [ "$REDIS_OK" = "PONG" ] && echo "$PG_OK" | grep -q "accepting"; then
    echo "Services healthy (Redis: PONG, Postgres: accepting connections)."
    break
  fi
  if [ $i -eq 30 ]; then
    echo "Timeout waiting for services — Redis: $REDIS_OK, Postgres: $PG_OK"
    docker-compose -f "$COMPOSE_FILE" logs --tail=50
    exit 1
  fi
  sleep 1
done

export REDIS_URL="redis://localhost:6379"
export NODE_ENV="test"
export SKIP_DB_TUNNEL="true"

OVERALL_RESULT=0
run_test() {
  local label="$1"
  local file="$2"
  echo ""
  echo "╔══════════════════════════════════════════════════════════╗"
  echo "║  $label"
  echo "╚══════════════════════════════════════════════════════════╝"
  npx vitest run "$file" || OVERALL_RESULT=1
}

[ "$RUN_5A" = true ] && run_test "Phase 5A — Reconciliation Gap Tests" "tests/phase-5a-reconciliation-gap-tests.test.js"
[ "$RUN_5B" = true ] && run_test "Phase 5B — Concurrent Ecosystem" "tests/phase-5b-concurrent-ecosystem.test.js"
[ "$RUN_5C" = true ] && run_test "Phase 5C — Catastrophic Fault Recovery" "tests/phase-5c-catastrophic-fault-recovery.test.js"
[ "$RUN_5D" = true ] && run_test "Phase 5D — 1-Hour Longitudinal Soak" "tests/phase-5d-longitudinal-constitutional-soak.test.js"

if [ "$KEEP_UP" = true ]; then
  echo ""
  echo "Docker stack left running (--keep-up)."
else
  docker-compose -f "$COMPOSE_FILE" down
fi

exit $OVERALL_RESULT
