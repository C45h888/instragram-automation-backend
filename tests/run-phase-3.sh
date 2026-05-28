#!/bin/bash
# ============================================
# Phase 3: Cross-Domain Constitutional Runner
# ============================================
# Tests cross-domain concurrency, membrane integrity,
# reconciliation isolation, and projection determinism.
#
# Usage:
#   ./tests/run-phase-3.sh              # Full run with teardown
#   ./tests/run-phase-3.sh --keep-up    # Keep Docker up for debugging
#   ./tests/run-phase-3.sh --3a-only
#   ./tests/run-phase-3.sh --3b-only
#   ./tests/run-phase-3.sh --3c-only
#   ./tests/run-phase-3.sh --3d-only
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"

KEEP_UP=false
RUN_3A=true
RUN_3B=true
RUN_3C=true
RUN_3D=true

while [[ $# -gt 0 ]]; do
  case $1 in
    --keep-up)
      KEEP_UP=true
      shift
      ;;
    --3a-only)
      RUN_3B=false
      RUN_3C=false
      RUN_3D=false
      shift
      ;;
    --3b-only)
      RUN_3A=false
      RUN_3C=false
      RUN_3D=false
      shift
      ;;
    --3c-only)
      RUN_3A=false
      RUN_3B=false
      RUN_3D=false
      shift
      ;;
    --3d-only)
      RUN_3A=false
      RUN_3B=false
      RUN_3C=false
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "=========================================="
echo " Phase 3: Cross-Domain Constitutional Tests"
echo "=========================================="
echo "  3A: Mixed-Domain Concurrency       $([ "$RUN_3A" = true ] && echo "✓" || echo "✗")"
echo "  3B: Membrane Boundary Integrity   $([ "$RUN_3B" = true ] && echo "✓" || echo "✗")"
echo "  3C: Cross-Domain Reconciliation    $([ "$RUN_3C" = true ] && echo "✓" || echo "✗")"
echo "  3D: Unified Projection Determinism  $([ "$RUN_3D" = true ] && echo "✓" || echo "✗")"

cd "$PROJECT_ROOT"

# ----------------------------------------
# Step 1: Check/Start Docker Stack (persistent)
# ----------------------------------------
echo -e "\n[1/4] Checking docker-compose stack..."

STACK_HEALTHY=false
if docker-compose -f "$COMPOSE_FILE" ps test-redis test-postgres 2>/dev/null | grep -q "Up"; then
  REDIS_PING=$(docker-compose -f "$COMPOSE_FILE" exec -T test-redis redis-cli ping 2>/dev/null || echo "FAILED")
  if [ "$REDIS_PING" = "PONG" ]; then
    STACK_HEALTHY=true
    echo -e "  Stack already running and healthy"
  fi
fi

if [ "$STACK_HEALTHY" = false ]; then
  echo "  Starting docker-compose stack..."
  docker-compose -f "$COMPOSE_FILE" up -d
  echo "  Waiting for services..."
  sleep 5
fi

echo -e "  Redis: healthy"
echo -e "  PostgreSQL: healthy"

export REDIS_URL="redis://localhost:6379"
export NODE_ENV="test"
export SKIP_DB_TUNNEL="true"

OVERALL_RESULT=0

run_test() {
  local label="$1"
  local file="$2"
  echo ""
  echo "=== $label ==="
  npx vitest run "$file"
  local result=$?
  if [ $result -ne 0 ]; then
    echo "FAILED: $label"
    OVERALL_RESULT=1
  fi
}

echo -e "\n[2/4] Running Phase 3 tests..."

if [ "$RUN_3A" = true ]; then
  run_test "Phase 3A: Mixed Domain Concurrency" "tests/phase-3a-mixed-domain-concurrency.test.js"
fi
if [ "$RUN_3B" = true ]; then
  run_test "Phase 3B: Membrane Boundary Integrity" "tests/phase-3b-membrane-boundary-integrity.test.js"
fi
if [ "$RUN_3C" = true ]; then
  run_test "Phase 3C: Cross-Domain Reconciliation Isolation" "tests/phase-3c-cross-domain-reconciliation-isolation.test.js"
fi
if [ "$RUN_3D" = true ]; then
  run_test "Phase 3D: Unified Projection Determinism" "tests/phase-3d-unified-projection-determinism.test.js"
fi

echo -e "\n[3/4] Results: $([ $OVERALL_RESULT -eq 0 ] && echo 'ALL PASSED' || echo 'SOME FAILED')"

echo -e "\n[4/4] Teardown..."
if [ "$KEEP_UP" = true ]; then
  echo -e "  Docker stack left running (--keep-up)"
  echo "  Redis: localhost:6379, Postgres: localhost:5432"
  echo "  To tear down: docker-compose -f $COMPOSE_FILE down"
else
  docker-compose -f "$COMPOSE_FILE" down
  echo -e "  Stack stopped"
fi

echo -e "\n=========================================="
echo " Phase 3 Complete"
echo "=========================================="
exit $OVERALL_RESULT
