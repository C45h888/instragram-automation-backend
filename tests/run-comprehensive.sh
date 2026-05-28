#!/bin/bash
# ============================================
# Comprehensive Constitutional Runtime Runner
# ============================================
# Runs the full test stack:
#   - Phase 1 (1A..1D + 1C chaos/replay)
#   - Phase 2 (2A..2D long-run suites)
#   - Phase 3 (3A..3D cross-domain suites)
#
# Usage:
#   ./tests/run-comprehensive.sh
#   ./tests/run-comprehensive.sh --keep-up
#   ./tests/run-comprehensive.sh --fast
#   ./tests/run-comprehensive.sh --long
#
# For LONG-DURATION OBSERVATION (no restarts, no teardown):
#   START_STACK_ONLY=true ./tests/run-comprehensive.sh --keep-up
#   # Then manually run tests against the persistent stack
#   docker-compose -f docker-compose.test.yml down  # cleanup when done
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

KEEP_UP=false
PROFILE="default"
START_STACK_ONLY=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --keep-up)
      KEEP_UP=true
      shift
      ;;
    --fast)
      PROFILE="fast"
      shift
      ;;
    --long)
      PROFILE="long"
      shift
      ;;
    --start-stack-only)
      START_STACK_ONLY=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [ "$PROFILE" = "fast" ]; then
  export PHASE2_SOAK_MS=20000
  export PHASE2_CHECKPOINT_MS=5000
  export PHASE2_RECYCLE_MS=7000
elif [ "$PROFILE" = "long" ]; then
  export PHASE2_SOAK_MS=1800000
  export PHASE2_CHECKPOINT_MS=60000
  export PHASE2_RECYCLE_MS=300000
fi

echo "Running comprehensive suite with profile: $PROFILE"

# ----------------------------------------
# PERSISTENT STACK MODE
# For long-duration monitoring without restarts
# ----------------------------------------
if [ "$START_STACK_ONLY" = true ]; then
  echo ""
  echo "=========================================="
  echo " PERSISTENT STACK MODE"
  echo "=========================================="
  echo "Starting Docker stack once, no teardown..."
  echo ""
  
  cd "$PROJECT_ROOT"
  
  # Start stack (will reuse existing if already running)
  docker-compose -f docker-compose.test.yml up -d
  
  # Wait for healthy
  echo "Waiting for services to be healthy..."
  sleep 5
  
  # Verify
  REDIS_HEALTH=$(docker-compose -f docker-compose.test.yml exec -T test-redis redis-cli ping 2>/dev/null || echo "FAILED")
  POSTGRES_HEALTH=$(docker-compose -f docker-compose.test.yml exec -T test-postgres pg_isready -U testuser -d testgovernance 2>/dev/null || echo "FAILED")
  
  echo "Redis:    $REDIS_HEALTH"
  echo "Postgres: $POSTGRES_HEALTH"
  
  echo ""
  echo "=========================================="
  echo " Stack is running. To run tests:"
  echo "  export REDIS_URL=redis://localhost:6379"
  echo "  npx vitest run tests/phase-2c-long-run-endurance.test.js"
  echo ""
  echo "To stop: docker-compose -f docker-compose.test.yml down"
  echo "=========================================="
  exit 0
fi

KEEP_FLAG=""
if [ "$KEEP_UP" = true ]; then
  KEEP_FLAG="--keep-up"
fi

run_phase1() {
  local phase="$1"
  echo ""
  echo "=== Phase 1: $phase ==="
  "$SCRIPT_DIR/run-phase-1a.sh" --phase "$phase" $KEEP_FLAG
}

run_phase2() {
  local flag="$1"
  echo ""
  echo "=== Phase 2: $flag ==="
  "$SCRIPT_DIR/run-phase-2.sh" "$flag" $KEEP_FLAG
}

# For persistent observation mode, start stack ONCE before all phases
if [ "$KEEP_UP" = true ]; then
  echo ""
  echo "=== PERSISTENT MODE: Starting stack once for all phases ==="
  cd "$PROJECT_ROOT"
  docker-compose -f docker-compose.test.yml up -d
  echo "Waiting for services..."
  sleep 5
fi

run_phase1 1a
run_phase1 1a-smoke
run_phase1 1b
run_phase1 1c
run_phase1 1c-chaos
run_phase1 1c-replay
run_phase1 1d

run_phase2 --2a-only
run_phase2 --2b-only
run_phase2 --2c-only
run_phase2 --2d-only

echo ""
echo "=== Phase 3: Cross-Domain Constitutional Interaction ==="
"$SCRIPT_DIR/run-phase-3.sh" $KEEP_FLAG

echo ""
echo "Comprehensive constitutional runtime suite complete."
