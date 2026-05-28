#!/bin/bash
# ============================================
# Phase 1A: Constitutional Runtime Test Runner
# ============================================
# This script orchestrates the validation of
# the constitutional runtime observability layer.
#
# Sequence:
#   1. Ensure docker-compose.test.yml is running
#   2. Wait for services to be healthy
#   3. Run Phase 1A vitest tests
#   4. Report results
#   5. Teardown (optional via --keep-up flag)
#
# Usage:
#   ./tests/run-phase-1a.sh              # Full run with teardown
#   ./tests/run-phase-1a.sh --keep-up    # Keep docker up for debugging
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Flags
KEEP_UP=false
PHASE="1a"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --keep-up)
      KEEP_UP=true
      shift
      ;;
    --phase)
      PHASE="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE} Phase ${PHASE}: Constitutional Runtime Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"

# ----------------------------------------
# Step 1: Ensure Docker Stack is Running
# ----------------------------------------
echo -e "\n${YELLOW}[1/5]${NC} Checking docker-compose stack..."

cd "$PROJECT_ROOT"

# Check if stack is already running and healthy
STACK_HEALTHY=false
if docker-compose -f "$COMPOSE_FILE" ps test-redis test-postgres 2>/dev/null | grep -q "Up"; then
  REDIS_PING=$(docker-compose -f "$COMPOSE_FILE" exec -T test-redis redis-cli ping 2>/dev/null || echo "FAILED")
  if [ "$REDIS_PING" = "PONG" ]; then
    STACK_HEALTHY=true
    echo -e "  ${GREEN}Stack already running and healthy${NC}"
  fi
fi

if [ "$STACK_HEALTHY" = false ]; then
  echo "  Starting docker-compose stack..."
  docker-compose -f "$COMPOSE_FILE" up -d
  echo "  Waiting for services to be healthy..."
  sleep 5
fi

# ----------------------------------------
# Step 2: Wait for Service Health
# ----------------------------------------
echo -e "\n${YELLOW}[2/5]${NC} Waiting for service health..."

MAX_WAIT=60
WAIT_COUNT=0

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
  REDIS_HEALTH=$(docker-compose -f "$COMPOSE_FILE" exec -T test-redis redis-cli ping 2>/dev/null || echo "FAILED")
  POSTGRES_HEALTH=$(docker-compose -f "$COMPOSE_FILE" exec -T test-postgres pg_isready -U testuser -d testgovernance 2>/dev/null || echo "FAILED")
  
  if [ "$REDIS_HEALTH" = "PONG" ] && echo "$POSTGRES_HEALTH" | grep -q "accepting"; then
    echo -e "  ${GREEN}Redis: healthy${NC}"
    echo -e "  ${GREEN}PostgreSQL: healthy${NC}"
    break
  fi
  
  echo "  Waiting for services... ($WAIT_COUNT/$MAX_WAIT)s"
  sleep 2
  WAIT_COUNT=$((WAIT_COUNT + 2))
done

if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
  echo -e "${RED}  ERROR: Services failed to become healthy within ${MAX_WAIT}s${NC}"
  docker-compose -f "$COMPOSE_FILE" logs test-redis test-postgres
  exit 1
fi

# ----------------------------------------
# Step 3: Run Phase Tests
# ----------------------------------------
echo -e "\n${YELLOW}[3/5]${NC} Running Phase ${PHASE} tests..."

# Set Redis URL to docker-exposed port
export REDIS_URL="redis://localhost:6379"
export NODE_ENV="test"

# Determine test file based on phase
case $PHASE in
  "1a")
    TEST_FILE="tests/phase-1a-observability.test.js"
    ;;
  "1a-smoke")
    TEST_FILE="tests/phase-1a-observability-contracts.test.js"
    ;;
  "1b")
    TEST_FILE="tests/phase-1b-deterministic-simulation.test.js"
    ;;
  "1c")
    TEST_FILE="tests/phase-1c-constitutional-verification.test.js"
    ;;
  "1c-chaos")
    TEST_FILE="tests/phase-1c-chaos-stress.test.js"
    ;;
  "1c-replay")
    TEST_FILE="tests/phase-1c-replay-repair.test.js"
    ;;
  "1d")
    TEST_FILE="tests/phase-1d-projection-integrity.test.js"
    ;;
  *)
    echo -e "${RED}Unknown phase: $PHASE${NC}"
    exit 1
    ;;
esac

if [ ! -f "$PROJECT_ROOT/$TEST_FILE" ]; then
  echo -e "${RED}  ERROR: Test file not found: $TEST_FILE${NC}"
  echo "  Available phases: 1a, 1b, 1c, 1d"
  exit 1
fi

echo "  Test file: $TEST_FILE"
echo ""

# Run vitest
cd "$PROJECT_ROOT"
npm run test:vitest -- "$TEST_FILE"
TEST_RESULT=$?

# ----------------------------------------
# Step 4: Report Results
# ----------------------------------------
echo -e "\n${YELLOW}[4/5]${NC} Test Results..."

if [ $TEST_RESULT -eq 0 ]; then
  echo -e "${GREEN}  ✓ All tests passed${NC}"
else
  echo -e "${RED}  ✗ Tests failed (exit code: $TEST_RESULT)${NC}"
fi

# ----------------------------------------
# Step 5: Teardown (if not --keep-up)
# ----------------------------------------
echo -e "\n${YELLOW}[5/5]${NC} Teardown..."

if [ "$KEEP_UP" = true ]; then
  echo -e "  ${YELLOW}Keeping docker stack up for debugging${NC}"
  echo -e "\n${BLUE}========================================${NC}"
  echo -e "Stack running at:"
  echo -e "  Redis:    localhost:6379"
  echo -e "  Postgres: localhost:5432"
  echo -e "\nTo tear down: docker-compose -f $COMPOSE_FILE down${NC}"
  echo -e "========================================${NC}"
else
  echo "  Stopping docker-compose stack..."
  docker-compose -f "$COMPOSE_FILE" down
  echo -e "${GREEN}  Stack stopped${NC}"
fi

echo -e "\n${BLUE}========================================${NC}"
echo -e "Phase ${PHASE} Test Run Complete"
echo -e "========================================${NC}"

exit $TEST_RESULT
