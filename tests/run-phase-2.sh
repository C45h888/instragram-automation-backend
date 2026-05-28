#!/bin/bash
# ============================================
# Phase 2: Long-Duration Runtime Test Runner
# ============================================
# This script orchestrates validation of:
#   - Long-duration lineage accumulation
#   - Reconciliation drift resistance
#   - Continuous ticker-driven event injection
#
# Sequence:
#   1. Ensure docker-compose.test.yml is running
#   2. Wait for services to be healthy
#   3. Run Phase 2 vitest tests (2A + 2B)
#   4. Report results
#   5. Teardown (optional via --keep-up flag)
#
# Usage:
#   ./tests/run-phase-2.sh              # Full run with teardown
#   ./tests/run-phase-2.sh --keep-up    # Keep docker up for debugging
#   ./tests/run-phase-2.sh --2a-only    # Run only Phase 2A
#   ./tests/run-phase-2.sh --2b-only    # Run only Phase 2B
#   ./tests/run-phase-2.sh --2c-only    # Run only Phase 2C
#   ./tests/run-phase-2.sh --2d-only    # Run only Phase 2D
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
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Flags
KEEP_UP=false
RUN_2A=true
RUN_2B=true
RUN_2C=true
RUN_2D=true

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --keep-up)
      KEEP_UP=true
      shift
      ;;
    --2a-only)
      RUN_2B=false
      RUN_2C=false
      RUN_2D=false
      shift
      ;;
    --2b-only)
      RUN_2A=false
      RUN_2C=false
      RUN_2D=false
      shift
      ;;
    --2c-only)
      RUN_2A=false
      RUN_2B=false
      RUN_2D=false
      shift
      ;;
    --2d-only)
      RUN_2A=false
      RUN_2B=false
      RUN_2C=false
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN} Phase 2: Long-Duration Runtime Tests${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
echo "  2A: Continuous Lineage Accumulation  $([ "$RUN_2A" = true ] && echo "${GREEN}✓${NC}" || echo "${RED}✗${NC}")"
echo "  2B: Reconciliation Drift Resistance  $([ "$RUN_2B" = true ] && echo "${GREEN}✓${NC}" || echo "${RED}✗${NC}")"
echo "  2C: Long-Run Endurance               $([ "$RUN_2C" = true ] && echo "${GREEN}✓${NC}" || echo "${RED}✗${NC}")"
echo "  2D: Redis Durability Window          $([ "$RUN_2D" = true ] && echo "${GREEN}✓${NC}" || echo "${RED}✗${NC}")"

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

echo "  Waiting for service health..."

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
# Step 3: Run Phase 2 Tests
# ----------------------------------------
echo -e "\n${YELLOW}[3/5]${NC} Running Phase 2 tests against Docker stack..."

# Set environment for Docker-exposed Redis
export REDIS_URL="redis://localhost:6379"
export NODE_ENV="test"
export SKIP_DB_TUNNEL="true"

# Optional: set longer timeout for long-duration tests
export VITEST_TIMEOUT=60000

OVERALL_RESULT=0

if [ "$RUN_2A" = true ]; then
  echo -e "\n  ${BLUE}--- Phase 2A: Continuous Lineage Accumulation ---${NC}"
  cd "$PROJECT_ROOT"
  npx vitest run tests/phase-2a-lineage-accumulation.test.js 2>&1
  RESULT_2A=$?
  if [ $RESULT_2A -ne 0 ]; then
    echo -e "  ${RED}✗ Phase 2A failed${NC}"
    OVERALL_RESULT=1
  else
    echo -e "  ${GREEN}✓ Phase 2A passed${NC}"
  fi
fi

if [ "$RUN_2B" = true ]; then
  echo -e "\n  ${BLUE}--- Phase 2B: Reconciliation Drift Resistance ---${NC}"
  cd "$PROJECT_ROOT"
  npx vitest run tests/phase-2b-reconciliation-drift.test.js 2>&1
  RESULT_2B=$?
  if [ $RESULT_2B -ne 0 ]; then
    echo -e "  ${RED}✗ Phase 2B failed${NC}"
    OVERALL_RESULT=1
  else
    echo -e "  ${GREEN}✓ Phase 2B passed${NC}"
  fi
fi

if [ "$RUN_2C" = true ]; then
  echo -e "\n  ${BLUE}--- Phase 2C: Long-Run Endurance ---${NC}"
  cd "$PROJECT_ROOT"
  npx vitest run tests/phase-2c-long-run-endurance.test.js 2>&1
  RESULT_2C=$?
  if [ $RESULT_2C -ne 0 ]; then
    echo -e "  ${RED}✗ Phase 2C failed${NC}"
    OVERALL_RESULT=1
  else
    echo -e "  ${GREEN}✓ Phase 2C passed${NC}"
  fi
fi

if [ "$RUN_2D" = true ]; then
  echo -e "\n  ${BLUE}--- Phase 2D: Redis Durability Window ---${NC}"
  cd "$PROJECT_ROOT"
  npx vitest run tests/phase-2d-redis-durability.test.js 2>&1
  RESULT_2D=$?
  if [ $RESULT_2D -ne 0 ]; then
    echo -e "  ${RED}✗ Phase 2D failed${NC}"
    OVERALL_RESULT=1
  else
    echo -e "  ${GREEN}✓ Phase 2D passed${NC}"
  fi
fi

# ----------------------------------------
# Step 4: Report Results
# ----------------------------------------
echo -e "\n${YELLOW}[4/5]${NC} Test Results..."

if [ $OVERALL_RESULT -eq 0 ]; then
  echo -e "${GREEN}  ✓ All Phase 2 tests passed${NC}"
else
  echo -e "${RED}  ✗ Some Phase 2 tests failed${NC}"
fi

# ----------------------------------------
# Step 5: Teardown (if not --keep-up)
# ----------------------------------------
echo -e "\n${YELLOW}[5/5]${NC} Teardown..."

if [ "$KEEP_UP" = true ]; then
  echo -e "  ${YELLOW}Keeping docker stack up for debugging${NC}"
  echo -e "\n${CYAN}========================================${NC}"
  echo -e "Stack running at:"
  echo -e "  Redis:    localhost:6379"
  echo -e "  Postgres: localhost:5432"
  echo -e ""
  echo -e "To tear down: docker-compose -f $COMPOSE_FILE down"
  echo -e "========================================${NC}"
else
  echo "  Stopping docker-compose stack..."
  docker-compose -f "$COMPOSE_FILE" down
  echo -e "${GREEN}  Stack stopped${NC}"
fi

echo -e "\n${CYAN}========================================${NC}"
echo -e "Phase 2 Test Run Complete"
echo -e "========================================${NC}"

exit $OVERALL_RESULT
