#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"

KEEP_UP=false
RUN_4A=true
RUN_4B=true
RUN_4C=true
RUN_4D=true
RUN_4E=true
RUN_4F=true
RUN_4G=true
RUN_4H=true
RUN_4I=true
RUN_4J=true
RUN_4K=true
RUN_4L=true
RUN_4M=true
RUN_4N=false  # false by default — 4N is a 30-min soak; enable with --4n or --4n-only

while [[ $# -gt 0 ]]; do
  case $1 in
    --keep-up)
      KEEP_UP=true
      # keep-up preserves stack; 4N must be explicitly enabled or defaults off
      shift ;;
    --4a-only) RUN_4N=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4b-only) RUN_4A=false; RUN_4N=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4c-only) RUN_4A=false; RUN_4B=false; RUN_4N=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4d-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4N=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4e-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4N=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4f-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4N=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4g-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4N=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4h-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4N=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4i-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4N=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4j-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4N=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4k-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4N=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4l-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4N=false; RUN_4M=false; shift ;;
    --4m-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4N=false; shift ;;
    --4n-only) RUN_4A=false; RUN_4B=false; RUN_4C=false; RUN_4D=false; RUN_4E=false; RUN_4F=false; RUN_4G=false; RUN_4H=false; RUN_4I=false; RUN_4J=false; RUN_4K=false; RUN_4L=false; RUN_4M=false; shift ;;
    --4n)
      # Enable 4N in addition to all other phase 4 tests
      RUN_4N=true; shift ;;
    --soak-fast)
      # 2-minute fast soak for CI — 4N enabled alongside all other tests
      export PHASE4N_SOAK_MS=120000
      export PHASE4N_TICK_MS=100
      export PHASE4N_ADV_INTERVAL=20
      export PHASE4N_CHECKPOINT_MS=10000
      export PHASE4N_RECYCLE_MS=30000
      RUN_4N=true
      shift ;;
    --soak-full)
      # 30-minute full soak — 4N enabled alongside all other tests
      export PHASE4N_SOAK_MS=1800000
      export PHASE4N_TICK_MS=120
      export PHASE4N_ADV_INTERVAL=20
      export PHASE4N_CHECKPOINT_MS=30000
      export PHASE4N_RECYCLE_MS=300000
      RUN_4N=true
      shift ;;
    --keep-up) KEEP_UP=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

cd "$PROJECT_ROOT"
docker-compose -f "$COMPOSE_FILE" up -d

# Wait for service health before running any tests
echo "Waiting for services to be healthy..."
for i in $(seq 1 20); do
  REDIS_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-redis redis-cli ping 2>/dev/null || echo "FAIL")
  PG_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-postgres pg_isready -U testuser -d testgovernance 2>/dev/null || echo "FAIL")
  if [ "$REDIS_OK" = "PONG" ] && [ "$PG_OK" = "OK" ]; then
    echo "Services healthy."
    break
  fi
  if [ $i -eq 20 ]; then
    echo "Timeout waiting for services — Redis: $REDIS_OK, Postgres: $PG_OK"
    docker-compose -f "$COMPOSE_FILE" logs
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
  echo "=== $label ==="
  npx vitest run "$file" || OVERALL_RESULT=1
}

[ "$RUN_4A" = true ] && run_test "Phase 4A" "tests/phase-4a-projection-ownership-integrity.test.js"
[ "$RUN_4B" = true ] && run_test "Phase 4B" "tests/phase-4b-relay-lineage-immutability.test.js"
[ "$RUN_4C" = true ] && run_test "Phase 4C" "tests/phase-4c-cross-domain-pressure-stability.test.js"
[ "$RUN_4D" = true ] && run_test "Phase 4D" "tests/phase-4d-restart-recovery-determinism.test.js"
[ "$RUN_4E" = true ] && run_test "Phase 4E" "tests/phase-4e-replay-reconstruction.test.js"
[ "$RUN_4F" = true ] && run_test "Phase 4F" "tests/phase-4f-causal-ordering.test.js"
[ "$RUN_4G" = true ] && run_test "Phase 4G" "tests/phase-4g-membrane-attack.test.js"
[ "$RUN_4H" = true ] && run_test "Phase 4H" "tests/phase-4h-consumer-pressure.test.js"
[ "$RUN_4I" = true ] && run_test "Phase 4I" "tests/phase-4i-concurrency-corruption.test.js"
[ "$RUN_4J" = true ] && run_test "Phase 4J" "tests/phase-4j-telemetry-isolation-pressure.test.js"
[ "$RUN_4K" = true ] && run_test "Phase 4K" "tests/phase-4k-durable-persistence-integrity.test.js"
[ "$RUN_4L" = true ] && run_test "Phase 4L" "tests/phase-4l-periodic-hash-convergence.test.js"
[ "$RUN_4M" = true ] && run_test "Phase 4M" "tests/phase-4m-unified-worker-recycle.test.js"
[ "$RUN_4N" = true ] && run_test "Phase 4N" "tests/phase-4n-mixed-constitutional-soak.test.js"

if [ "$KEEP_UP" = true ]; then
  echo "Docker stack left running (--keep-up)."
else
  docker-compose -f "$COMPOSE_FILE" down
fi

exit $OVERALL_RESULT
