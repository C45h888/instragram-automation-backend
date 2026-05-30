#!/bin/bash
# ============================================
# Unified Constitutional Test Suite Runner
# Single execution path — all tests run inside
# the test-runner container via docker exec.
# ============================================
#
# Architecture:
#   Docker (docker-compose.test.yml) provides the stable test runtime
#   substrate (Redis + Postgres + test-runner container).
#   Vitest executes inside the test-runner container via docker exec.
#   Container DNS (test-redis:6379) provides consistent addressing.
#   docker-entrypoint.sh handles pre-flight checks per invocation.
#
# Phase ordering:
#   Phases 1–3 run first (fast, produce JSON output for verification).
#   Phases 4–5 run sequentially after (including soak tests).
#   Soak tests (4N, 5D) have no fast path — they run at full duration.
#
# Usage:
#   ./tests/run-all-tests.sh --all          Run all phases 1–5
#   ./tests/run-all-tests.sh --phase-1      Phase 1 only
#   ./tests/run-all-tests.sh --phase-2      Phase 2 only
#   ./tests/run-all-tests.sh --phase-3      Phase 3 only
#   ./tests/run-all-tests.sh --phase-4      Phase 4 only (includes 4N soak)
#   ./tests/run-all-tests.sh --phase-5      Phase 5 only (includes 5D soak)
#   ./tests/run-all-tests.sh --phases-1-3   Fast phases only (1–3)
#   ./tests/run-all-tests.sh --skip-soaks   All phases but skip 4N and 5D
#   ./tests/run-all-tests.sh --keep-up      Leave stack running after tests
#
# Examples:
#   ./tests/run-all-tests.sh --all --keep-up
#   ./tests/run-all-tests.sh --phases-1-3
#   ./tests/run-all-tests.sh --phase-4 --skip-soaks
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"

# ── Defaults ─────────────────────────────────────────────────────────────────
KEEP_UP=false
SKIP_SOAKS=false

RUN_P1=false
RUN_P2=false
RUN_P3=false
RUN_P4=false
RUN_P5=false

# ── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --all)
      RUN_P1=true; RUN_P2=true; RUN_P3=true; RUN_P4=true; RUN_P5=true; shift ;;
    --phases-1-3)
      RUN_P1=true; RUN_P2=true; RUN_P3=true; shift ;;
    --phase-1) RUN_P1=true; shift ;;
    --phase-2) RUN_P2=true; shift ;;
    --phase-3) RUN_P3=true; shift ;;
    --phase-4) RUN_P4=true; shift ;;
    --phase-5) RUN_P5=true; shift ;;
    --skip-soaks) SKIP_SOAKS=true; shift ;;
    --keep-up) KEEP_UP=true; shift ;;
    *)
      echo "Usage: $0 [--all|--phases-1-3|--phase-1|--phase-2|--phase-3|--phase-4|--phase-5] [--skip-soaks] [--keep-up]"
      exit 1 ;;
  esac
done

# ── If nothing selected, print usage ─────────────────────────────────────────
if [ "$RUN_P1" = false ] && [ "$RUN_P2" = false ] && [ "$RUN_P3" = false ] && \
   [ "$RUN_P4" = false ] && [ "$RUN_P5" = false ]; then
  echo "No phases selected. Use --all, --phases-1-3, or --phase-N."
  echo "Usage: $0 [--all|--phases-1-3|--phase-1|--phase-2|--phase-3|--phase-4|--phase-5] [--skip-soaks] [--keep-up]"
  exit 1
fi

# ── Ensure Docker stack is healthy ───────────────────────────────────────────
echo "════════════════════════════════════════════════════════════"
echo "  Unified Constitutional Test Suite"
echo "  Runtime: Docker container-native (test-runner)"
echo "════════════════════════════════════════════════════════════"
echo ""

STACK_UP=$(docker-compose -f "$COMPOSE_FILE" ps --services --filter "status=running" 2>/dev/null | wc -l | tr -d ' ')

if [ "$STACK_UP" -lt 3 ]; then
  echo "[runner] Stack not fully up (${STACK_UP}/3 services). Starting..."
  docker-compose -f "$COMPOSE_FILE" up -d
  echo "[runner] Waiting for services to be healthy..."
  for i in $(seq 1 30); do
    REDIS_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-redis redis-cli ping 2>/dev/null || echo "FAIL")
    PG_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-postgres pg_isready -U testuser -d testgovernance 2>/dev/null || echo "FAIL")
    if [ "$REDIS_OK" = "PONG" ] && echo "$PG_OK" | grep -q "accepting"; then
      echo "[runner] Stack healthy (Redis: PONG, Postgres: accepting connections)."
      break
    fi
    if [ $i -eq 30 ]; then
      echo "[runner] ERROR: Timeout waiting for stack. Dumping logs:"
      docker-compose -f "$COMPOSE_FILE" logs --tail=30
      exit 1
    fi
    sleep 1
  done
else
  echo "[runner] Stack already up (${STACK_UP}/3 services running)."
fi

# ── Helper: run a test inside the test-runner container ──────────────────────
# Args: label test_file [extra_env_vars...]
run_test() {
  local label="$1"
  local file="$2"
  shift 2
  local extra_env=("$@")

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  ${label}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  local env_args=()
  for ev in "${extra_env[@]}"; do
    env_args+=("-e" "$ev")
  done

  docker-compose -f "$COMPOSE_FILE" exec -T "${env_args[@]}" test-runner \
    npx vitest run "$file" \
    --reporter=verbose \
    --config /app/tests/vitest.config.js

  local result=$?
  if [ $result -ne 0 ]; then
    echo "  [FAIL] ${label} (exit code: ${result})"
  else
    echo "  [PASS] ${label}"
  fi
  return $result
}

# ── Run phases ───────────────────────────────────────────────────────────────
OVERALL_RESULT=0
START_TIME=$(date +%s)

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Observability & Deterministic Foundation (fast, ~2 min)
# ═══════════════════════════════════════════════════════════════════════════════
if [ "$RUN_P1" = true ]; then
  echo ""
  echo "┌──────────────────────────────────────────────────────────┐"
  echo "│  PHASE 1 — Observability & Deterministic Foundation      │"
  echo "└──────────────────────────────────────────────────────────┘"

  run_test "1A — Observability Infrastructure"    "tests/phase-1a-observability.test.js"              || OVERALL_RESULT=1
  run_test "1A — Observability Contracts"         "tests/phase-1a-observability-contracts.test.js"    || OVERALL_RESULT=1
  run_test "1B — Deterministic Simulation"        "tests/phase-1b-deterministic-simulation.test.js"   || OVERALL_RESULT=1
  run_test "1C — Constitutional Verification"    "tests/phase-1c-constitutional-verification.test.js" || OVERALL_RESULT=1
  run_test "1C — Chaos Stress"                   "tests/phase-1c-chaos-stress.test.js"               || OVERALL_RESULT=1
  run_test "1C — Replay Repair"                  "tests/phase-1c-replay-repair.test.js"              || OVERALL_RESULT=1
  run_test "1D — Projection Integrity"           "tests/phase-1d-projection-integrity.test.js"       || OVERALL_RESULT=1
fi

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — Long-Duration Runtime (fast, ~4 min)
# ═══════════════════════════════════════════════════════════════════════════════
if [ "$RUN_P2" = true ]; then
  echo ""
  echo "┌──────────────────────────────────────────────────────────┐"
  echo "│  PHASE 2 — Long-Duration Runtime Validation              │"
  echo "└──────────────────────────────────────────────────────────┘"

  run_test "2A — Continuous Lineage Accumulation"  "tests/phase-2a-lineage-accumulation.test.js"     || OVERALL_RESULT=1
  run_test "2B — Reconciliation Drift Resistance" "tests/phase-2b-reconciliation-drift.test.js"     || OVERALL_RESULT=1
  run_test "2C — Long-Run Endurance"              "tests/phase-2c-long-run-endurance.test.js"       || OVERALL_RESULT=1
  run_test "2D — Redis Durability Window"         "tests/phase-2d-redis-durability.test.js"         || OVERALL_RESULT=1
fi

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — Cross-Domain Constitutional (fast, ~3 min)
# ═══════════════════════════════════════════════════════════════════════════════
if [ "$RUN_P3" = true ]; then
  echo ""
  echo "┌──────────────────────────────────────────────────────────┐"
  echo "│  PHASE 3 — Cross-Domain Constitutional Interaction       │"
  echo "└──────────────────────────────────────────────────────────┘"

  run_test "3A — Mixed-Domain Concurrency"              "tests/phase-3a-mixed-domain-concurrency.test.js"              || OVERALL_RESULT=1
  run_test "3B — Membrane Boundary Integrity"          "tests/phase-3b-membrane-boundary-integrity.test.js"           || OVERALL_RESULT=1
  run_test "3C — Cross-Domain Reconciliation Isolation" "tests/phase-3c-cross-domain-reconciliation-isolation.test.js" || OVERALL_RESULT=1
  run_test "3D — Unified Projection Determinism"       "tests/phase-3d-unified-projection-determinism.test.js"        || OVERALL_RESULT=1
fi

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — Projection Isolation & Relay Integrity (~40 min with soak)
# ═══════════════════════════════════════════════════════════════════════════════
if [ "$RUN_P4" = true ]; then
  echo ""
  echo "┌──────────────────────────────────────────────────────────┐"
  echo "│  PHASE 4 — Projection Isolation & Relay Integrity        │"
  echo "└──────────────────────────────────────────────────────────┘"

  run_test "4A — Projection Ownership Integrity"    "tests/phase-4a-projection-ownership-integrity.test.js"    || OVERALL_RESULT=1
  run_test "4B — Relay Lineage Immutability"       "tests/phase-4b-relay-lineage-immutability.test.js"        || OVERALL_RESULT=1
  run_test "4C — Cross-Domain Pressure Stability"  "tests/phase-4c-cross-domain-pressure-stability.test.js"   || OVERALL_RESULT=1
  run_test "4D — Restart Recovery Determinism"     "tests/phase-4d-restart-recovery-determinism.test.js"      || OVERALL_RESULT=1
  run_test "4E — Replay Reconstruction"            "tests/phase-4e-replay-reconstruction.test.js"             || OVERALL_RESULT=1
  run_test "4F — Causal Ordering"                  "tests/phase-4f-causal-ordering.test.js"                   || OVERALL_RESULT=1
  run_test "4G — Membrane Attack Resistance"       "tests/phase-4g-membrane-attack.test.js"                   || OVERALL_RESULT=1
  run_test "4H — Consumer Pressure"                "tests/phase-4h-consumer-pressure.test.js"                 || OVERALL_RESULT=1
  run_test "4I — Concurrency Corruption"           "tests/phase-4i-concurrency-corruption.test.js"            || OVERALL_RESULT=1
  run_test "4J — Telemetry Isolation Pressure"     "tests/phase-4j-telemetry-isolation-pressure.test.js"      || OVERALL_RESULT=1
  run_test "4K — Durable Persistence Integrity"    "tests/phase-4k-durable-persistence-integrity.test.js"     || OVERALL_RESULT=1
  run_test "4L — Periodic Hash Convergence"        "tests/phase-4l-periodic-hash-convergence.test.js"         || OVERALL_RESULT=1
  run_test "4M — Unified Worker Recycle"           "tests/phase-4m-unified-worker-recycle.test.js"            || OVERALL_RESULT=1

  # 4N — 30-minute constitutional soak (no fast path)
  if [ "$SKIP_SOAKS" = true ]; then
    echo ""
    echo "  [SKIP] 4N — Mixed Constitutional Soak (--skip-soaks)"
  else
    run_test "4N — Mixed Constitutional Soak (30 min)" \
      "tests/phase-4n-mixed-constitutional-soak.test.js" \
      "PHASE4N_SOAK_MS=1800000" \
      "PHASE4N_TICK_MS=120" \
      "PHASE4N_ADV_INTERVAL=20" \
      "PHASE4N_CHECKPOINT_MS=30000" \
      "PHASE4N_RECYCLE_MS=300000" \
      || OVERALL_RESULT=1
  fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 5 — Reconciliation Gaps & Longitudinal Soak (~70 min with soak)
# ═══════════════════════════════════════════════════════════════════════════════
if [ "$RUN_P5" = true ]; then
  echo ""
  echo "┌──────────────────────────────────────────────────────────┐"
  echo "│  PHASE 5 — Reconciliation Gaps & Longitudinal Soak       │"
  echo "└──────────────────────────────────────────────────────────┘"

  run_test "5A — Reconciliation Gap Tests"       "tests/phase-5a-reconciliation-gap-tests.test.js"       || OVERALL_RESULT=1
  run_test "5B — Concurrent Ecosystem"           "tests/phase-5b-concurrent-ecosystem.test.js"           || OVERALL_RESULT=1
  run_test "5C — Catastrophic Fault Recovery"    "tests/phase-5c-catastrophic-fault-recovery.test.js"    || OVERALL_RESULT=1

  # 5D — 1-hour longitudinal soak (no fast path)
  if [ "$SKIP_SOAKS" = true ]; then
    echo ""
    echo "  [SKIP] 5D — Longitudinal Constitutional Soak (--skip-soaks)"
  else
    run_test "5D — Longitudinal Constitutional Soak (1 hr)" \
      "tests/phase-5d-longitudinal-constitutional-soak.test.js" \
      "PHASE5D_SOAK_MS=3600000" \
      "PHASE5D_TICK_MS=500" \
      "PHASE5D_ADV_INTERVAL=30" \
      "PHASE5D_RECON_MS=60000" \
      "PHASE5D_CHECKPOINT_MS=300000" \
      "PHASE5D_RECYCLE_MS=600000" \
      || OVERALL_RESULT=1
  fi
fi

# ── Report ────────────────────────────────────────────────────────────────────
ELAPSED=$(($(date +%s) - START_TIME))
ELAPSED_MIN=$((ELAPSED / 60))
ELAPSED_SEC=$((ELAPSED % 60))

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Test Suite Complete"
echo "  Elapsed: ${ELAPSED_MIN}m ${ELAPSED_SEC}s"
if [ $OVERALL_RESULT -eq 0 ]; then
  echo "  Result:  ALL PASSED"
else
  echo "  Result:  SOME FAILED"
fi
echo "════════════════════════════════════════════════════════════"

# ── Output files ──────────────────────────────────────────────────────────────
echo ""
echo "  Output files:"
docker-compose -f "$COMPOSE_FILE" exec -T test-runner sh -c \
  'ls -la /app/tests/output/ 2>/dev/null || echo "  (no output files)"' || true

# ── Container status ──────────────────────────────────────────────────────────
echo ""
echo "  Container status:"
docker-compose -f "$COMPOSE_FILE" ps

# ── Teardown ──────────────────────────────────────────────────────────────────
if [ "$KEEP_UP" = true ]; then
  echo ""
  echo "[runner] Stack left running (--keep-up)."
  echo "  Tear down: docker-compose -f docker-compose.test.yml down"
else
  echo ""
  echo "[runner] Tearing down stack..."
  docker-compose -f "$COMPOSE_FILE" down
fi

exit $OVERALL_RESULT
