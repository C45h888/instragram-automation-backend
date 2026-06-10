#!/bin/bash
# ============================================
# Phase 7 Runtime Validation — Test Runner
# ============================================
# Phase 7 is a runtime validation framework, not a
# collection of unit tests. It validates the kernelized
# architecture as an operating organism. The primary
# unit of coverage is the KERNEL.
#
# Architecture:
#   - Boots all 9 kernel batteries (one per kernel).
#   - Cognition-layer cross-cuts run inside the
#     capability + acquisition + publishing batteries.
#   - Integration tests run after all kernel batteries
#     pass.
#   - Tests run inside the test-runner container via
#     docker exec, same pattern as Phases 1-6.
#
# Cadence tiers (PHASE7_CADENCE_TIER env var):
#   short:  25-50 ticks, every commit
#   medium: 100-250 ticks, Phase 7 + CI (DEFAULT)
#   long:   500-1000 ticks, manual / pre-release
#
# Usage:
#   ./tests/phase-7/phase-7-runner.sh                          # run all kernel batteries + integration (medium)
#   ./tests/phase-7/phase-7-runner.sh --tier=short             # short cadence
#   ./tests/phase-7/phase-7-runner.sh --tier=long              # long cadence (manual)
#   ./tests/phase-7/phase-7-runner.sh --kernel=acquisition     # run a single kernel battery
#   ./tests/phase-7/phase-7-runner.sh --integration-only       # skip kernel batteries
#   ./tests/phase-7/phase-7-runner.sh --kernels-only           # skip integration
#   ./tests/phase-7/phase-7-runner.sh --keep-up                # leave stack running
#   ./tests/phase-7/phase-7-runner.sh --help                   # usage
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"
PHASE7_DIR="tests/phase-7"

# ── Defaults ─────────────────────────────────────────────────────────────────
KEEP_UP=false
TIER="medium"
SINGLE_KERNEL=""
INTEGRATION_ONLY=false
KERNELS_ONLY=false
RUN_KERNELS=true
RUN_INTEGRATION=true

# ── Usage ────────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Phase 7 Runtime Validation Runner

Usage:
  $0 [options]

Options:
  --tier=SHORT|MEDIUM|LONG   Cadence tier for multi-tick-survival (default: medium)
  --kernel=NAME              Run a single kernel battery (e.g. acquisition, publishing)
  --integration-only         Skip kernel batteries, run integration only
  --kernels-only             Skip integration, run kernel batteries only
  --keep-up                  Leave Docker stack running after tests
  --help                     Show this help

Tiers:
  short   25-50 ticks   (every commit)
  medium  100-250 ticks (Phase 7 + CI)  [DEFAULT]
  long    500-1000 ticks (manual / pre-release)

Examples:
  $0 --tier=short
  $0 --kernel=acquisition
  $0 --tier=long --keep-up
EOF
}

# ── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --tier=*)  TIER="${1#*=}"; shift ;;
    --kernel=*) SINGLE_KERNEL="${1#*=}"; shift ;;
    --integration-only) INTEGRATION_ONLY=true; RUN_KERNELS=false; shift ;;
    --kernels-only)     KERNELS_ONLY=true;     RUN_INTEGRATION=false; shift ;;
    --keep-up) KEEP_UP=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

# Validate tier
case "$TIER" in
  short|medium|long) ;;
  *) echo "Invalid tier: $TIER (use short|medium|long)"; exit 1 ;;
esac

# ── Ensure Docker stack is healthy ───────────────────────────────────────────
echo "════════════════════════════════════════════════════════════"
echo "  Phase 7 Runtime Validation Framework"
echo "  Cadence tier: ${TIER}"
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
    --config /app/tests/phase-7/vitest.config.js

  local result=$?
  if [ $result -ne 0 ]; then
    echo "  [FAIL] ${label} (exit code: ${result})"
    dump_latest_report
  else
    echo "  [PASS] ${label}"
  fi
  return $result
}

# ── Helper: dump the latest phase-7 report on failure ────────────────────────
dump_latest_report() {
  local report_dir="$PROJECT_ROOT/$PHASE7_DIR/reports"
  if [ -d "$report_dir" ]; then
    local latest
    latest=$(ls -1t "$report_dir" 2>/dev/null | head -1)
    if [ -n "$latest" ] && [ -f "$report_dir/$latest/report.json" ]; then
      echo ""
      echo "  ── Latest runtime report: $report_dir/$latest/report.json ──"
      head -c 2000 "$report_dir/$latest/report.json"
      echo ""
    fi
  fi
}

# ── Run kernel batteries ─────────────────────────────────────────────────────
OVERALL_RESULT=0
START_TIME=$(date +%s)

if [ "$RUN_KERNELS" = true ]; then
  echo ""
  echo "┌──────────────────────────────────────────────────────────┐"
  echo "│  PHASE 7 — KERNEL BATTERIES                               │"
  echo "│  (per-kernel 7-category runtime validation)              │"
  echo "└──────────────────────────────────────────────────────────┘"

  KERNELS=(acquisition publishing capability reconciliation retry persistence telemetry scheduling dedup uat-refresh)

  if [ -n "$SINGLE_KERNEL" ]; then
    # Run only the requested kernel
    found=false
    for k in "${KERNELS[@]}"; do
      if [ "$k" = "$SINGLE_KERNEL" ]; then
        run_test "Kernel Battery: ${k}" \
          "$PHASE7_DIR/kernels/${k}.test.js" \
          || OVERALL_RESULT=1
        found=true
        break
      fi
    done
    if [ "$found" = false ]; then
      echo "Unknown kernel: $SINGLE_KERNEL"
      echo "Available: ${KERNELS[*]}"
      OVERALL_RESULT=1
    fi
  else
    for k in "${KERNELS[@]}"; do
      run_test "Kernel Battery: ${k}" \
        "$PHASE7_DIR/kernels/${k}.test.js" \
        || OVERALL_RESULT=1
    done
  fi
fi

# ── Run integration tests ────────────────────────────────────────────────────
if [ "$RUN_INTEGRATION" = true ]; then
  echo ""
  echo "┌──────────────────────────────────────────────────────────┐"
  echo "│  PHASE 7 — RUNTIME-WIDE INTEGRATION                       │"
  echo "│  (cross-kernel, multi-tick, observability)               │"
  echo "└──────────────────────────────────────────────────────────┘"

  run_test "Full Runtime Composition" \
    "$PHASE7_DIR/integration/full-runtime-composition.test.js" \
    || OVERALL_RESULT=1

  run_test "Multi-Tick Survival (tier=${TIER})" \
    "$PHASE7_DIR/integration/multi-tick-survival.test.js" \
    "PHASE7_CADENCE_TIER=${TIER}" \
    || OVERALL_RESULT=1

  run_test "Cross-Kernel Event Causality" \
    "$PHASE7_DIR/integration/cross-kernel-event-causality.test.js" \
    || OVERALL_RESULT=1

  run_test "Governance Boundaries" \
    "$PHASE7_DIR/integration/governance-boundaries.test.js" \
    || OVERALL_RESULT=1

  run_test "Capability as Dependency" \
    "$PHASE7_DIR/integration/capability-as-dependency.test.js" \
    || OVERALL_RESULT=1

  run_test "Observability Coverage" \
    "$PHASE7_DIR/integration/observability-coverage.test.js" \
    || OVERALL_RESULT=1
fi

# ── Report ────────────────────────────────────────────────────────────────────
ELAPSED=$(($(date +%s) - START_TIME))
ELAPSED_MIN=$((ELAPSED / 60))
ELAPSED_SEC=$((ELAPSED % 60))

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Phase 7 Runtime Validation Complete"
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
  'ls -la /app/tests/phase-7/reports/ 2>/dev/null || echo "  (no reports)"' || true

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
