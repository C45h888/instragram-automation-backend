#!/bin/bash
# ============================================
# Phase 8 — Cross-Kernel Communication Runner
# ============================================
# Extends docker-compose.test.yml (adds the webhook-simulator
# service) and exercises:
#   - constitutional-flow/  (4 tests)
#   - webhook/              (4 tests)
#   - cross-kernel/         (20 tests — 5x4 directed matrix)
#   - integration/          (3 tests)
#
# Total: 31 tests, each writes a per-test JSON report.
#
# Cadence tiers (heavier than phase-7, since these tests hit
# real workers over the network):
#   short   = 50   (every commit)
#   medium  = 250  (CI, default)
#   long    = 1000 (manual, pre-release)
#
# Usage:
#   ./tests/phase-8/phase-8-runner.sh                          # medium tier, all suites
#   ./tests/phase-8/phase-8-runner.sh --tier=short
#   ./tests/phase-8/phase-8-runner.sh --tier=long
#   ./tests/phase-8/phase-8-runner.sh --suite=constitutional
#   ./tests/phase-8/phase-8-runner.sh --suite=webhook
#   ./tests/phase-8/phase-8-runner.sh --suite=cross-kernel
#   ./tests/phase-8/phase-8-runner.sh --suite=integration
#   ./tests/phase-8/phase-8-runner.sh --keep-up
#   ./tests/phase-8/phase-8-runner.sh --diagram-only
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"
PHASE8_DIR="tests/phase-8"

KEEP_UP=false
TIER="medium"
SUITE_FILTER=""

# ── Defaults ───────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Phase 8 — Cross-Kernel Communication Runner

Usage:
  $0 [options]

Options:
  --tier=SHORT|MEDIUM|LONG   Cadence tier (default: medium)
  --suite=NAME               Run a single suite (constitutional|webhook|cross-kernel|integration)
  --keep-up                  Leave Docker stack running after tests
  --diagram-only             Render the diagram and exit (no tests)
  --help                     Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --tier=*)     TIER="${1#*=}"; shift ;;
    --suite=*)    SUITE_FILTER="${1#*=}"; shift ;;
    --keep-up)    KEEP_UP=true; shift ;;
    --diagram-only) DIAGRAM_ONLY=true; shift ;;
    --help|-h)    usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

case "$TIER" in
  short|medium|long) ;;
  *) echo "Invalid tier: $TIER"; exit 1 ;;
esac

# ── Diagram ────────────────────────────────────────────────────────────
echo "[phase-8] Rendering architecture diagram..."
node "$SCRIPT_DIR/runtime/render-diagram.mjs" || echo "[phase-8] diagram render failed (non-fatal)"

if [ "${DIAGRAM_ONLY:-false}" = "true" ]; then
  echo "[phase-8] diagram-only mode — done"
  exit 0
fi

# ── Print coverage matrix ──────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Phase 8 — Cross-Kernel Communication Suite"
echo "  Cadence tier: ${TIER}"
echo "  Coverage matrix:"
echo "    capability   →  acquisition, publishing, recovery, insights"
echo "    acquisition  →  capability, publishing, recovery, insights"
echo "    publishing   →  capability, acquisition, recovery, insights"
echo "    recovery     →  capability, acquisition, publishing, insights"
echo "    insights     →  capability, acquisition, publishing, recovery"
echo "  2 constitutional flow paths:"
echo "    A. Webhook → Ingress → Parser → Governance → FSM → Worker → State"
echo "    B. Graph  → Worker  → Governance → State"
echo "════════════════════════════════════════════════════════════"
echo ""

# ── Ensure stack is up ────────────────────────────────────────────────
STACK_UP=$(docker-compose -f "$COMPOSE_FILE" ps --services --filter "status=running" 2>/dev/null | wc -l | tr -d ' ')

if [ "$STACK_UP" -lt 4 ]; then
  echo "[runner] Stack not fully up (${STACK_UP}/4 services). Starting..."
  docker-compose -f "$COMPOSE_FILE" up -d
  echo "[runner] Waiting for services to be healthy..."
  for i in $(seq 1 30); do
    REDIS_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-redis redis-cli ping 2>/dev/null || echo "FAIL")
    PG_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T test-postgres pg_isready -U testuser -d testgovernance 2>/dev/null || echo "FAIL")
    WH_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T webhook-simulator sh -c "wget -q -O- http://127.0.0.1:9200/health 2>/dev/null || exit 1" 2>/dev/null && echo OK || echo "FAIL")
    GR_OK=$(docker-compose -f "$COMPOSE_FILE" exec -T graph-simulator sh -c "wget -q -O- http://127.0.0.1:9100/v1/accounts 2>/dev/null || exit 1" 2>/dev/null && echo OK || echo "FAIL")
    if [ "$REDIS_OK" = "PONG" ] && echo "$PG_OK" | grep -q "accepting" && [ "$WH_OK" = "OK" ] && [ "$GR_OK" = "OK" ]; then
      echo "[runner] Stack healthy (Redis, Postgres, Webhook-sim, Graph-sim)."
      break
    fi
    if [ $i -eq 30 ]; then
      echo "[runner] ERROR: Timeout. Dumping logs:"
      docker-compose -f "$COMPOSE_FILE" logs --tail=50
      exit 1
    fi
    sleep 1
  done
else
  echo "[runner] Stack already up (${STACK_UP}/4 services running)."
fi

# ── Run a single vitest file inside the test-runner ───────────────────
run_file() {
  local label="$1"
  local file="$2"
  local extra_env="$3"

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  ${label}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  local env_args=()
  if [ -n "$extra_env" ]; then
    for kv in $extra_env; do env_args+=("-e" "$kv"); done
  fi

  docker-compose -f "$COMPOSE_FILE" exec -T "${env_args[@]}" test-runner \
    npx vitest run "$file" \
    --reporter=verbose \
    --config /app/$PHASE8_DIR/vitest.config.js \
    || return 1
  return 0
}

# ── Walk the manifest ─────────────────────────────────────────────────
OVERALL=0
START_TIME=$(date +%s)

if [ -n "$SUITE_FILTER" ]; then
  case "$SUITE_FILTER" in
    constitutional) run_file "constitutional-flow/*" "$PHASE8_DIR/constitutional-flow/webhook-to-state.test.mjs" "" || OVERALL=1
                    run_file "constitutional-flow/*" "$PHASE8_DIR/constitutional-flow/graph-to-state.test.mjs" "" || OVERALL=1
                    run_file "constitutional-flow/*" "$PHASE8_DIR/constitutional-flow/worker-subordination.test.mjs" "" || OVERALL=1
                    run_file "constitutional-flow/*" "$PHASE8_DIR/constitutional-flow/retry-cadence-survival.test.mjs" "" || OVERALL=1 ;;
    webhook)        for f in ingress-parser governance-decision no-unrelated-mutation webhook-chaos; do
                      run_file "webhook/$f" "$PHASE8_DIR/webhook/$f.test.mjs" "" || OVERALL=1
                    done ;;
    cross-kernel)   for f in $(ls "$SCRIPT_DIR/cross-kernel"/*.test.mjs | xargs -n1 basename); do
                      run_file "cross-kernel/$f" "$PHASE8_DIR/cross-kernel/$f" "" || OVERALL=1
                    done ;;
    integration)    run_file "integration/phase-8-full-composition" "$PHASE8_DIR/integration/phase-8-full-composition.test.mjs" "" || OVERALL=1
                    run_file "integration/phase-8-multi-tick-survival" "$PHASE8_DIR/integration/phase-8-multi-tick-survival.test.mjs" "PHASE8_CADENCE_TIER=$TIER" || OVERALL=1
                    run_file "integration/phase-8-architectural-drift" "$PHASE8_DIR/integration/phase-8-architectural-drift.test.mjs" "" || OVERALL=1 ;;
    *) echo "Unknown suite: $SUITE_FILTER"; exit 1 ;;
  esac
else
  # Full sweep
  echo "┌─ constitutional-flow/ ─────────────────────────────────┐"
  for t in webhook-to-state graph-to-state worker-subordination retry-cadence-survival; do
    run_file "constitutional-flow/$t" "$PHASE8_DIR/constitutional-flow/$t.test.mjs" "" || OVERALL=1
  done

  echo ""
  echo "┌─ webhook/ ──────────────────────────────────────────────┐"
  for t in ingress-parser governance-decision no-unrelated-mutation webhook-chaos; do
    run_file "webhook/$t" "$PHASE8_DIR/webhook/$t.test.mjs" "" || OVERALL=1
  done

  echo ""
  echo "┌─ cross-kernel/ (20 pairs) ──────────────────────────────┐"
  for f in $(ls "$SCRIPT_DIR/cross-kernel"/*.test.mjs | xargs -n1 basename); do
    run_file "cross-kernel/$f" "$PHASE8_DIR/cross-kernel/$f" "" || OVERALL=1
  done

  echo ""
  echo "┌─ integration/ ─────────────────────────────────────────┐"
  run_file "integration/phase-8-full-composition" "$PHASE8_DIR/integration/phase-8-full-composition.test.mjs" "" || OVERALL=1
  run_file "integration/phase-8-multi-tick-survival" "$PHASE8_DIR/integration/phase-8-multi-tick-survival.test.mjs" "PHASE8_CADENCE_TIER=$TIER" || OVERALL=1
  run_file "integration/phase-8-architectural-drift" "$PHASE8_DIR/integration/phase-8-architectural-drift.test.mjs" "" || OVERALL=1
fi

ELAPSED=$(($(date +%s) - START_TIME))
EM=$((ELAPSED / 60)); ES=$((ELAPSED % 60))

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Phase 8 Complete — elapsed ${EM}m ${ES}s"
if [ $OVERALL -eq 0 ]; then
  echo "  Result: ALL PASSED"
else
  echo "  Result: SOME FAILED"
fi
echo "  Reports: $PHASE8_DIR/reports/"
echo "════════════════════════════════════════════════════════════"

# Show report inventory
echo ""
echo "  Per-test JSON reports:"
docker-compose -f "$COMPOSE_FILE" exec -T test-runner \
  sh -c "find /app/$PHASE8_DIR/reports -name '*.json' 2>/dev/null | sort" || true

if [ "$KEEP_UP" = true ]; then
  echo ""
  echo "[runner] Stack left running (--keep-up)."
else
  echo ""
  echo "[runner] Tearing down stack..."
  docker-compose -f "$COMPOSE_FILE" down
fi

exit $OVERALL
