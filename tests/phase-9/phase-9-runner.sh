#!/bin/bash
# ============================================
# Phase 9 — Runtime Verification Runner
# ============================================
# Runs the phase-9 test suite inside the
# test-runner container (same pattern as phase-7/8).
#
# Cadence tiers:
#   short  = 25   (default, every commit)
#   medium = 100  (CI)
#   long   = 500  (manual, pre-release)
#   epic   = 1000 (full soak)
#
# Usage:
#   ./tests/phase-9/phase-9-runner.sh                          # short tier
#   ./tests/phase-9/phase-9-runner.sh --tier=medium
#   ./tests/phase-9/phase-9-runner.sh --tier=long
#   ./tests/phase-9/phase-9-runner.sh --tier=epic
#   ./tests/phase-9/phase-9-runner.sh --keep-up
#   ./tests/phase-9/phase-9-runner.sh --help
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"
PHASE9_DIR="tests/phase-9"

KEEP_UP=false
TIER="short"

usage() {
  cat <<EOF
Phase 9 — Runtime Verification Runner

Usage:
  $0 [options]

Options:
  --tier=SHORT|MEDIUM|LONG|EPIC  Cadence tier (default: short)
  --keep-up                       Leave Docker stack running after tests
  --help                          Show this help

Tiers:
  short  25 ticks    (default, every commit)
  medium 100 ticks   (CI)
  long   500 ticks   (manual, pre-release)
  epic   1000 ticks  (full soak)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*)  TIER="${1#*=}" ;;
    --keep-up) KEEP_UP=true ;;
    --help|-h) usage; exit 0 ;;
    *)         echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
  shift
done

# Map tier to a tick count for the multi-tick tests.
case "$TIER" in
  short)  TICKS=25  ;;
  medium) TICKS=100 ;;
  long)   TICKS=500 ;;
  epic)   TICKS=1000 ;;
  *)      echo "Unknown tier: $TIER"; usage; exit 1 ;;
esac

export PHASE9_CADENCE_TIER="$TIER"

echo "============================================"
echo "  Phase 9 — Runtime Verification"
echo "  Tier: $TIER  (${TICKS} ticks)"
echo "============================================"

# Bring the test stack up.
docker compose -f "$COMPOSE_FILE" up -d
trap '[ "$KEEP_UP" = true ] || docker compose -f "$COMPOSE_FILE" down' EXIT

# Run the tests inside the test-runner container.
docker exec instagram-test-runner \
  env PHASE9_CADENCE_TIER="$TIER" \
  node --experimental-vm-modules node_modules/vitest/vitest.mjs run \
  --config "$PHASE9_DIR/vitest.config.js" \
  "$PHASE9_DIR"

echo
echo "============================================"
echo "  Phase 9 complete"
echo "  Reports: $PHASE9_DIR/reports/"
echo "============================================"
