#!/bin/sh
# ============================================
# Docker Test Entrypoint — Transparent Gateway
# Constitutional Runtime Test Suite
# ============================================
# Lives at /app/docker-entrypoint.sh inside test-runner container.
#
# This entrypoint is intentionally minimal. It does NOT perform health
# checks or Redis flushes — those are owned by:
#   - run-all-tests.sh (host-side): stack lifecycle and service health
#   - vitest setup files (container-side): Redis test:* keyspace management
#
# Duplicating health checks or flushes here creates a race with vitest's
# own SCAN-based flush in test-setup.js and double-validates services
# already confirmed by compose healthchecks + the host runner.
#
# Default CMD (no args): sleep infinity for interactive use.
# All other commands pass through transparently.
# ============================================

set -e

if [ $# -eq 0 ]; then
    exec sleep infinity
fi

exec "$@"