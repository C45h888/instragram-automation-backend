// backend.api/services/sync/index.js
// BOOT WRAPPER for token health checks. Pure delegation to graph-capability-kernel.
//
// Migration status (2026-06-07):
//   The legacy services/sync/token-health.js has been migrated to:
//     graph-capability-kernel/substrates/health-substrate/
//   - index.js (orchestrator façade)
//   - workers/scan-credentials-worker.js
//   - workers/recovery-worker.js
//   - workers/uat-refresh-worker.js
//   - workers/data-access-expiry-worker.js
//
// This file is the BOOT CHOREOGRAPHY WRAPPER. server.js calls runStartupHealthChecks
// once at boot. The actual health logic now lives inside the graph-capability
// kernel's health substrate.
//
// Future pass (NOT in this migration):
//   server.js → CK → graph-capability-FSM → health-substrate boot choreography.
//   Currently: server.js → this wrapper → health substrate directly.
//   The CK↔FSM↔server relationship is the next architectural pass.

const { runTokenHealthCheck, runUATRefreshCheck, start: startHealth, stop: stopHealth } = require('../../graph-capability-kernel/substrates/health-substrate');

// ── Startup health checks ────────────────────────────────────────────────────

/**
 * Runs token health and UA token refresh checks once at startup.
 * Called by server.js after DB init. Non-fatal — errors are logged, not thrown.
 *
 * Idempotent on the substrate: the substrate's start() guards against double-start.
 */
async function runStartupHealthChecks() {
  console.log('[Health] Running startup token health checks...');
  startHealth();
  try {
    await runTokenHealthCheck();
    await runUATRefreshCheck();
    console.log('[Health] Token health checks complete');
  } catch (err) {
    console.error('[Health] Token health check failed:', err.message);
  }
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  runStartupHealthChecks,
  runTokenHealthCheck,
  runUATRefreshCheck,
  stopHealth,
};
