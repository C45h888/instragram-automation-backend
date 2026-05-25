// backend.api/services/sync/index.js
// Pure function module — no timers, no cron, no scheduling.
// Token health checks run once at startup.
//
// External callers:
//   server.js → require('./services/sync').runStartupHealthChecks

const { runTokenHealthCheck, runUATRefreshCheck } = require('./token-health');
const { isAccountRateLimited, markAccountRateLimited } = require('../../substrates/retry');

// ── Startup health checks ────────────────────────────────────────────────────

/**
 * Runs token health and UA token refresh checks once at startup.
 * Called by server.js after DB init. Non-fatal — errors are logged, not thrown.
 */
async function runStartupHealthChecks() {
  console.log('[Health] Running startup token health checks...');
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
  isAccountRateLimited,
  markAccountRateLimited,
};
