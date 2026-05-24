// control-plane/runtime/operational-safety.js
// Operational Safety: bounded operational health checks.
//
// Owns: calling heartbeat failover via canonical db-worker.
// Does NOT own: DB reads/writes (delegates to db-worker), evaluation, emission.
//
// Contract:
//   safety.runChecks()  → run heartbeat failover via db-worker

const dbWorker = require('../execution/db-worker');

const HEARTBEAT_STALE_MINUTES = parseInt(process.env.HEARTBEAT_STALE_MINUTES || '30', 10);

/**
 * Run operational safety checks.
 * Delegates all DB operations to the canonical db-worker.
 *
 * @returns {Promise<{ok: boolean, error?: string}>}
 */
async function runChecks() {
  try {
    await dbWorker.heartbeatFailover(HEARTBEAT_STALE_MINUTES);
    return { ok: true };
  } catch (err) {
    console.error('[safety] Heartbeat failover error:', err.message);
    return { ok: false, error: err.message };
  }
}

module.exports = { runChecks };
