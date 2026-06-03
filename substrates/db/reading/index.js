// substrates/db/reading/index.js
// DB Reading Substrate: governed read dispatch layer.
//
// Owns: dispatching read operations to domain-bounded workers.
// Does NOT own: governance policy (persist-telemetry-fsm),
//               read execution logic (workers), IG API calls.
//
// Mirrors substrates/db/writers/index.js pattern.
//
// Flow: FSM → readingSubstrate.dispatchRead() → registry → worker.execute()
//   → CK(DB_READ_COMPLETE)

const registry = require('./registry');

let _governance = null;

function setGovernance(gov) { _governance = gov; }

/**
 * Dispatch a governed read to the appropriate domain worker.
 * Async — returns Promise. Worker emits completion via governance.
 *
 * @param {string} domain   — read domain ('db.media')
 * @param {object} params   — { accountId, query }
 * @param {string} readId   — unique read identifier
 * @returns {Promise<{success: boolean, data?, error?, latencyMs: number, cached?: boolean}>}
 */
async function dispatchRead(domain, params, readId) {
  const worker = registry.getWorker(domain);

  if (!worker) {
    if (_governance) {
      _governance.dispatch({
        type: 'DB_READ_COMPLETE',
        readDomain: domain,
        accountId: params.accountId,
        readId,
        success: false,
        error: `unknown_read_domain: ${domain}`,
        latencyMs: 0,
      });
    }
    return { success: false, data: null, error: `unknown_read_domain: ${domain}`, latencyMs: 0 };
  }

  return worker.execute(params, _governance);
}

// ── Cache management ─────────────────────────────────────────────────────────

function invalidateCache(domain, accountId) {
  const worker = registry.getWorker(domain);
  if (worker && typeof worker.clearCache === 'function') {
    worker.clearCache(accountId);
  }
}

function invalidateAllCaches() {
  for (const domain of registry.getDomains()) {
    invalidateCache(domain);
  }
}

module.exports = {
  dispatchRead,
  setGovernance,
  invalidateCache,
  invalidateAllCaches,
  getDomains: registry.getDomains,
};
