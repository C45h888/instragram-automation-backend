// control-plane/governance/domains/reading-substrate/index.js
// Reading Substrate: governance dispatch layer for governed reads.
//
// Owns: concurrent read tracking, aggregate health signals,
//        delegation to substrates/db/reading/ (the operational substrate).
// Does NOT own: governance policy (FSM), routing (CK),
//               domain-specific read logic (substrate workers).
//
// Instantiated by CK. FSM delegates reads here.
//
// Architecture:
//   CK → FSM(gate) → reading-substrate/index.js (governance dispatch)
//   → substrates/db/reading/index.js (operational substrate)
//   → registry → worker.execute()

const dbReading = require('../../../../substrates/db/reading');

// ── Governance reference — set by CK at boot ────────────────────────────────
let _governance = null;
let _fsm = null;

function init(govContext) {
  _governance = govContext.governance;
  _fsm = govContext.fsm;

  // Wire governance into the operational substrate
  dbReading.setGovernance(_governance);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Concurrent Read Tracking
// ═══════════════════════════════════════════════════════════════════════════════

let _readsInFlight = 0;
const _activeReads = new Map(); // readId → { domain, accountId, startedAt }

function getReadsInFlight() { return _readsInFlight; }

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Execute Governed Read — delegates to operational substrate
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Execute a governed read operation.
 * Called by persist-telemetry-fsm after gate validation.
 * Delegates to substrates/db/reading/ — the operational plane.
 *
 * @param {string} domain   — read domain (e.g., 'db.media')
 * @param {object} params   — { accountId, query, ...domain-specific }
 * @param {string} readId   — unique read identifier for tracking
 * @returns {Promise<object>} { success, data, error, latencyMs, cached? }
 */
async function executeRead(domain, params, readId) {
  const startTime = Date.now();

  // Track in-flight
  _readsInFlight++;
  _activeReads.set(readId, { domain, accountId: params.accountId, startedAt: startTime });

  try {
    const result = await dbReading.dispatchRead(domain, params, readId);

    _readsInFlight = Math.max(0, _readsInFlight - 1);
    _activeReads.delete(readId);

    return result;
  } catch (err) {
    _readsInFlight = Math.max(0, _readsInFlight - 1);
    _activeReads.delete(readId);

    return {
      success: false,
      data: null,
      error: err.message,
      latencyMs: Date.now() - startTime,
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Health
// ═══════════════════════════════════════════════════════════════════════════════

function getHealth() {
  return {
    ok: _readsInFlight < 20,
    readsInFlight: _readsInFlight,
    domains: dbReading.getDomains(),
  };
}

function getDomainWhitelist() {
  return dbReading.getDomains();
}

// ── Cache invalidation — delegates to substrate ──────────────────────────────

function invalidateCache(domain, accountId) {
  dbReading.invalidateCache(domain, accountId);
}

function invalidateAllCaches() {
  dbReading.invalidateAllCaches();
}

module.exports = {
  init,
  executeRead,
  getHealth,
  getDomainWhitelist,
  getReadsInFlight,
  invalidateCache,
  invalidateAllCaches,
};
