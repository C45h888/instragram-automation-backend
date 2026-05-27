// control-plane/governance/interpreters/fsm-lineage-interpreter.js
// FSM Lineage Interpreter: bounded namespace filter for FSM consumers.
//
// Owns: filtering lineage entries by FSM domain jurisdiction.
// Does NOT own: telemetry interpretation, reconciliation lineage.
//
// FSM consumers only see entries within their constitutional domain.
// This ensures bounded authority isolation — FSMs cannot observe
// transitions outside their jurisdiction.
//
// Filter criteria: domain field matches FSM's registered domain(s)
//
// Canonical single source: lineage:ledger:entries (unchanged)
// Interpreters apply namespace filtering at read time.

const lineageLedger = require('../lineage-ledger');

// FSM domain registry — each FSM has bounded jurisdiction
const FSM_DOMAINS = {
  acquisition: 'acquisition',
  publishing: 'publishing',
  scheduling: 'scheduling',
};

/**
 * Get lineage entries filtered to a specific FSM domain.
 * Returns only entries where domain matches the FSM's jurisdiction.
 *
 * @param {string} domainName — 'acquisition' | 'publishing' | 'scheduling'
 * @param {number} [n] — optional limit on entries returned
 * @returns {Promise<Array<object>>} filtered lineage entries
 */
async function getFSMLineage(domainName, n) {
  if (!FSM_DOMAINS[domainName]) {
    console.warn(`[fsm-lineage-interpreter] Unknown FSM domain: ${domainName}`);
    return [];
  }

  const allLineage = await lineageLedger.getLineage(n);
  return allLineage.filter(entry => entry.domain === domainName);
}

/**
 * Get the current state for a specific FSM domain from lineage.
 *
 * @param {string} domainName
 * @returns {Promise<object|null>} last entry for domain or null
 */
async function getFSMDomainState(domainName) {
  const entries = await getFSMLineage(domainName, 1);
  return entries.length > 0 ? entries[entries.length - 1] : null;
}

/**
 * Materialize FSM domain state from filtered lineage entries.
 *
 * @param {string} domainName
 * @returns {Promise<{ state: string, transitionCount: number, lastTs: number|null }>}
 */
async function materializeFSMState(domainName) {
  const entries = await getFSMLineage(domainName);
  if (!entries || entries.length === 0) {
    return { state: 'IDLE', transitionCount: 0, lastTs: null };
  }

  let lastEntry = null;
  for (const entry of entries) {
    if (entry.domain === domainName) {
      lastEntry = entry;
    }
  }

  return {
    state: lastEntry ? lastEntry.nextState : 'IDLE',
    transitionCount: entries.length,
    lastTs: lastEntry ? lastEntry.timestamp : null,
  };
}

/**
 * Check if an entry is within FSM jurisdiction.
 *
 * @param {object} entry — lineage entry
 * @param {string} domainName — FSM domain to check against
 * @returns {boolean}
 */
function isInJurisdiction(entry, domainName) {
  return entry.domain === domainName;
}

/**
 * Get all FSM domains currently tracked in lineage.
 *
 * @param {number} [n] — number of recent entries to inspect
 * @returns {Promise<Array<string>>} list of active domains
 */
async function getActiveDomains(n = 100) {
  const entries = await lineageLedger.getLineage(n);
  const domains = new Set();
  for (const entry of entries) {
    if (entry.domain) domains.add(entry.domain);
  }
  return [...domains];
}

module.exports = {
  FSM_DOMAINS,
  getFSMLineage,
  getFSMDomainState,
  materializeFSMState,
  isInJurisdiction,
  getActiveDomains,
};
