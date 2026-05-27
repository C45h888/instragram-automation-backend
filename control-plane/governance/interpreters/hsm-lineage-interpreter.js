// control-plane/governance/interpreters/hsm-lineage-interpreter.js
// HSM Lineage Interpreter: full observability pass-through for HSM consumers.
//
// Owns: providing HSM with complete lineage access for hierarchical state evaluation.
// Does NOT own: FSM domain filtering, reconciliation lineage interpretation.
//
// HSM (Hierarchical State Machine) requires full observability across all domains
// to evaluate state hierarchies and cross-domain transitions. HSM does NOT filter
// by domain — it sees the complete lineage for hierarchical analysis.
//
// Filter criteria: NONE (pass-through — full observability)
//
// Canonical single source: lineage:ledger:entries (unchanged)

const lineageLedger = require('../lineage-ledger');

/**
 * Get all lineage entries (full observability for HSM).
 * No domain filtering — HSM needs complete lineage for hierarchical evaluation.
 *
 * @param {number} [n] — optional limit on entries returned
 * @returns {Promise<Array<object>>} complete lineage entries
 */
async function getHSMLineage(n) {
  return lineageLedger.getLineage(n);
}

/**
 * Get lineage entries across multiple domains for hierarchical analysis.
 *
 * @param {Array<string>} domainNames — domains to include
 * @param {number} [n] — optional limit per domain
 * @returns {Promise<object>} map of domain -> entries
 */
async function getCrossDomainLineage(domainNames, n) {
  const result = {};
  for (const domain of domainNames) {
    const entries = await lineageLedger.getDomainLineage(domain, n);
    result[domain] = entries;
  }
  return result;
}

/**
 * Get hierarchical state transitions (entries with parentTransitionId).
 * HSM uses these to analyze state hierarchy and causation chains.
 *
 * @param {number} [n] — optional limit
 * @returns {Promise<Array<object>>} entries with hierarchical causation
 */
async function getHierarchicalTransitions(n) {
  const allEntries = await lineageLedger.getLineage(n);
  return allEntries.filter(entry => entry.parentTransitionId != null);
}

/**
 * Materialize global state from complete lineage for HSM evaluation.
 *
 * @returns {Promise<{ globalState: string, domains: object, entryCount: number }>}
 */
async function materializeGlobalState() {
  const entries = await lineageLedger.getLineage();
  return lineageLedger.materializeState(entries);
}

/**
 * Get state transitions for a specific entity across all domains.
 * HSM may need to track entity state across domain boundaries.
 *
 * @param {string} entityId — entity identifier
 * @param {number} [n] — optional limit
 * @returns {Promise<Array<object>>} entries for this entity
 */
async function getEntityLineage(entityId, n) {
  const allEntries = await lineageLedger.getLineage(n);
  return allEntries.filter(entry => entry.entityId === entityId);
}

module.exports = {
  getHSMLineage,
  getCrossDomainLineage,
  getHierarchicalTransitions,
  materializeGlobalState,
  getEntityLineage,
};
