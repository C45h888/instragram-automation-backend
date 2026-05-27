// control-plane/governance/interpreters/recon-lineage-interpreter.js
// Reconciliation Lineage Interpreter: full observability for reconciliation engine.
//
// Owns: providing reconciliation engine with complete lineage access.
// Does NOT own: FSM domain filtering, HSM hierarchical analysis.
//
// The reconciliation engine requires full lineage observability to perform
// three-reality reconciliation (lineage ledger + runtime projections + substrate).
// Reconciliation must see all entries to detect anomalies and verify constitutional
// convergence across all domains.
//
// Filter criteria: NONE (pass-through — full observability)
//
// Canonical single source: lineage:ledger:entries (unchanged)

const lineageLedger = require('../lineage-ledger');

/**
 * Get all lineage entries for reconciliation (full observability).
 *
 * @param {number} [n] — optional limit on entries returned
 * @returns {Promise<Array<object>>} complete lineage entries
 */
async function getReconLineage(n) {
  return lineageLedger.getLineage(n);
}

/**
 * Get lineage entries grouped by domain for reconciliation analysis.
 *
 * @param {number} [n] — optional limit per domain
 * @returns {Promise<object>} map of domain -> entries
 */
async function getDomainGroupedLineage(n) {
  const domains = ['acquisition', 'publishing', 'scheduling', 'governance'];
  const result = {};

  for (const domain of domains) {
    result[domain] = await lineageLedger.getDomainLineage(domain, n);
  }

  return result;
}

/**
 * Get the constitutional hash for reconciliation verification.
 *
 * @returns {Promise<string>} SHA-256 hash
 */
async function getConstitutionalHash() {
  return lineageLedger.computeHash();
}

/**
 * Get divergence entries for reconciliation analysis.
 * These are structural anomalies recorded by the lineage worker.
 *
 * @param {number} [n] — optional limit
 * @returns {Promise<Array<object>>} divergence entries
 */
async function getDivergenceEntries(n) {
  const allEntries = await lineageLedger.getLineage(n);
  return allEntries.filter(entry => entry.entryType === 'divergence');
}

/**
 * Get projection snapshot entries for reconciliation comparison.
 *
 * @param {number} [n] — optional limit
 * @returns {Promise<Array<object>>} projection snapshot entries
 */
async function getProjectionSnapshotEntries(n) {
  const allEntries = await lineageLedger.getLineage(n);
  return allEntries.filter(entry => entry.entryType === 'projection_snapshot');
}

/**
 * Get health entries for reconciliation health verification.
 *
 * @param {number} [n] — optional limit
 * @returns {Promise<Array<object>>} health entries
 */
async function getHealthEntries(n) {
  const allEntries = await lineageLedger.getLineage(n);
  return allEntries.filter(entry => entry.entryType === 'health');
}

/**
 * Materialize state for reconciliation from complete lineage.
 *
 * @returns {Promise<{ globalState: string, domains: object, lastEvent: object|null, entryCount: number }>}
 */
async function materializeForReconciliation() {
  const entries = await lineageLedger.getLineage();
  return lineageLedger.materializeState(entries);
}

module.exports = {
  getReconLineage,
  getDomainGroupedLineage,
  getConstitutionalHash,
  getDivergenceEntries,
  getProjectionSnapshotEntries,
  getHealthEntries,
  materializeForReconciliation,
};
