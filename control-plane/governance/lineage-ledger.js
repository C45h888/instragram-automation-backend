// control-plane/governance/lineage-ledger.js
// Lineage Ledger: append-only governance event log.
//
// Owns: immutable event recording, lineage retrieval, state materialization.
// Does NOT own: state transitions, governance policy, action emission.
//
// Used by BOTH the constitutional kernel and domain FSMs.
// Every governance event — constitutional or domain — writes here.
// Current state is a materialized projection derived from lineage,
// not the primary source of truth.
//
// Contract:
//   ledger.record(entry)          → lineageId
//   ledger.getLineage([n])         → Array<GovernanceEvent>
//   ledger.getSize()               → number

const crypto = require('crypto');

const _lineage = []; // append-only, never mutated or deleted

/**
 * Record a governance event in the append-only lineage log.
 *
 * @param {{ authority: string, layer: 'constitutional'|'domain',
 *           intent: string, priorState: string, resultantState: string,
 *           legitimacy?: object, meta?: object }} entry
 * @returns {{ id: string, ts: number }} the recorded entry identifiers
 */
function record(entry) {
  const event = {
    id: crypto.randomUUID(),
    ts: Date.now(),
    authority: entry.authority || 'unknown',
    layer: entry.layer || 'domain',
    intent: entry.intent,
    priorState: entry.priorState,
    resultantState: entry.resultantState,
    legitimacy: entry.legitimacy || null,
    meta: entry.meta || {},
  };
  _lineage.push(event);
  return { id: event.id, ts: event.ts };
}

/**
 * Returns the last N lineage records (or all if n is omitted).
 * Records are append-only and never mutated.
 *
 * @param {number} [n] — number of recent records to return
 * @returns {Array<object>}
 */
function getLineage(n) {
  if (typeof n === 'number' && n > 0) {
    return _lineage.slice(-n);
  }
  return [..._lineage];
}

/**
 * Returns total number of recorded lineage events.
 * @returns {number}
 */
function getSize() {
  return _lineage.length;
}

module.exports = { record, getLineage, getSize };
