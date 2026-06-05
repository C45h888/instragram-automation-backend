// reconciliation-kernel/worker.js
// Reconciliation Worker: execution-blind operational unit.
//
// Bounded to: Reconciliation Substrate
// Receives: data only (entries, fsms, substrates)
// Does NOT touch: dispatch, checkpointer, lineageLedger, governance decisions
//
// Cognitive invariant: worker is a dumb execution unit.
// It receives structured data and returns structured data.
// No awareness of governance topology, checkpoint policy, or orchestration context.

const engine = require('./engine');

// Lazy — substrate provides lineageLedger at call time, not at construction
let _lineageLedgerRef = null;

function _getLineageLedger() {
  if (!_lineageLedgerRef) {
    _lineageLedgerRef = require('../control-plane/governance/lineage-ledger');
  }
  return _lineageLedgerRef;
}

/**
 * Run a reconciliation cycle against an immutable constitutional snapshot.
 *
 * @param {object} params
 * @param {Array}  params.entries       — T0 snapshot entries from substrate
 * @param {Map}    params.fsms          — domain FSMs map (from CK _domains)
 * @param {object} params.substrates    — query interface from substrate
 *
 * @returns {Promise<{ observations: Array, worstSeverity: number, hash: string }>}
 */
async function run({ entries, fsms, substrates }) {
  const lineageLedger = _getLineageLedger();

  // Engine is cognitively blind — receives data only
  // snapshotEntries is the T0 constitutional snapshot (no Redis re-read)
  const results = await engine.compare({
    fsms,
    substrates,
    lineageLedger,
    snapshotEntries: entries,
  });

  return {
    observations: results.observations || [],
    worstSeverity: results.worstSeverity || 0,
    hash: results.hash || '',
  };
}

module.exports = { run };