// graph-capability-kernel/substrates/capability-check-substrate/workers/quota-intelligence-worker.js
// Quota Intelligence Worker (substrate wrapper): reads quota state from the
// existing QuotaIntelligenceWorker membrane.
//
// Owns: nothing — reads state only.
// Does NOT own: rolling windows, header recording, pressure transitions.
//
// Contract: execute(params, governance) → { quotaState }

const fsm = require('../../../fsm');

async function execute(params, governance) {
  const { businessAccountId } = params;

  // Access the wired quota-intelligence membrane from the FSM
  const qEntry = fsm.getMembraneEntry('quota-intelligence');

  if (!qEntry || !qEntry.wired || !qEntry.substrate) {
    return { quotaState: 'NONE' };
  }

  // The substrate has a getState() method that returns the current quota snapshot
  const substrate = qEntry.substrate;
  const state = typeof substrate.getState === 'function'
    ? substrate.getState()
    : { state: substrate._state || 'NONE' };

  return {
    quotaState: state.state || 'NONE',
  };
}

module.exports = { execute };
