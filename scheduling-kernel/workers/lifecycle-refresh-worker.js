// scheduling-kernel/workers/lifecycle-refresh-worker.js
// Lifecycle Refresh Worker: bounded execution unit for account discovery.
//
// Owns: executing a single lifecycle refresh — comparing provided accounts
//       against the lifecycle substrate's membership set.
// Does NOT own: governance policy, observability emission, data sourcing
//               — those belong to the FSM.
//
// Contract:
//   worker.execute({ accounts }) → { added, removed, currentIds }
//
// The FSM performs the governed read through CK, then invokes this worker
// with the account data. The worker delegates to the lifecycle substrate
// (pure mechanics) and returns deltas. The FSM owns the governance decision
// of what to DO with those deltas.

const lifecycle = require('../substrates/cadence/lifecycle');

function execute(params = {}) {
  const accounts = params.accounts || [];
  const result = lifecycle.refresh(accounts);
  return {
    added: result.added,
    removed: result.removed,
    currentIds: result.currentIds,
  };
}

module.exports = { execute };
