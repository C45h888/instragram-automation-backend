// graph-capability-kernel/substrates/capability-check-substrate/workers/credential-capability-worker.js
// Credential Capability Worker: resolves per-account credential state.
//
// Owns: reading _byCred from the graph-capability FSM.
// Does NOT own: DB reads (governedRead), token retrieval (vault), quota state.
//
// Contract: execute(params, governance) → { state, lastObservedAt, freshnessMs, consecutiveFailures }

const fsm = require('../../../fsm');

async function execute(params, governance) {
  const { businessAccountId } = params;

  // Read credential state from FSM's canonical record
  const { state: capabilityState, cred } = fsm.getState(businessAccountId);

  if (!cred) {
    return {
      state: 'UNKNOWN',
      lastObservedAt: null,
      freshnessMs: null,
      consecutiveFailures: 0,
    };
  }

  const lastObservedAt = cred.lastObservedAt || null;
  const freshnessMs = lastObservedAt ? Date.now() - lastObservedAt : null;
  const consecutiveFailures = cred.consecutiveFailures || 0;

  return {
    state: capabilityState || 'UNKNOWN',
    lastObservedAt,
    freshnessMs,
    consecutiveFailures,
  };
}

module.exports = { execute };
