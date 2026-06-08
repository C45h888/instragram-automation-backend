// dedup-kernel/substrates/dedup-mutation/workers/check-mutation-dedup-worker.js
// Check Mutation Dedup Worker: bounded, semantically blind.
//
// Bound to: dedup-kernel/substrates/dedup-mutation/index.js
// One bounded I/O call: substrate.isInFlightMutation()
//
// Belt-and-suspenders: checks BOTH mutation-layer key AND intake key.
// FSM interprets the result — duplicate vs replay vs proceed.
// Worker does NOT classify, does NOT decide, does NOT track batches.
//
// Authority chain: CK → dedup FSM → ctx.invokeWorker → this worker → substrate

const substrate = require('../index');

/**
 * @param {{ accountId: string, actionType: string, resourceId: string, intentId: string }} params
 * @returns {Promise<{ blocked: boolean, reason: string|null, existingIntentId: string|null }>}
 */
async function execute(params) {
  const { accountId, actionType, resourceId, intentId } = params;
  return substrate.isInFlightMutation(accountId, actionType, resourceId, intentId);
}

module.exports = { execute };