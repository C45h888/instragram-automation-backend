// dedup-kernel/substrates/dedup-mutation/workers/check-emission-dedup-worker.js
// Check Emission Dedup Worker: bounded, semantically blind.
//
// Bound to: dedup-kernel/substrates/dedup-mutation/index.js
// One bounded I/O call: substrate.isInFlightEmission()
//
// Belt-and-suspenders: checks BOTH emission-layer key AND intake key.
// FSM interprets the result. Worker does NOT classify, decide, or track batches.
//
// Authority chain: CK → dedup FSM → ctx.invokeWorker → this worker → substrate

const substrate = require('../index');

/**
 * @param {{ accountId: string, actionType: string, resourceId: string, intentId: string }} params
 * @returns {Promise<{ blocked: boolean, reason: string|null, existingIntentId: string|null }>}
 */
async function execute(params) {
  const { accountId, actionType, resourceId, intentId } = params;
  return substrate.isInFlightEmission(accountId, actionType, resourceId, intentId);
}

module.exports = { execute };