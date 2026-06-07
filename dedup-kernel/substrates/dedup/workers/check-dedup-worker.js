// dedup-kernel/substrates/dedup/workers/check-dedup-worker.js
// Check Dedup Worker: operationally bounded, semantically blind.
//
// Bound to: substrates/dedup/index.js (canonical Redis owner).
// One bounded I/O call: substrate.isInFlight() — Redis EXISTS + GET.
//
// Semantically blind: returns raw { blocked, reason, existingIntentId }.
// FSM interprets the result — duplicate vs replay vs proceed.
// Worker does NOT classify, does NOT decide, does NOT track batches.
//
// Authority chain: CK → dedup FSM → ctx.invokeWorker → this worker → substrate

const substrate = require('../index');

/**
 * Execute one bounded dedup check through the canonical substrate.
 *
 * @param {{ accountId: string, actionType: string, resourceId: string, intentId: string }} params
 * @returns {Promise<{ blocked: boolean, reason: string|null, existingIntentId: string|null }>}
 */
async function execute(params) {
  const { accountId, actionType, resourceId, intentId } = params;
  return substrate.isInFlight(accountId, actionType, resourceId, intentId);
}

module.exports = { execute };
