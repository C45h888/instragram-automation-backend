// dedup-kernel/substrates/dedup/workers/mark-in-flight-worker.js
// Mark In-Flight Worker: operationally bounded, semantically blind.
//
// Bound to: substrates/dedup/index.js (canonical Redis owner).
// One bounded I/O call: substrate.markInFlight() — Redis SET NX + SET.
//
// Semantically blind: writes keys, returns void. FSM interprets the
// outcome — counter tracking, replay classification, batch metrics.
// Worker does NOT classify, does NOT decide, does NOT track batches.
//
// Authority chain: CK → dedup FSM → ctx.invokeWorker → this worker → substrate

const substrate = require('../index');

/**
 * Execute one bounded mark-in-flight operation through the canonical substrate.
 *
 * @param {{ accountId: string, actionType: string, resourceId: string, intentId: string }} params
 */
async function execute(params) {
  const { accountId, actionType, resourceId, intentId } = params;
  await substrate.markInFlight(accountId, actionType, resourceId, { intentId });
}

module.exports = { execute };
