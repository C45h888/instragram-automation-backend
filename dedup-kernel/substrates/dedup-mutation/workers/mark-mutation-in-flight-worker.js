// dedup-kernel/substrates/dedup-mutation/workers/mark-mutation-in-flight-worker.js
// Mark Mutation In-Flight Worker: bounded, semantically blind.
//
// Bound to: dedup-kernel/substrates/dedup-mutation/index.js
// One bounded I/O call: substrate.markInFlightMutation()
//
// Authority chain: CK → dedup FSM → ctx.invokeWorker → this worker → substrate

const substrate = require('../index');

/**
 * @param {{ accountId: string, actionType: string, resourceId: string, intentId: string }} params
 */
async function execute(params) {
  const { accountId, actionType, resourceId, intentId } = params;
  await substrate.markInFlightMutation(accountId, actionType, resourceId, { intentId });
}

module.exports = { execute };