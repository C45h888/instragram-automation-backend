// graph-capability-kernel/substrates/vault/scope-substrate/index.js
// Scope substrate façade: factory-creates workers, owns pre-flight.
// Does NOT do I/O — workers do.
// Migrated from substrates/vault/scope-substrate/index.js
//
// Constitutional wiring:
//   On success, emits a CAPABILITY_EVALUATE trigger via signal-dispatch → trigger-bridge → ck → FSM.

const DetectDynamicWorker = require('./workers/detect-dynamic-worker');
const signalDispatch = require('../signal-dispatch');

/**
 * Detect live scopes for a token via /debug_token with 7-day DB cache.
 * @param {{ token: string, supabase?: object, credentialId?: string|null, triggerBridge?: object, businessAccountId?: string, userId?: string }} input
 * @returns {Promise<string[]>}
 */
async function detectDynamic({ triggerBridge, businessAccountId, userId, token, supabase, credentialId = null }) {
  if (!token) {
    throw new Error('token is required');
  }
  const worker = new DetectDynamicWorker();
  const result = await worker.execute({ token, supabase, credentialId });
  signalDispatch.emitEvaluate({
    triggerBridge,
    businessAccountId,
    userId,
    source: 'vault.scope.detectDynamic',
  });
  return result;
}

module.exports = { detectDynamic };