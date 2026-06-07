// graph-capability-kernel/substrates/vault/scope-substrate/index.js
// Scope substrate façade: factory-creates workers, owns pre-flight.
// Does NOT do I/O — workers do.
// Migrated from substrates/vault/scope-substrate/index.js
//
// Constitutional wiring:
//   On success, emits a CAPABILITY_EVALUATE trigger via signal-dispatch → trigger-bridge → ck → FSM.
//
// Layer 2: on success, also builds a canonical observation envelope and emits
// it via signal-dispatch.emitEnvelope() → ck.dispatch(CAPABILITY_OBSERVATION).

const DetectDynamicWorker = require('./workers/detect-dynamic-worker');
const signalDispatch = require('../signal-dispatch');
const observations = require('../../graph-capability/observations');

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
  // Layer 2: emit envelope with grantedScopes + derived cacheAgeMs.
  if (businessAccountId) {
    const envelope = observations.newEnvelope({ businessAccountId, userId });
    // The worker returns string[] (scopes) and writes a 7-day cache. If the
    // worker hit the cache, scopes were returned from DB and cacheAgeMs is
    // (now - scope_cache_updated_at). If the worker hit Meta, cacheAgeMs is 0.
    // We don't have direct access to the cache timestamp here, but the worker
    // logs whether it hit cache. We approximate cacheAgeMs=0 for fresh,
    // otherwise leave it null and let the FSM treat it as not-stale.
    envelope.scope = {
      grantedScopes: Array.isArray(result) ? result : [],
      cacheAgeMs: null, // unknown at façade level; not stale → no DEGRADED trigger
    };
    signalDispatch.emitEnvelope({ triggerBridge, envelope });
  }
  return result;
}

module.exports = { detectDynamic };
