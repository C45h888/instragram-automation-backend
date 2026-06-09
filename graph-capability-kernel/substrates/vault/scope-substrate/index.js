// graph-capability-kernel/substrates/vault/scope-substrate/index.js
// Scope substrate façade: owns pre-flight (cache check via FSM-governed read),
// worker factory, post-flight (cache write via CK-governed write), signal dispatch.
//
// Constitutional order (Phase D, signal-dispatch rewired):
//   substrate → fsm.dispatch(CAPABILITY_DATA_REQUEST) → fsm internals
//     → ctx.dispatchGlobal(DB_READ_REQUESTED) → CK → persist-telemetry
//     → read-scope-cache-worker → READ_RESULT_AVAILABLE
//     → CK → ctx.dispatchGlobal(READ_RESULT_AVAILABLE) → fsm → resolves Promise.
//
// The substrate never dispatches to the CK directly for reads. The FSM is the
// constitutional ingress for CAPABILITY_DATA_REQUEST; cross-domain routing
// happens via ctx.dispatchGlobal inside the FSM.

const crypto = require('crypto');
const DetectDynamicWorker = require('./workers/detect-dynamic-worker');
const signalDispatch = require('../signal-dispatch');
const fsm = require('../../../fsm');

/**
 * Fire a governed read through the graph-capability FSM.
 * The FSM tracks the request, routes to persist-telemetry, and resolves
 * the Promise when READ_RESULT_AVAILABLE arrives.
 */
function _governedRead(businessAccountId, readDomain, params) {
  const resolvedFsm = signalDispatch.getFsm();
  const ctx = signalDispatch.getCtx();
  if (!resolvedFsm) {
    return Promise.resolve({ success: false, data: null, error: 'fsm_not_bound' });
  }
  const readId = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const dispatchResult = resolvedFsm.dispatch({
      type: 'CAPABILITY_DATA_REQUEST',
      businessAccountId,
      readDomain,
      readId,
      params,
      source: 'scope-substrate',
      _resolve: resolve,
      _reject: reject,
    }, ctx);
    if (!dispatchResult || !dispatchResult.allowed) {
      resolve({ success: false, data: null, error: (dispatchResult && dispatchResult.reason) || 'fsm_dispatch_failed' });
    }
  });
}

/**
 * Detect live scopes for a token via /debug_token.
 * Cache read governed through FSM. Cache write governed through CK.
 *
 * @param {{ token: string, credentialId?: string|null, businessAccountId?: string, userId?: string }} input
 * @returns {Promise<string[]>}
 */
async function detectDynamic({ businessAccountId, userId, token, credentialId = null }) {
  if (!token) {
    throw new Error('token is required');
  }

  // ── Pre-flight: governed cache read through FSM ───────────────────────────
  if (signalDispatch.getFsm() && credentialId && businessAccountId) {
    try {
      const result = await _governedRead(businessAccountId, 'db.scope-cache', { credentialId });
      if (result.success && result.data?.scope_cache) {
        const cacheAge = result.data.scope_cache_updated_at
          ? Date.now() - new Date(result.data.scope_cache_updated_at).getTime()
          : Infinity;
        if (cacheAge < 7 * 24 * 60 * 60 * 1000) {
          console.log('✅ Using cached scope via governed read (age: ' + Math.floor(cacheAge / 1000 / 60 / 60) + 'h)');
          // T2: emit envelope so FSM records the cached observation with real cacheAgeMs
          if (businessAccountId) {
            const cachedEnv = fsm.newEnvelope({ businessAccountId, userId });
            cachedEnv.scope = { grantedScopes: result.data.scope_cache, cacheAgeMs: cacheAge };
            signalDispatch.emitEnvelope({ envelope: cachedEnv });
          }
          return result.data.scope_cache;
        }
      }
    } catch (_) { /* governed read failed or timed out — proceed to live call */ }
  }

  // ── Worker: bounded /debug_token call ─────────────────────────────────────
  const worker = new DetectDynamicWorker();
  const result = await worker.execute({ token });

  // ── Post-flight: fallback + governed cache write ──────────────────────────
  const scopes = (result !== null && Array.isArray(result))
    ? result
    : fsm.PAT_SCOPE_DEFAULTS;

  // Cache write routed through the FSM's requestDBWrite (constitutional ingress
  // for DB writes). The FSM forwards to persist-telemetry via ctx.dispatchGlobal.
  if (result !== null && credentialId) {
    fsm.requestDBWrite({
      table: 'instagram_credentials',
      operation: 'write_scope_cache',
      accountId: businessAccountId,
      rows: [{ credentialId, scopes }],
    });
  }

  // ── Signal dispatch ───────────────────────────────────────────────────────
  signalDispatch.emitEvaluate({
    businessAccountId,
    userId,
    source: 'vault.scope.detectDynamic',
  });

  if (businessAccountId) {
    const envelope = fsm.newEnvelope({ businessAccountId, userId });
    envelope.scope = {
      grantedScopes: scopes,
      cacheAgeMs: 0,
    };
    signalDispatch.emitEnvelope({ envelope });
  }

  return scopes;
}

module.exports = { detectDynamic };
