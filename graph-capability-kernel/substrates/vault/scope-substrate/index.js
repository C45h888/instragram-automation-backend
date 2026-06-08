// graph-capability-kernel/substrates/vault/scope-substrate/index.js
// Scope substrate façade: owns pre-flight (cache check via FSM-governed read),
// worker factory, post-flight (cache write via CK-governed write), signal dispatch.
//
// Cache reads flow: CAPABILITY_DATA_REQUEST → graph-capability FSM → DB_READ_REQUESTED
// → persist-telemetry → read-scope-cache-worker → READ_RESULT_AVAILABLE → FSM resolves Promise.

const crypto = require('crypto');
const DetectDynamicWorker = require('./workers/detect-dynamic-worker');
const signalDispatch = require('../signal-dispatch');
const fsm = require('../../../fsm');

/**
 * Fire a governed read through the graph-capability FSM.
 * The FSM tracks the request, routes to persist-telemetry, and resolves
 * the Promise when READ_RESULT_AVAILABLE arrives.
 */
function _governedRead(ck, businessAccountId, readDomain, params) {
  const readId = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const dispatchResult = ck.dispatch({
      type: 'CAPABILITY_DATA_REQUEST',
      businessAccountId,
      readDomain,
      readId,
      params,
      source: 'scope-substrate',
      _resolve: resolve,
      _reject: reject,
    });
    if (!dispatchResult.allowed) {
      resolve({ success: false, data: null, error: dispatchResult.reason });
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

  const ck = signalDispatch.getCk();

  // ── Pre-flight: governed cache read through FSM ───────────────────────────
  if (ck && credentialId && businessAccountId) {
    try {
      const result = await _governedRead(ck, businessAccountId, 'db.scope-cache', { credentialId });
      if (result.success && result.data?.scope_cache) {
        const cacheAge = result.data.scope_cache_updated_at
          ? Date.now() - new Date(result.data.scope_cache_updated_at).getTime()
          : Infinity;
        if (cacheAge < 7 * 24 * 60 * 60 * 1000) {
          console.log('✅ Using cached scope via governed read (age: ' + Math.floor(cacheAge / 1000 / 60 / 60) + 'h)');
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

  if (result !== null && ck && typeof ck.dispatch === 'function' && credentialId) {
    ck.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'graph-capability',
      accountId: businessAccountId,
      table: 'instagram_credentials',
      operation: 'write_scope_cache',
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
