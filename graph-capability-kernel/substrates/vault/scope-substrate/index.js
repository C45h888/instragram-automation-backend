// graph-capability-kernel/substrates/vault/scope-substrate/index.js
// Scope substrate façade: owns pre-flight (cache check via governed read),
// worker factory, post-flight (cache write via governed write), signal dispatch.
// Worker is semantically blind — just /debug_token.
//
// DB reads/writes route through CK → persist-telemetry FSM → graph-capability substrate workers.

const DetectDynamicWorker = require('./workers/detect-dynamic-worker');
const signalDispatch = require('../signal-dispatch');
const fsm = require('../../fsm');

/**
 * Detect live scopes for a token via /debug_token.
 * Cache read/write governed through CK → persist-telemetry.
 *
 * @param {{ token: string, credentialId?: string|null, businessAccountId?: string, userId?: string }} input
 * @returns {Promise<string[]>}
 */
async function detectDynamic({ businessAccountId, userId, token, credentialId = null }) {
  if (!token) {
    throw new Error('token is required');
  }

  // ── Pre-flight: governed cache read (CK → persist-telemetry FSM → worker) ─
  const ck = signalDispatch.getCk();
  if (ck && typeof ck.governedRead === 'function' && credentialId) {
    try {
      const result = await ck.governedRead('db.scope-cache', {
        credentialId,
        accountId: businessAccountId,
      });
      if (result.success && result.data?.scope_cache) {
        const cacheAge = result.data.scope_cache_updated_at
          ? Date.now() - new Date(result.data.scope_cache_updated_at).getTime()
          : Infinity;
        if (cacheAge < 7 * 24 * 60 * 60 * 1000) {
          console.log('✅ Using cached scope via governed read (age: ' + Math.floor(cacheAge / 1000 / 60 / 60) + 'h)');
          return result.data.scope_cache;
        }
      }
    } catch (_) { /* governed read failed — proceed to live call */ }
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
      rows: [{
        credentialId,
        scopes,
      }],
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
