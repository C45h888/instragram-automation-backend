// graph-capability-kernel/substrates/vault/signal-dispatch.js
// Centralized signal-dispatch adapter for vault + health substrates.
//
// Constitutional role:
//   Substrate façades (pat, uat, scope, health) emit trigger events when a
//   worker call succeeds. This module is the boundary between substrate
//   emissions and the constitutional ingress.
//
//   Constitutional order:
//     worker → substrate → signal-dispatch → FSM.dispatch → FSM interprets
//       → FSM may ctx.dispatchGlobal → CK for cross-domain
//
//   The substrate does NOT route through the CK directly. The FSM is the
//   constitutional ingress for observation events. The CK is downstream
//   of the FSM in the observation direction (for cross-domain side effects,
//   lineage recording, and the action fabric).
//
//   The action fabric is CK-owned (CK.subscribeAction is the implementation).
//   The substrate subscribes to RUN_* actions via the CK — this is a
//   top-down direction (CK → substrate) for governance actions, distinct
//   from the bottom-up signal flow (substrate → FSM → CK for cross-domain).
//
// Single source of truth: every substrate façade uses this module. No
// inline dispatch wrappers in substrate façades.

// ── FSM binding (replaces bindCk) ──────────────────────────────────────────

let _fsm = null;
let _ctx = null;
let _warnedNoFsm = false;

/**
 * Bind the FSM and dispatch context. Called once at install time.
 * Idempotent: re-binding the same fsm is a no-op. Re-binding a different
 * fsm replaces the reference.
 *
 * @param {object} fsm — the graph-capability FSM (must have .dispatch)
 * @param {object} ctx — dispatch context { validate, dispatchGlobal,
 *                       getGlobalState, sanityCheck }
 */
function bindFsm(fsm, ctx) {
  _fsm = fsm;
  _ctx = ctx || null;
  _warnedNoFsm = false;
}

function getFsm() {
  return _fsm;
}

function getCtx() {
  return _ctx;
}

/**
 * Resolve the dispatch target. Priority order:
 *   1. Explicit `fsm` param to the emit* call
 *   2. Module-level _fsm (set by bindFsm at install time)
 *   3. null (fallback — emit will warn-once and return undefined)
 *
 * Never throws. The constitutional ingress swallows missing-fsm as
 * a soft failure (warning + drop), not a hard failure.
 */
function _resolveFsm(explicitFsm) {
  if (explicitFsm && typeof explicitFsm.dispatch === 'function') return explicitFsm;
  if (_fsm && typeof _fsm.dispatch === 'function') return _fsm;
  return null;
}

// Backwards-compat: many existing callers still call bindCk/getCk. The
// functions are kept as no-ops so importing them does not crash — but
// they no longer wire a CK. The canonical binding is bindFsm. Tests that
// need the old behaviour should be updated to bindFsm.
function bindCk(ck) {
  // No-op. Retained for import-compatibility. Use bindFsm instead.
  if (ck) {
    _warnedNoFsm = true; // we won't process signals routed through CK
  }
}

function getCk() {
  return null;
}

// ── Emit functions — all route through fsm.dispatch() directly ─────────────

/**
 * Emit a CAPABILITY_EVALUATE trigger. Used by every successful substrate
 * worker call to inform the FSM that vault state has changed.
 *
 * @param {{ fsm?: object, ctx?: object, businessAccountId?: string|null, userId?: string|null, source: string }} params
 * @returns {any} the dispatch result, or undefined if no FSM is available
 */
function emitEvaluate({ fsm, ctx, businessAccountId, userId, source }) {
  const resolvedFsm = _resolveFsm(fsm);
  if (!resolvedFsm) {
    if (!_warnedNoFsm) {
      console.warn('[signal-dispatch] emitEvaluate called without a bound FSM — signals will be dropped. Call signalDispatch.bindFsm(fsm, ctx) at install time.');
      _warnedNoFsm = true;
    }
    return undefined;
  }
  const resolvedCtx = ctx || _ctx;
  try {
    return resolvedFsm.dispatch({
      type: 'CAPABILITY_EVALUATE',
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source,
    }, resolvedCtx);
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitEvaluate failed:', err.message);
    return undefined;
  }
}

/**
 * Emit a NEW_ACCOUNT_CONNECTED trigger. Used by vault.pat.store on success.
 */
function emitNewAccountConnected({ fsm, ctx, businessAccountId, userId }) {
  const resolvedFsm = _resolveFsm(fsm);
  if (!resolvedFsm) {
    if (!_warnedNoFsm) {
      console.warn('[signal-dispatch] emitNewAccountConnected called without a bound FSM — signals will be dropped. Call signalDispatch.bindFsm(fsm, ctx) at install time.');
      _warnedNoFsm = true;
    }
    return undefined;
  }
  const resolvedCtx = ctx || _ctx;
  try {
    return resolvedFsm.dispatch({
      type: 'NEW_ACCOUNT_CONNECTED',
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source: 'oauth_callback',
    }, resolvedCtx);
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitNewAccountConnected failed:', err.message);
    return undefined;
  }
}

/**
 * Emit a TOKEN_REFRESHED trigger. Used by vault.uat.refresh on success.
 */
function emitTokenRefreshed({ fsm, ctx, businessAccountId, userId }) {
  const resolvedFsm = _resolveFsm(fsm);
  if (!resolvedFsm) {
    if (!_warnedNoFsm) {
      console.warn('[signal-dispatch] emitTokenRefreshed called without a bound FSM — signals will be dropped. Call signalDispatch.bindFsm(fsm, ctx) at install time.');
      _warnedNoFsm = true;
    }
    return undefined;
  }
  const resolvedCtx = ctx || _ctx;
  try {
    return resolvedFsm.dispatch({
      type: 'TOKEN_REFRESHED',
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source: 'uat_refresh',
    }, resolvedCtx);
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitTokenRefreshed failed:', err.message);
    return undefined;
  }
}

/**
 * Layer 2: Emit a CAPABILITY_OBSERVATION event. The envelope is the canonical
 * worker observation shape produced by fsm.newEnvelope() and populated
 * by substrate façades. Routes through fsm.dispatch(CAPABILITY_OBSERVATION).
 * The FSM merges evidence, infers state, and may ctx.dispatchGlobal for
 * cross-domain work (e.g. lineage).
 */
function emitEnvelope({ fsm, ctx, envelope }) {
  if (!envelope || typeof envelope !== 'object') {
    console.warn('[signal-dispatch] emitEnvelope called without an envelope — skipped');
    return undefined;
  }
  const resolvedFsm = _resolveFsm(fsm);
  if (!resolvedFsm) {
    if (!_warnedNoFsm) {
      console.warn('[signal-dispatch] emitEnvelope called without a bound FSM — observation will be dropped. Call signalDispatch.bindFsm(fsm, ctx) at install time.');
      _warnedNoFsm = true;
    }
    return undefined;
  }
  const resolvedCtx = ctx || _ctx;
  try {
    return resolvedFsm.dispatch({
      type: 'CAPABILITY_OBSERVATION',
      envelope,
      businessAccountId: envelope.businessAccountId || null,
      userId: envelope.userId || null,
      source: 'vault.observation',
    }, resolvedCtx);
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitEnvelope failed:', err.message);
    return undefined;
  }
}

module.exports = {
  // Canonical binding (FSM is the constitutional ingress)
  bindFsm,
  getFsm,
  getCtx,
  // Existing surface (emit semantics)
  emitEvaluate,
  emitNewAccountConnected,
  emitTokenRefreshed,
  emitEnvelope,
  // Legacy CK binding (no-op retained for import-compatibility)
  bindCk,
  getCk,
};
