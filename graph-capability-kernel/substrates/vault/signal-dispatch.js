// graph-capability-kernel/substrates/vault/signal-dispatch.js
// Centralized signal-dispatch adapter for vault façades.
//
// Constitutional role:
//   Vault façades (pat, uat, scope) emit trigger events when a worker call succeeds.
//   This module is the vault's local adapter: it packages success events and
//   routes them through CK.dispatch(). No intermediate trigger-bridge needed —
//   CK IS the event ingress.
//
// Single source of truth: every vault façade uses this module. No inline
// dispatch wrappers in substrate façades.
//
// ── Authority boundary ──────────────────────────────────────────────────────
//   Layer 1 fix: bindCk(ck) is called once at install() time by the
//   graph-capability wiring. The CK reference is stored in a private
//   module-level slot. Every emit* call threads _ck into CK.dispatch().
//
//   This makes signal-dispatch the SINGLE authority boundary for vault
//   signal ingress. There is no path through which a vault success event
//   can reach the FSM without going through the constitutional CK.

// ── CK binding (Layer 1.1) ──────────────────────────────────────────────────

let _ck = null;
let _warnedNoCk = false;/**
 * Bind the constitutional kernel reference. Called once at install() time.
 * Idempotent: re-binding the same ck is a no-op. Re-binding a different ck
 * is allowed (re-install scenario) and replaces the reference.
 *
 * @param {object|null} ck — the constitutional kernel module (must have .dispatch)
 */
function bindCk(ck) {
  _ck = ck;
  _warnedNoCk = false; // reset warning latch on a real binding
}

function getCk() {
  return _ck;
}

/**
 * Resolve the CK reference. Priority order:
 *   1. Explicit `ck` param to the emit* call
 *   2. Module-level _ck (set by bindCk at install time)
 *   3. null (fallback — emit will warn-once and return undefined)
 *
 * Never throws. The constitutional ingress swallows missing-ck as
 * a soft failure (warning + drop), not a hard failure.
 */
function _resolveCk(explicitCk) {
  if (explicitCk && typeof explicitCk.dispatch === 'function') return explicitCk;
  if (_ck && typeof _ck.dispatch === 'function') return _ck;
  return null;
}

// ── Emit functions — all route through CK.dispatch() directly ────────────────

/**
 * Emit a CAPABILITY_EVALUATE trigger. Used by every successful vault worker call
 * to inform the FSM that vault state has changed.
 *
 * @param {{ ck?: object, businessAccountId?: string|null, userId?: string|null, source: string }} params
 * @returns {any} the dispatch result, or undefined if no CK is available
 */
function emitEvaluate({ ck, businessAccountId, userId, source }) {
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitEvaluate called without a bound CK — signals will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  try {
    return resolvedCk.dispatch({
      type: 'CAPABILITY_EVALUATE',
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source,
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitEvaluate failed:', err.message);
    return undefined;
  }
}

/**
 * Emit a NEW_ACCOUNT_CONNECTED trigger. Used by vault.pat.store on success.
 *
 * @param {{ ck?: object, businessAccountId?: string|null, userId?: string|null }} params
 * @returns {any}
 */
function emitNewAccountConnected({ ck, businessAccountId, userId }) {
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitNewAccountConnected called without a bound CK — signals will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  try {
    return resolvedCk.dispatch({
      type: 'NEW_ACCOUNT_CONNECTED',
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source: 'oauth_callback',
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitNewAccountConnected failed:', err.message);
    return undefined;
  }
}

/**
 * Emit a TOKEN_REFRESHED trigger. Used by vault.uat.refresh on success.
 *
 * @param {{ ck?: object, businessAccountId?: string|null, userId?: string|null }} params
 * @returns {any}
 */
function emitTokenRefreshed({ ck, businessAccountId, userId }) {
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitTokenRefreshed called without a bound CK — signals will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  try {
    return resolvedCk.dispatch({
      type: 'TOKEN_REFRESHED',
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source: 'uat_refresh',
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitTokenRefreshed failed:', err.message);
    return undefined;
  }
}

/**
 * Layer 2: Emit a CAPABILITY_OBSERVATION event. The envelope is the canonical
 * worker observation shape produced by fsm.newEnvelope() and populated
 * by substrate façades. Routes through ck.dispatch(CAPABILITY_OBSERVATION) which
 * lands in the FSM via DOMAIN_EVENT_MAP. The FSM's CAPABILITY_OBSERVATION
 * transition consumes the envelope, merges evidence, and infers state.
 *
 * @param {{ ck?: object, envelope: object }} params
 * @returns {any}
 */
function emitEnvelope({ ck, envelope }) {
  if (!envelope || typeof envelope !== 'object') {
    console.warn('[signal-dispatch] emitEnvelope called without an envelope — skipped');
    return undefined;
  }
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitEnvelope called without a bound CK — observation will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  try {
    return resolvedCk.dispatch({
      type: 'CAPABILITY_OBSERVATION',
      envelope,
      businessAccountId: envelope.businessAccountId || null,
      userId: envelope.userId || null,
      source: 'vault.observation',
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitEnvelope failed:', err.message);
    return undefined;
  }
}

module.exports = {
  // Layer 1.1 — authority boundary
  bindCk,
  getCk,
  // Existing surface
  emitEvaluate,
  emitNewAccountConnected,
  emitTokenRefreshed,
  // Layer 2 — observation envelope ingress
  emitEnvelope,
};
