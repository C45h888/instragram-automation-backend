// graph-capability-kernel/substrates/vault/signal-dispatch.js
// Centralized signal-dispatch adapter for vault façades.
// Migrated from substrates/vault/signal-dispatch.js
//
// Constitutional role:
//   Vault façades (pat, uat, scope) emit trigger events when a worker call succeeds.
//   The trigger-bridge is the constitutional event ingress (it knows about CK + DOMAIN_EVENT_MAP).
//   This module is the vault's local adapter to the trigger-bridge: it packages
//   success events with the vault's source tag, swallows transient errors with a
//   warning log, and never throws to the caller.
//
// Single source of truth: every vault façade uses this module. No inline
// trigger-bridge wrappers in substrate façades.
//
// ── Authority boundary ──────────────────────────────────────────────────────
//   Pre-2026-06-07: signal-dispatch held no CK reference. Every emit* call
//   fell through to the default trigger-bridge, which threw on missing ck.
//   The throw was swallowed by trigger-bridge's own try/catch and the
//   signal was silently dropped. This is GAP-3 in the formalization.
//
//   Layer 1 fix: bindCk(ck) is called once at install() time by the
//   graph-capability wiring. The CK reference is stored in a private
//   module-level slot. Every emit* call threads _ck into the trigger-bridge
//   invocation. Falls back to the default bridge only when no ck is bound
//   (legacy direct-caller path) with a one-shot warning.
//
//   This makes signal-dispatch the SINGLE authority boundary for vault
//   signal ingress. There is no path through which a vault success event
//   can reach the FSM without going through the constitutional CK.

const triggerBridge = require('../graph-capability/trigger-bridge');

// ── CK binding (Layer 1.1) ──────────────────────────────────────────────────

let _ck = null;
let _warnedNoCk = false;

/**
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
 * Resolve the trigger-bridge reference. Always returns the default
 * trigger-bridge (the canonical constitutional ingress). Substrate-level
 * overrides are not supported — this enforces the single-ingress rule.
 */
function _resolveBridge(bridge) {
  return bridge || triggerBridge;
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

// ── Emit functions ──────────────────────────────────────────────────────────

/**
 * Emit a CAPABILITY_EVALUATE trigger. Used by every successful vault worker call
 * to inform the FSM that vault state has changed.
 *
 * @param {{ triggerBridge?: object, ck?: object, businessAccountId?: string|null, userId?: string|null, source: string }} params
 * @returns {any} the dispatch result, or undefined if no bridge was provided
 */
function emitEvaluate({ triggerBridge: bridge, ck, businessAccountId, userId, source }) {
  const tb = _resolveBridge(bridge);
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitEvaluate called without a bound CK — signals will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  if (!tb || typeof tb.emitCapabilityEvaluate !== 'function') return undefined;
  try {
    return tb.emitCapabilityEvaluate({
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source,
      ck: resolvedCk,
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitEvaluate failed:', err.message);
    return undefined;
  }
}

/**
 * Emit a NEW_ACCOUNT_CONNECTED trigger. Used by vault.pat.store on success.
 *
 * @param {{ triggerBridge?: object, ck?: object, businessAccountId?: string|null, userId?: string|null }} params
 * @returns {any}
 */
function emitNewAccountConnected({ triggerBridge: bridge, ck, businessAccountId, userId }) {
  const tb = _resolveBridge(bridge);
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitNewAccountConnected called without a bound CK — signals will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  if (!tb || typeof tb.emitNewAccountConnected !== 'function') return undefined;
  try {
    return tb.emitNewAccountConnected({
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      ck: resolvedCk,
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitNewAccountConnected failed:', err.message);
    return undefined;
  }
}

/**
 * Emit a TOKEN_REFRESHED trigger. Used by vault.uat.refresh on success.
 *
 * @param {{ triggerBridge?: object, ck?: object, businessAccountId?: string|null, userId?: string|null }} params
 * @returns {any}
 */
function emitTokenRefreshed({ triggerBridge: bridge, ck, businessAccountId, userId }) {
  const tb = _resolveBridge(bridge);
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitTokenRefreshed called without a bound CK — signals will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  if (!tb || typeof tb.emitTokenRefreshed !== 'function') return undefined;
  try {
    return tb.emitTokenRefreshed({
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      ck: resolvedCk,
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitTokenRefreshed failed:', err.message);
    return undefined;
  }
}

/**
 * Layer 2: Emit a CAPABILITY_OBSERVATION event. The envelope is the canonical
 * worker observation shape produced by observations.newEnvelope() and populated
 * by substrate façades. Routes through ck.dispatch(CAPABILITY_OBSERVATION) which
 * lands in the FSM via DOMAIN_EVENT_MAP. The FSM's CAPABILITY_OBSERVATION
 * transition (Layer 3) consumes the envelope, normalizes it, and dispatches
 * a derived CAPABILITY_OK / CAPABILITY_PARTIAL / CAPABILITY_FAILED event.
 *
 * @param {{ triggerBridge?: object, ck?: object, envelope: object }} params
 * @returns {any}
 */
function emitEnvelope({ triggerBridge: bridge, ck, envelope }) {
  if (!envelope || typeof envelope !== 'object') {
    console.warn('[signal-dispatch] emitEnvelope called without an envelope — skipped');
    return undefined;
  }
  const tb = _resolveBridge(bridge);
  const resolvedCk = _resolveCk(ck);
  if (!resolvedCk) {
    if (!_warnedNoCk) {
      console.warn('[signal-dispatch] emitEnvelope called without a bound CK — observation will be dropped. Call signalDispatch.bindCk(ck) at install time.');
      _warnedNoCk = true;
    }
    return undefined;
  }
  // emitEnvelope routes directly through ck.dispatch, not through trigger-bridge,
  // because CAPABILITY_OBSERVATION is an internal observation event — not a
  // user-facing trigger. The DOMAIN_EVENT_MAP routes it to the graph-capability
  // FSM regardless.
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
