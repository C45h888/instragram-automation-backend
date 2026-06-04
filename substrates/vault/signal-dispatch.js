// substrates/vault/signal-dispatch.js
// Centralized signal-dispatch adapter for vault façades.
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

const triggerBridge = require('../graph-capability/trigger-bridge');

/**
 * Emit a CAPABILITY_EVALUATE trigger. Used by every successful vault worker call
 * to inform the FSM that vault state has changed.
 *
 * @param {{ triggerBridge?: object, businessAccountId?: string|null, userId?: string|null, source: string }} params
 * @returns {any} the dispatch result, or undefined if no bridge was provided
 */
function emitEvaluate({ triggerBridge: bridge, businessAccountId, userId, source }) {
  const tb = bridge || triggerBridge;
  if (!tb || typeof tb.emitCapabilityEvaluate !== 'function') return undefined;
  try {
    return tb.emitCapabilityEvaluate({
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
 * @param {{ triggerBridge?: object, businessAccountId?: string|null, userId?: string|null }} params
 * @returns {any}
 */
function emitNewAccountConnected({ triggerBridge: bridge, businessAccountId, userId }) {
  const tb = bridge || triggerBridge;
  if (!tb || typeof tb.emitNewAccountConnected !== 'function') return undefined;
  try {
    return tb.emitNewAccountConnected({
      businessAccountId: businessAccountId || null,
      userId: userId || null,
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitNewAccountConnected failed:', err.message);
    return undefined;
  }
}

/**
 * Emit a TOKEN_REFRESHED trigger. Used by vault.uat.refresh on success.
 *
 * @param {{ triggerBridge?: object, businessAccountId?: string|null, userId?: string|null }} params
 * @returns {any}
 */
function emitTokenRefreshed({ triggerBridge: bridge, businessAccountId, userId }) {
  const tb = bridge || triggerBridge;
  if (!tb || typeof tb.emitTokenRefreshed !== 'function') return undefined;
  try {
    return tb.emitTokenRefreshed({
      businessAccountId: businessAccountId || null,
      userId: userId || null,
    });
  } catch (err) {
    console.warn('⚠️ signal-dispatch emitTokenRefreshed failed:', err.message);
    return undefined;
  }
}

module.exports = {
  emitEvaluate,
  emitNewAccountConnected,
  emitTokenRefreshed,
};
