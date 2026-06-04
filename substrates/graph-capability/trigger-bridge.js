// substrates/graph-capability/trigger-bridge.js
// Event ingress bridge. Thin wrapper that converts domain events into CK dispatches.
//
// Architecture:
//   Trigger source (token-health, OAuth callback, worker) → triggerBridge.emit*() → ck.dispatch() → CK routes → FSM
//   The bridge does NOT import the FSM directly. It only knows about CK.
//   The CK is the only event ingress. The FSM is the only state writer.
//
// Contract:
//   All emit* functions take a CK reference and call ck.dispatch({type, ...}).
//   They return the dispatch result. They never mutate state directly.
//
// Events emitted (all route to 'graph-capability' domain via CK DOMAIN_EVENT_MAP):
//   CAPABILITY_EVALUATE      — generic trigger: start evaluation
//   NEW_ACCOUNT_CONNECTED    — OAuth callback success
//   TOKEN_REFRESHED          — UAT refresh success
//   REPEATED_GRAPH_FAILURE   — multiple /debug_token failures

/**
 * Resolve the constitutional kernel reference. Accepts either:
 *   - the CK module exports object (has .dispatch)
 *   - a wrapper that has .dispatch
 * Throws if CK is missing or lacks .dispatch.
 */
function _resolveCk(ck) {
  if (!ck) {
    throw new Error('[trigger-bridge] CK reference required');
  }
  if (typeof ck.dispatch !== 'function') {
    throw new Error('[trigger-bridge] CK reference must have .dispatch() method');
  }
  return ck;
}

/**
 * Emit a generic capability evaluation trigger.
 * @param {{ businessAccountId: string, userId?: string, source: string, ck: object, reason?: string }} params
 * @returns {{ allowed: boolean, [key: string]: any }}
 */
function emitCapabilityEvaluate({ businessAccountId, userId, source, ck, reason }) {
  const k = _resolveCk(ck);
  return k.dispatch({
    type: 'CAPABILITY_EVALUATE',
    source: source || 'unknown',
    businessAccountId: businessAccountId || null,
    userId: userId || null,
    reason: reason || null,
  });
}

/**
 * Emit a NEW_ACCOUNT_CONNECTED trigger (OAuth callback success path).
 * @param {{ businessAccountId: string, userId: string, ck: object }} params
 * @returns {{ allowed: boolean, [key: string]: any }}
 */
function emitNewAccountConnected({ businessAccountId, userId, ck }) {
  const k = _resolveCk(ck);
  return k.dispatch({
    type: 'NEW_ACCOUNT_CONNECTED',
    businessAccountId: businessAccountId || null,
    userId: userId || null,
    source: 'oauth_callback',
  });
}

/**
 * Emit a TOKEN_REFRESHED trigger (UAT refresh success).
 * @param {{ businessAccountId: string, userId: string, ck: object }} params
 * @returns {{ allowed: boolean, [key: string]: any }}
 */
function emitTokenRefreshed({ businessAccountId, userId, ck }) {
  const k = _resolveCk(ck);
  return k.dispatch({
    type: 'TOKEN_REFRESHED',
    businessAccountId: businessAccountId || null,
    userId: userId || null,
    source: 'uat_refresh',
  });
}

/**
 * Emit a REPEATED_GRAPH_FAILURE trigger (multiple /debug_token failures).
 * @param {{ businessAccountId: string, userId: string, ck: object, failureCount?: number }} params
 * @returns {{ allowed: boolean, [key: string]: any }}
 */
function emitRepeatedGraphFailure({ businessAccountId, userId, ck, failureCount }) {
  const k = _resolveCk(ck);
  return k.dispatch({
    type: 'REPEATED_GRAPH_FAILURE',
    businessAccountId: businessAccountId || null,
    userId: userId || null,
    failureCount: failureCount || null,
    source: 'detection_worker',
  });
}

module.exports = {
  emitCapabilityEvaluate,
  emitNewAccountConnected,
  emitTokenRefreshed,
  emitRepeatedGraphFailure,
};
