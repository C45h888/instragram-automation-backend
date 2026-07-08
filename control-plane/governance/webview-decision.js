// control-plane/governance/webview-decision.js
// Decision computation for the WebView reactive membrane (Pass 7 / S5).
//
// Pure deterministic function. The decision surface is EXACTLY three
// literal values: 'ACCEPTED' | 'REJECTED' | 'TIMEOUT'. No PENDING,
// UNCONFIRMED, null, or empty strings. V34 enforces this closure.
//
// Inputs:
//   - dispatchResult — { allowed: boolean, reason?: string } | undefined
//
// The CK dispatch can be (in practice):
//   1. resolved with { allowed: true, ... }              → ACCEPTED
//   2. resolved with { allowed: false, reason: '...' }   → REJECTED
//   3. resolved with an unexpected shape (no `allowed`)
//                                                          → REJECTED (fail-closed)
//   4. throws                                          → TIMEOUT-style,
//                                                          but mapped to
//                                                          REJECTED because
//                                                          the FSM contract
//                                                          is fail-closed
//   5. timed out / never resolved (caller passes null)
//                                                          → TIMEOUT
//
// Per spec invariant I34, no other decision value may be returned.

'use strict';

// The closed decision surface. Frozen; runtime check + V34 grep gate.
const DECISIONS = Object.freeze({
  ACCEPTED: 'ACCEPTED',
  REJECTED: 'REJECTED',
  TIMEOUT: 'TIMEOUT',
});
const DECISION_SET = new Set(Object.values(DECISIONS));

/**
 * computeDecision — pure, exhaustive.
 *
 * @param {object|null|undefined} dispatchResult
 *   The result of webview-fsm.dispatch() + the FSM guard evaluation.
 *   Shape: { allowed: boolean, reason?: string, ... }
 *
 * @returns {{ decision: 'ACCEPTED'|'REJECTED'|'TIMEOUT',
 *             reason?: string,
 *             ruleFingerprint?: string }}
 *   The decision plus the reason (for REJECTED) and the rule
 *   fingerprint (if the dispatchResult carries it). The
 *   ruleFingerprint is forwarded so the receipt stream can be
 *   correlated with the FSM decision trail.
 */
function computeDecision(dispatchResult) {
  // TIMEOUT case: caller observed no completion within the deadline.
  // The pump wraps each dispatch in a Promise.race against a
  // timeout; on timeout, it calls computeDecision(null) explicitly.
  if (dispatchResult === null || dispatchResult === undefined) {
    return {
      decision: DECISIONS.TIMEOUT,
      reason: 'webview-fsm.dispatch did not resolve within timeout window',
    };
  }

  // Fail-closed: any non-conforming shape becomes REJECTED.
  if (typeof dispatchResult !== 'object') {
    return {
      decision: DECISIONS.REJECTED,
      reason: 'webview-fsm.dispatch returned non-object result',
    };
  }

  if (dispatchResult.allowed === true) {
    const out = { decision: DECISIONS.ACCEPTED };
    if (typeof dispatchResult.ruleFingerprint === 'string') {
      out.ruleFingerprint = dispatchResult.ruleFingerprint;
    }
    if (typeof dispatchResult.lineageId === 'string') {
      out.lineageId = dispatchResult.lineageId;
    }
    return out;
  }

  // allowed: false (or anything other than true)
  const reason = (typeof dispatchResult.reason === 'string'
                  && dispatchResult.reason.length > 0)
    ? dispatchResult.reason
    : 'webview-fsm.dispatch denied without explicit reason';
  return {
    decision: DECISIONS.REJECTED,
    reason,
  };
}

module.exports = {
  computeDecision,
  DECISIONS,
  DECISION_SET,
};
