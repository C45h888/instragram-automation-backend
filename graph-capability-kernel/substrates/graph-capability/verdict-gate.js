// graph-capability-kernel/substrates/graph-capability/verdict-gate.js
// Read-side capability gate. Pure read adapter to the FSM. No state mutation.
// Migrated from substrates/graph-capability/verdict-gate.js
//
// Architecture (post-strengthening):
//   Consumer → verdictGate.requireCapability(baId, requiredScopes)
//     → fsm.getCapabilityVerdict(baId)   ← sole interpreter
//     → verdict-gate applies required-scope diff
//     → returns { allowed, state, reason, missingScopes, observedAt, evidence }
//
// The FSM owns state, evidence, and interpretation. Verdict-gate is the
// pure read adapter that adds scope-difference logic.

const fsm = require('../../fsm');

const FRESH_OBSERVATION_MS = 30 * 60 * 1000; // matches FSM's OBSERVATION_FRESHNESS_MS

/**
 * Read the per-cred capability verdict from the FSM and evaluate against
 * required scopes.
 *
 * @param {string} userId — kept for API compat (not used in evaluation)
 * @param {string} businessAccountId
 * @param {string[]} requiredScopes — scopes the operation depends on
 * @returns {{
 *   allowed: boolean,
 *   state: string,
 *   reason: string|null,
 *   missingScopes: string[],
 *   observedAt: number|null,
 *   evidence: object|null
 * }}
 */
function requireCapability(userId, businessAccountId, requiredScopes = []) {
  const verdict = fsm.getCapabilityVerdict(businessAccountId);
  const { state, observedAt, evidence, missingScopes: fsmMissingScopes } = verdict;

  // FSM has already computed missingScopes for LIMITED state. For other
  // states (AUTHORIZED, UNAUTHORIZED, DEGRADED, PENDING, UNKNOWN), we
  // compute it from the observed scope slot.
  let grantedScopes = [];
  if (evidence && evidence.scope) {
    grantedScopes = evidence.scope.grantedScopes || [];
  }
  let missingScopes;
  if (state === 'LIMITED') {
    missingScopes = fsmMissingScopes || [];
  } else if (Array.isArray(requiredScopes) && requiredScopes.length > 0) {
    missingScopes = requiredScopes.filter(s => !grantedScopes.includes(s));
  } else {
    missingScopes = [];
  }

  const isFresh = observedAt
    ? (Date.now() - observedAt) < FRESH_OBSERVATION_MS
    : false;

  switch (state) {
    case 'AUTHORIZED':
      if (missingScopes.length > 0) {
        return {
          allowed: false,
          state,
          reason: `Required scopes not in capability evidence: ${missingScopes.join(', ')}`,
          missingScopes,
          observedAt,
          evidence,
        };
      }
      if (!isFresh) {
        return {
          allowed: false,
          state,
          reason: 'Observation stale — re-evaluation required',
          missingScopes: [],
          observedAt,
          evidence,
        };
      }
      return {
        allowed: true,
        state,
        reason: null,
        missingScopes: [],
        observedAt,
        evidence,
      };

    case 'LIMITED':
      if (missingScopes.length > 0) {
        return {
          allowed: false,
          state,
          reason: `Required scopes missing: ${missingScopes.join(', ')}`,
          missingScopes,
          observedAt,
          evidence,
        };
      }
      return {
        allowed: true,
        state,
        reason: 'Partial capability — required scopes present',
        missingScopes: [],
        observedAt,
        evidence,
      };

    case 'DEGRADED':
      if (missingScopes.length > 0) {
        return {
          allowed: false,
          state,
          reason: `Degraded AND required scopes missing: ${missingScopes.join(', ')}`,
          missingScopes,
          observedAt,
          evidence,
        };
      }
      return {
        allowed: true,
        state,
        reason: 'Degraded mode — reliability impaired',
        missingScopes: [],
        observedAt,
        evidence,
      };

    case 'UNAUTHORIZED':
      return {
        allowed: false,
        state,
        reason: 'Capability denied — required capability unavailable',
        missingScopes,
        observedAt,
        evidence,
      };

    case 'PAT_PENDING':
    case 'UAT_PENDING':
    case 'DETECTION_PENDING':
    case 'SCOPE_PENDING':
      return {
        allowed: false,
        state,
        reason: `Capability not yet fully observed: ${state} (awaiting ${state.replace('_PENDING', '').toLowerCase()} slot)`,
        missingScopes,
        observedAt,
        evidence,
      };

    case 'UNKNOWN':
    default:
      return {
        allowed: false,
        state: state || 'UNKNOWN',
        reason: 'Capability not yet evaluated',
        missingScopes,
        observedAt,
        evidence,
      };
  }
}

/**
 * Non-throwing read. Returns the raw verdict for a credential.
 * @param {string} [businessAccountId]
 * @returns {{ state: string, observedAt: number|null, evidence: object|null, missingScopes: string[] }}
 */
function peekVerdict(businessAccountId) {
  return fsm.getCapabilityVerdict(businessAccountId);
}

module.exports = {
  requireCapability,
  peekVerdict,
  FRESH_OBSERVATION_MS,
};
