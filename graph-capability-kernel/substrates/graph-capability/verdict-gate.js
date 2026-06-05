// graph-capability-kernel/substrates/graph-capability/verdict-gate.js
// Read-side capability gate. Pure read. No state mutation. No dispatch.
// Migrated from substrates/graph-capability/verdict-gate.js
//
// Architecture:
//   Consumer (route/helper) → verdictGate.requireCapability() → fsm.getCapabilityVerdict() → decision
//   The gate is the ONLY read surface consumers should use.
//   The FSM is the source of truth. The CK is the event ingress. The gate is the read bridge.
//
// Contract:
//   requireCapability(userId, businessAccountId, requiredScopes)
//     → { allowed: boolean, state, reason, missingScopes, observedAt, evidence }
//
// State semantics:
//   AUTHORIZED       → allowed=true (all required capabilities present and fresh)
//   LIMITED          → allowed=true if missingScopes ∩ requiredScopes is empty (partial is OK for non-required)
//                      allowed=false if requiredScopes intersect missing
//   DEGRADED         → allowed=true with warning (reliability impaired but functional)
//   UNAUTHORIZED     → allowed=false
//   UNKNOWN          → allowed=false, reason='capability not yet evaluated' (consumer may trigger evaluate)

const fsm = require('../../fsm');

const FRESH_OBSERVATION_MS = 30 * 60 * 1000; // 30 min — matches FSM's OBSERVATION_FRESHNESS_MS

/**
 * Read the current capability verdict from the FSM and evaluate against required scopes.
 * Pure function. No side effects. No I/O.
 *
 * @param {string} userId
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
  const verdict = fsm.getCapabilityVerdict();
  const { state, observedAt, evidence } = verdict;

  // Extract granted scopes from worker observations (if present)
  const grantedScopes = (evidence && evidence.scope && evidence.scope.grantedScopes) || [];
  const missingScopes = Array.isArray(requiredScopes)
    ? requiredScopes.filter(s => !grantedScopes.includes(s))
    : [];

  // Check observation freshness
  const isFresh = observedAt
    ? (Date.now() - observedAt) < FRESH_OBSERVATION_MS
    : false;

  switch (state) {
    case 'AUTHORIZED':
      if (missingScopes.length > 0) {
        // Verdict says authorized but requested scopes not in evidence — fail closed
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
      // LIMITED is allowed only if missing scopes are NOT in the required set
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
      // Degraded mode: allow but flag warning. Operation proceeds under degradation policy.
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
 * Non-throwing read. Returns the raw verdict without scope diffing.
 * Useful for observability and diagnostics.
 *
 * @returns {{ state: string, observedAt: number|null, evidence: object|null }}
 */
function peekVerdict() {
  return fsm.getCapabilityVerdict();
}

module.exports = {
  requireCapability,
  peekVerdict,
  FRESH_OBSERVATION_MS,
};