// graph-capability-kernel/substrates/graph-capability/observations.js
// Observation envelope normalizer for the Graph Capability substrate.
// Migrated from substrates/graph-capability/observations.js
//
// Workers emit raw observations. The façade aggregates and normalizes them
// into a single canonical capability observation that the FSM consumes.
//
// Canonical states: AUTHORIZED | LIMITED | DEGRADED | UNAUTHORIZED | UNKNOWN

const { REQUIRED_SCOPES } = require('../../fsm');

/**
 * Normalize a set of worker observations into a single canonical observation.
 *
 * @param {object} aggregate — { pat, scope, uat, detection } raw observation envelopes
 * @returns {{
 *   state: 'AUTHORIZED'|'LIMITED'|'DEGRADED'|'UNAUTHORIZED'|'UNKNOWN',
 *   observedAt: number,
 *   evidence: object,
 *   reason: string|null,
 *   missingScopes: string[]
 * }}
 */
function normalize(aggregate) {
  const observedAt = Date.now();
  const evidence = {
    pat: aggregate.pat || null,
    scope: aggregate.scope || null,
    uat: aggregate.uat || null,
    detection: aggregate.detection || null,
  };

  // UNAUTHORIZED: any required worker reports failure
  if (evidence.pat && evidence.pat.isDecryptable === false) {
    return {
      state: 'UNAUTHORIZED',
      observedAt,
      evidence,
      reason: 'PAT not decryptable',
      missingScopes: [],
    };
  }

  if (evidence.uat && evidence.uat.isDecryptable === false) {
    return {
      state: 'UNAUTHORIZED',
      observedAt,
      evidence,
      reason: 'UAT not decryptable',
      missingScopes: [],
    };
  }

  if (evidence.detection && evidence.detection.isValid === false) {
    return {
      state: 'UNAUTHORIZED',
      observedAt,
      evidence,
      reason: evidence.detection.reason || 'Token validation failed',
      missingScopes: [],
    };
  }

  // LIMITED: scope verification reveals missing required scopes
  if (evidence.scope) {
    const granted = evidence.scope.grantedScopes || [];
    const missingScopes = REQUIRED_SCOPES.filter(req => !granted.includes(req));
    if (missingScopes.length > 0) {
      return {
        state: 'LIMITED',
        observedAt,
        evidence,
        reason: `Missing required scopes: ${missingScopes.join(', ')}`,
        missingScopes,
      };
    }
  }

  // DEGRADED: detection returning errors persistently OR observation stale
  if (evidence.detection && evidence.detection.reliabilityImpaired) {
    return {
      state: 'DEGRADED',
      observedAt,
      evidence,
      reason: 'Detection reliability impaired',
      missingScopes: [],
    };
  }

  if (evidence.scope && evidence.scope.cacheAgeMs != null) {
    const STALE_THRESHOLD_MS = 2 * 6 * 60 * 60 * 1000; // 2x SCOPE_RECHECK_INTERVAL_MS
    if (evidence.scope.cacheAgeMs > STALE_THRESHOLD_MS) {
      return {
        state: 'DEGRADED',
        observedAt,
        evidence,
        reason: `Scope cache stale: ${evidence.scope.cacheAgeMs}ms`,
        missingScopes: [],
      };
    }
  }

  // AUTHORIZED: all four workers green
  if (evidence.pat && evidence.scope && evidence.uat && evidence.detection) {
    return {
      state: 'AUTHORIZED',
      observedAt,
      evidence,
      reason: null,
      missingScopes: [],
    };
  }

  // UNKNOWN: observation set is partial — awaiting more workers
  return {
    state: 'UNKNOWN',
    observedAt,
    evidence,
    reason: 'Observation set incomplete',
    missingScopes: [],
  };
}

/**
 * Build an empty observation envelope for a worker that has not yet reported.
 *
 * @returns {object} null-shaped envelope
 */
function empty() {
  return null;
}

module.exports = { normalize, empty };