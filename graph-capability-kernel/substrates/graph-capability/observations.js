// graph-capability-kernel/substrates/graph-capability/observations.js
// Observation envelope normalizer for the Graph Capability substrate.
// Migrated from substrates/graph-capability/observations.js
//
// ── Layer 2: Canonical Envelope Contract ────────────────────────────────────
// Workers emit raw observations. The façade aggregates and normalizes them
// into a single canonical capability observation that the FSM consumes.
//
// Canonical states: AUTHORIZED | LIMITED | DEGRADED | UNAUTHORIZED | UNKNOWN
//
// ── Canonical Envelope Shape ────────────────────────────────────────────────
// Every worker success / failure path is wrapped in this envelope before
// being passed to signal-dispatch.emitEnvelope().
//
//   {
//     envelopeId:   string,                  // unique id for this envelope
//     observedAt:   number,                  // Date.now() at envelope build
//     businessAccountId: string | null,
//     userId:       string | null,
//
//     pat:          { isDecryptable: boolean, ...patWorkerOutput } | null,
//     uat:          { isDecryptable: boolean, ...uatWorkerOutput } | null,
//     detection:    { isValid: boolean, reliabilityImpaired: boolean,
//                     reason: string | null, ...detectWorkerOutput } | null,
//     scope:        { grantedScopes: string[], cacheAgeMs: number | null,
//                     ...detectDynamicWorkerOutput } | null,
//   }
//
// All four inner keys are optional and nullable. normalize() reads whichever
// are present. Missing keys → that domain is treated as "not observed".
//
// Field mapping (canonical name → worker that produces it):
//   pat.isDecryptable      ← pat-substrate/workers/retrieve-worker.execute()
//   uat.isDecryptable      ← uat-substrate/workers/retrieve-worker.execute()
//   detection.isValid      ← uat-substrate/workers/detect-worker.execute()
//   detection.reliabilityImpaired ← derived (consecutive /debug_token errors)
//   scope.grantedScopes    ← scope-substrate/workers/detect-dynamic-worker.execute()
//   scope.cacheAgeMs       ← derived (Date.now() - scope_cache_updated_at)

// Lazy-load fsm to break a circular dependency (fsm.js ↔ observations.js).
// observations.js needs fsm.REQUIRED_SCOPES; fsm.js's _aggregateAndDispatch
// needs observations.normalize. Both are runtime-resolved, so a lazy require
// (inside the function) avoids the partial-export trap on first require.
let _REQUIRED_SCOPES = null;
function _getRequiredScopes() {
  if (!_REQUIRED_SCOPES) {
    _REQUIRED_SCOPES = require('../../fsm').REQUIRED_SCOPES;
  }
  return _REQUIRED_SCOPES;
}

/**
 * Build an empty envelope (no observations yet).
 * @returns {object}
 */
function emptyEnvelope() {
  return {
    envelopeId: null,
    observedAt: null,
    businessAccountId: null,
    userId: null,
    pat: null,
    uat: null,
    detection: null,
    scope: null,
  };
}

/**
 * Build a fresh envelope with identity fields. All four inner keys are null.
 * @param {{ envelopeId?: string, businessAccountId?: string|null, userId?: string|null }} input
 * @returns {object}
 */
function newEnvelope({ envelopeId, businessAccountId, userId } = {}) {
  return {
    envelopeId: envelopeId || `env-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    observedAt: Date.now(),
    businessAccountId: businessAccountId || null,
    userId: userId || null,
    pat: null,
    uat: null,
    detection: null,
    scope: null,
  };
}

/**
 * Normalize a set of worker observations into a single canonical observation.
 *
 * @param {object} aggregate — envelope with { pat, scope, uat, detection } slots
 * @returns {{
 *   state: 'AUTHORIZED'|'LIMITED'|'DEGRADED'|'UNAUTHORIZED'|'UNKNOWN',
 *   observedAt: number,
 *   evidence: object,
 *   reason: string|null,
 *   missingScopes: string[]
 * }}
 */
function normalize(aggregate) {
  const observedAt = (aggregate && aggregate.observedAt) || Date.now();
  const evidence = {
    pat: aggregate?.pat || null,
    scope: aggregate?.scope || null,
    uat: aggregate?.uat || null,
    detection: aggregate?.detection || null,
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
    const missingScopes = _getRequiredScopes().filter(req => !granted.includes(req));
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

module.exports = {
  // Layer 2: canonical envelope construction
  emptyEnvelope,
  newEnvelope,
  // Existing normalizer (unchanged behaviour)
  normalize,
  empty,
};
