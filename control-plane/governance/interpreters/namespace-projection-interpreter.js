// control-plane/governance/interpreters/namespace-projection-interpreter.js
// Namespace Projection Interpreter: CK-dispatched domain state projection writer.
//
// Owns: computing domain state projections from accepted ledger entries,
//        writing projection state to domain namespace Redis keys,
//        replay-deterministic synthesis (pure function of lineage entries).
//
// Does NOT own: constitutional validation, governance decisions,
//               ledger persistence, semantic interpretation of raw telemetry.
//
// Architectural identity:
//   This interpreter absorbs the lineage worker's Layer B (_projections).
//   It is dispatched by CK ONLY for ACCEPTED entries — never for PENDING or REJECTED.
//   It is a PURE FUNCTION of immutable ledger entries — replay-deterministic.
//
//   CK triggers: CK.dispatchGlobal({ type: 'INTERPRET_PROJECTION', ledgerId, entry })
//   → subscribeAction('INTERPRET_PROJECTION', handler)
//   → compute domain projection → write to lineage:projection:{domain}

const lineageLedger = require('../lineage-ledger');

// ═══════════════════════════════════════════════════════════════════════════════
// Domain projection state (mirrors lineage-worker.js Layer B structure)
// ═══════════════════════════════════════════════════════════════════════════════

const _projections = {
  // Projection: domain-scoped (acquisition, publishing, scheduling, dedup,
  // reconciliation, capability)
  domain: {
    acquisition: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    publishing: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    scheduling: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0, cadenceContinuity: 1.0 },
    dedup: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    reconciliation: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    capability: { state: 'UNKNOWN', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
  },
  governanceRuntime: {
    runtimeState: 'BOOTING',
    degradationSignals: {},
    replayContinuity: 'intact',
    domainInstability: 0,
    epochCount: 0,
    lastStateTransition: null,
  },
  health: {
    executionHealth: 'STABLE',
    transitionCount: 0,
    lastTransition: null,
    authorityStability: 1.0,
  },
  authority: {
    acquisition: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
    publishing: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
    scheduling: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
  },
  integrity: {
    structuralAnomalyCount: 0,
    replayAnomalyProbability: 0,
    cadenceGapProbability: 0,
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// Projection computation — pure function of ledger entries
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Compute domain state projections from an accepted ledger entry.
 * Pure function — same entry always produces same projection delta.
 *
 * @param {string} domain — projection namespace (integrity, authority, runtime, health, systemic)
 * @param {object} entry — accepted ledger entry
 * @returns {object} updated projection state
 */
function _computeDomainProjection(domain, entry) {
  const raw = entry.raw || {};
  const payload = raw.projectionPayload || {};

  switch (domain) {
    case 'integrity':
      if (payload.structuralAnomalyCount !== undefined) {
        _projections.integrity.structuralAnomalyCount += payload.structuralAnomalyCount;
      }
      if (payload.replayAnomalyProbability !== undefined) {
        _projections.integrity.replayAnomalyProbability = Math.max(
          _projections.integrity.replayAnomalyProbability,
          payload.replayAnomalyProbability,
        );
      }
      if (payload.cadenceGapProbability !== undefined) {
        _projections.integrity.cadenceGapProbability = Math.max(
          _projections.integrity.cadenceGapProbability,
          payload.cadenceGapProbability,
        );
      }
      break;

    case 'authority':
      // Authority projections track cross-domain authority stability
      for (const [subdomain, subdata] of Object.entries(payload)) {
        if (_projections.authority[subdomain]) {
          _projections.authority[subdomain].authorityCount++;
          _projections.authority[subdomain].lastAuthority = entry.authority;
        }
      }
      break;

    case 'runtime':
      for (const [runtimeDomain, runtimeData] of Object.entries(payload)) {
        if (_projections.domain[runtimeDomain]) {
          _projections.domain[runtimeDomain].transitionCount++;
          _projections.domain[runtimeDomain].lastTransition = entry.timestamp;
        }
      }
      break;

    case 'health':
      if (payload.executionHealth) {
        _projections.health.executionHealth = payload.executionHealth;
      }
      if (payload.failureRate !== undefined) {
        _projections.health.transitionCount++;
        _projections.health.lastTransition = entry.timestamp;
      }
      break;

    case 'systemic':
      if (payload.governancePressure !== undefined) {
        _projections.governanceRuntime.runtimeState = 'DEGRADED';
      }
      if (payload.domainInstability !== undefined) {
        _projections.governanceRuntime.domainInstability = Math.max(
          _projections.governanceRuntime.domainInstability,
          payload.domainInstability,
        );
      }
      break;

    case 'capability':
      // Capability projections track the graph-capability FSM state.
      if (payload.currentCapabilityState) {
        _projections.domain.capability.state = payload.currentCapabilityState;
      }
      if (payload.capabilityAuthorityStability !== undefined) {
        _projections.domain.capability.authorityStability = Math.max(
          0,
          Math.min(1, payload.capabilityAuthorityStability),
        );
      }
      _projections.domain.capability.transitionCount++;
      _projections.domain.capability.lastTransition = entry.timestamp || Date.now();
      break;

    default:
      break;
  }

  return _projections;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Interpreter handler — dispatched by CK on PROJECTION_ACCEPTED
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Consume an accepted projection action and update domain state projections.
 * Subscribed via CK.subscribeAction('PROJECTION_ACCEPTED', ...).
 *
 * @param {object} action — { type: 'PROJECTION_ACCEPTED', ledgerId, entry }
 */
function interpret(action) {
  if (!action || !action.entry) return;

  const entry = action.entry;
  const raw = entry.raw || {};
  const domain = raw.projectionNamespace || entry.domain;

  if (!domain || domain === 'unknown') return;

  // Compute domain projection (pure function)
  _computeDomainProjection(domain, entry);

  // Persist projection snapshot to Redis
  lineageLedger.persistWorkerProjection({
    updatedAt: Date.now(),
    entryLedgerId: action.ledgerId,
    projections: _projections,
  }).catch(err => {
    console.error('[namespace-projection-interpreter] Persist error:', err.message);
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════════

function getProjections() {
  return JSON.parse(JSON.stringify(_projections));
}

function getDomainProjection(domainName) {
  return _projections.domain[domainName]
    ? JSON.parse(JSON.stringify(_projections.domain[domainName]))
    : null;
}

module.exports = {
  interpret,
  getProjections,
  getDomainProjection,
};
