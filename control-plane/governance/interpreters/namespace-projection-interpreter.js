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
//   This interpreter absorbs the transition writers' projection output.
//   It is dispatched by CK ONLY for ACCEPTED entries — never for PENDING or REJECTED.
//   It is a PURE FUNCTION of immutable ledger entries — replay-deterministic.
//
//   CK triggers: CK.dispatchGlobal({ type: 'INTERPRET_PROJECTION', ledgerId, entry })
//   → subscribeAction('INTERPRET_PROJECTION', handler)
//   → compute domain projection → write to lineage:projection:{domain}

const lineageLedger = require('../lineage-ledger');

// ═══════════════════════════════════════════════════════════════════════════════
// Domain projection state (mirrors transition writers' Layer B output structure)
// ═══════════════════════════════════════════════════════════════════════════════

const _projections = {
  // Projection: domain-scoped (acquisition, publishing, scheduling, dedup,
  // reconciliation, capability)
  domain: {
    acquisition: { state: 'IDLE', transitionCount: 0, lastTransition: null, intentCount: 0, failureCount: 0 },
    publishing: { state: 'IDLE', transitionCount: 0, lastTransition: null, publicationCount: 0, failureCount: 0 },
    scheduling: { state: 'IDLE', transitionCount: 0, lastTransition: null, cadenceContinuity: 1.0, accountCount: 0 },
    dedup: { state: 'IDLE', transitionCount: 0, lastTransition: null, batchMarks: 0, batchReplays: 0, collisionCount: 0 },
    reconciliation: { state: 'IDLE', transitionCount: 0, lastTransition: null, epochCount: 0, driftedEpochCount: 0, escalationSignaled: false, driftRate: 0 },
    capability: { state: 'UNKNOWN', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    'persist-telemetry': { state: 'UNKNOWN', transitionCount: 0, lastTransition: null, failureCount: 0, writeCount: 0, readCount: 0, failureRate: 0 },
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
      if (payload.currentCapabilityState) {
        _projections.domain.capability.state = payload.currentCapabilityState;
      }
      if (payload.capabilityAuthorityStability !== undefined) {
        _projections.domain.capability.authorityStability = Math.max(
          _projections.domain.capability.authorityStability,
          payload.capabilityAuthorityStability,
        );
      }
      if (payload.projectionId || payload.timestamp) {
        _projections.domain.capability.transitionCount++;
        _projections.domain.capability.lastTransition = payload.timestamp || Date.now();
      }
      break;

    case 'persist-telemetry':
      if (payload.currentPersistTelemetryState) {
        _projections.domain['persist-telemetry'].state = payload.currentPersistTelemetryState;
      }
      if (payload.failureCount !== undefined) {
        _projections.domain['persist-telemetry'].failureCount =
          (_projections.domain['persist-telemetry'].failureCount || 0) + payload.failureCount;
      }
      if (payload.writeCount !== undefined) {
        _projections.domain['persist-telemetry'].writeCount =
          (_projections.domain['persist-telemetry'].writeCount || 0) + payload.writeCount;
      }
      if (payload.readCount !== undefined) {
        _projections.domain['persist-telemetry'].readCount =
          (_projections.domain['persist-telemetry'].readCount || 0) + payload.readCount;
      }
      if (payload.failureRate !== undefined) {
        _projections.domain['persist-telemetry'].failureRate = payload.failureRate;
      }
      if (payload.projectionId || payload.timestamp) {
        _projections.domain['persist-telemetry'].transitionCount++;
        _projections.domain['persist-telemetry'].lastTransition = payload.timestamp || Date.now();
      }
      break;

    case 'reconciliation':
      if (payload.currentReconciliationState) {
        _projections.domain.reconciliation.state = payload.currentReconciliationState;
      }
      if (payload.epochCount !== undefined) {
        _projections.domain.reconciliation.epochCount = payload.epochCount;
      }
      if (payload.driftedEpochCount !== undefined) {
        _projections.domain.reconciliation.driftedEpochCount = payload.driftedEpochCount;
      }
      if (payload.escalationSignaled !== undefined) {
        _projections.domain.reconciliation.escalationSignaled = payload.escalationSignaled;
      }
      if (payload.driftRate !== undefined) {
        _projections.domain.reconciliation.driftRate = payload.driftRate;
      }
      if (payload.projectionId || payload.timestamp) {
        _projections.domain.reconciliation.transitionCount++;
        _projections.domain.reconciliation.lastTransition = payload.timestamp || Date.now();
      }
      break;

    case 'scheduling':
      if (payload.currentSchedulingState) {
        _projections.domain.scheduling.state = payload.currentSchedulingState;
      }
      if (payload.cadenceContinuity !== undefined) {
        _projections.domain.scheduling.cadenceContinuity = payload.cadenceContinuity;
      }
      if (payload.accountCount !== undefined) {
        _projections.domain.scheduling.accountCount = payload.accountCount;
      }
      if (payload.projectionId || payload.timestamp) {
        _projections.domain.scheduling.transitionCount++;
        _projections.domain.scheduling.lastTransition = payload.timestamp || Date.now();
      }
      break;

    case 'dedup':
      if (payload.currentDedupState) {
        _projections.domain.dedup.state = payload.currentDedupState;
      }
      if (payload.batchMarks !== undefined) {
        _projections.domain.dedup.batchMarks = payload.batchMarks;
      }
      if (payload.batchReplays !== undefined) {
        _projections.domain.dedup.batchReplays = payload.batchReplays;
      }
      if (payload.collisionCount !== undefined) {
        _projections.domain.dedup.collisionCount = payload.collisionCount;
      }
      if (payload.projectionId || payload.timestamp) {
        _projections.domain.dedup.transitionCount++;
        _projections.domain.dedup.lastTransition = payload.timestamp || Date.now();
      }
      break;

    case 'publishing':
      if (payload.currentPublishingState) {
        _projections.domain.publishing.state = payload.currentPublishingState;
      }
      if (payload.publicationCount !== undefined) {
        _projections.domain.publishing.publicationCount += payload.publicationCount;
      }
      if (payload.failureCount !== undefined) {
        _projections.domain.publishing.failureCount += payload.failureCount;
      }
      if (payload.projectionId || payload.timestamp) {
        _projections.domain.publishing.transitionCount++;
        _projections.domain.publishing.lastTransition = payload.timestamp || Date.now();
      }
      break;

    case 'acquisition':
      if (payload.currentAcquisitionState) {
        _projections.domain.acquisition.state = payload.currentAcquisitionState;
      }
      if (payload.intentCount !== undefined) {
        _projections.domain.acquisition.intentCount += payload.intentCount;
      }
      if (payload.failureCount !== undefined) {
        _projections.domain.acquisition.failureCount += payload.failureCount;
      }
      if (payload.projectionId || payload.timestamp) {
        _projections.domain.acquisition.transitionCount++;
        _projections.domain.acquisition.lastTransition = payload.timestamp || Date.now();
      }
      break;
  }

  return _projections;
}

/**
 * Persist a single domain's projection state to a namespace-specific
 * lineage Redis key at lineage:projection:domain:{domain}. This gives
 * each FSM a bounded authority isolation path to read its own projection
 * state without needing to parse the entire aggregate snapshot.
 *
 * Fire-and-forget — never blocks the interpreter tick.
 */
function _persistDomainProjection(domain, ledgerId) {
  try {
    // eslint-disable-next-line global-require
    const { getRedisClient } = require('../../config/redis');
    const redis = getRedisClient();
    if (!redis || redis.status !== 'ready') return;

    const domainProjection = _projections.domain[domain];
    if (!domainProjection) return;

    const key = `lineage:projection:domain:${domain}`;
    redis.set(key, JSON.stringify({
      domain,
      projection: domainProjection,
      updatedAt: Date.now(),
      entryLedgerId: ledgerId,
    }), 'EX', 60).catch(() => {});
  } catch (_) {
    // Best-effort. Never blocks the interpreter.
  }
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

  // Persist aggregate projection snapshot to Redis (all namespaces)
  const persistPromise = lineageLedger.persistWorkerProjection({
    updatedAt: Date.now(),
    entryLedgerId: action.ledgerId,
    projections: _projections,
  });
  if (persistPromise && typeof persistPromise.catch === 'function') {
    persistPromise.catch(err => {
      console.error('[namespace-projection-interpreter] Persist error:', err.message);
    });
  }

  // Persist namespace-specific projection to lineage domain key so the FSM
  // can read its own projection state from lineage:projection:domain:{domain}.
  // This provides bounded authority isolation — each FSM reads only its own
  // namespace projection, not the entire aggregate.
  _persistDomainProjection(domain, action.ledgerId);
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
