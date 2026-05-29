// ============================================
// Event Injection Framework
// ============================================
// Purpose: Provides deterministic event injection
// into the constitutional runtime for testing.
//
// The framework simulates:
//   - Governance intent injection
//   - Substrate observation injection
//   - Lineage event emission
//   - Reconciliation triggers
//
// Usage:
//   const injector = require('./event-injector');
//   await injector.injectAcquisitionIntent({ variant: 'success' });
// ============================================

const observability = require('../control-plane/observability/index');
const { getRedisClient } = require('../config/redis');

// Load mock substrates
const mockSubstrates = {
  success: require('./mock-substrates/success/media-success.json'),
  malformed: require('./mock-substrates/malformed/media-malformed.json'),
  duplicate: require('./mock-substrates/duplicate/media-duplicate.json'),
  stale: require('./mock-substrates/stale/media-stale.json'),
  rateLimited: require('./mock-substrates/rate-limited/acquisition-rate-limited.json'),
  partial: require('./mock-substrates/partial/media-partial.json'),
};

/**
 * Inject an acquisition intent into the runtime.
 * Simulates the full fetch → normalize → lineage → projection flow.
 *
 * @param {object} options
 * @param {string} options.variant - 'success'|'malformed'|'duplicate'|'stale'|'rateLimited'|'partial'
 * @param {string} [options.accountId] - Override account ID
 * @returns {Promise<object>} Injection result with lineageId
 */
async function injectAcquisitionIntent({ variant = 'success', accountId = 'test-account-001' }) {
  const substrate = mockSubstrates[variant];
  if (!substrate) {
    throw new Error(`Unknown variant: ${variant}. Available: ${Object.keys(mockSubstrates).join(', ')}`);
  }

  const lineageId = `lineage-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  const intentId = `intent-${Date.now()}`;

  // Emit acquisition intent received
  observability.transition({
    domain: 'acquisition',
    entity: 'acquisition_intent',
    entityId: intentId,
    previousState: null,
    nextState: 'RECEIVED',
    authority: 'event-injector',
    raw: {
      lineageId,
      variant,
      accountId,
      substrate,
    },
  });

  // If there's a media payload, emit normalized state
  if (substrate.media) {
    observability.transition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId: intentId,
      previousState: 'RECEIVED',
      nextState: 'NORMALIZED',
      authority: 'normalization-layer',
      raw: {
        lineageId,
        media: substrate.media,
        validationPassed: variant === 'success',
      },
    });
  }

  // If error present (rate-limited), emit error state
  if (substrate.error) {
    observability.transition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId: intentId,
      previousState: 'RECEIVED',
      nextState: 'FAILED',
      authority: 'event-injector',
      raw: {
        lineageId,
        error: substrate.error,
        expectedBehavior: substrate.expectedBehavior,
      },
    });
  }

  return {
    lineageId,
    intentId,
    variant,
    substrate,
    timestamp: Date.now(),
  };
}

/**
 * Inject a lineage event for replay testing.
 * Simulates events being replayed from the lineage ledger.
 *
 * @param {object} options
 * @param {string} options.eventType - Event type to emit
 * @param {string} options.domain - Domain for the event
 * @param {string} options.fromState - Previous state
 * @param {string} options.toState - New state
 * @returns {Promise<object>} Injection result
 */
async function injectLineageEvent({ eventType, domain, fromState, toState }) {
  const lineageId = `lineage-replay-${Date.now()}`;
  const entityId = `${domain}-${Date.now()}`;

  observability.transition({
    domain,
    entity: 'fsm',
    entityId,
    previousState: fromState,
    nextState: toState,
    authority: 'event-injector',
    raw: {
      lineageId,
      eventType,
      replayed: true,
    },
  });

  return { lineageId, entityId, eventType, timestamp: Date.now() };
}

/**
 * Inject a reconciliation trigger.
 * Simulates a CADENCE_TICK triggering reconciliation.
 *
 * @returns {Promise<object>} Injection result
 */
async function injectReconciliationTick() {
  const tickId = `reconcile-${Date.now()}`;

  observability.transition({
    domain: 'reconciliation',
    entity: 'fsm',
    entityId: 'reconciliation-fsm',
    previousState: 'IDLE',
    nextState: 'RECONCILING',
    authority: 'event-injector',
    raw: {
      tickId,
      triggeredBy: 'cadence-tick',
    },
  });

  return { tickId, timestamp: Date.now() };
}

/**
 * Inject a coordinated mixed-domain wave.
 * Simulates concurrent lifecycle evolution across bounded domains.
 *
 * @param {object} options
 * @param {string} options.waveId - Stable wave identifier
 * @param {number} options.seq - Sequence number within the wave
 * @param {boolean} [options.includeFault=false] - Whether to inject disorder
 * @returns {Promise<object>}
 */
async function injectMixedDomainWave({ waveId, seq, includeFault = false }) {
  const baseId = `${waveId}-${seq}`;
  const now = Date.now();
  const domains = [
    {
      domain: 'acquisition',
      entity: 'fsm',
      entityId: `acq-${baseId}`,
      previousState: 'IDLE',
      nextState: includeFault ? 'DEGRADED' : 'FETCHING',
    },
    {
      domain: 'engagement',
      entity: 'fsm',
      entityId: `eng-${baseId}`,
      previousState: 'IDLE',
      nextState: 'EVALUATING',
    },
    {
      domain: 'publishing',
      entity: 'fsm',
      entityId: `pub-${baseId}`,
      previousState: 'QUEUED',
      nextState: includeFault ? 'RETRYING' : 'PUBLISHING',
    },
    {
      domain: 'scheduling',
      entity: 'fsm',
      entityId: `sch-${baseId}`,
      previousState: 'SCHEDULED',
      nextState: 'DISPATCHING',
    },
    {
      domain: 'telemetry',
      entity: 'fsm',
      entityId: `tel-${baseId}`,
      previousState: 'IDLE',
      nextState: 'PROJECTING',
    },
    {
      domain: 'reconciliation',
      entity: 'fsm',
      entityId: `rec-${baseId}`,
      previousState: 'IDLE',
      nextState: includeFault ? 'RECONCILING' : 'MONITORING',
    },
    {
      domain: 'projection',
      entity: 'fsm',
      entityId: `prj-${baseId}`,
      previousState: 'IDLE',
      nextState: 'MATERIALIZING',
    },
  ];

  for (const transition of domains) {
    observability.transition({
      ...transition,
      authority: 'phase-3-wave-injector',
      raw: {
        waveId,
        seq,
        includeFault,
        emittedAt: now,
      },
    });
  }

  if (includeFault) {
    observability.transition({
      domain: 'reconciliation',
      entity: 'event_gate',
      entityId: `gate-${baseId}`,
      previousState: 'PASS',
      nextState: 'QUARANTINED',
      authority: 'phase-3-wave-injector',
      raw: {
        waveId,
        seq,
        reason: 'injected-disorder-window',
      },
    });
  }

  return {
    waveId,
    seq,
    emittedTransitions: includeFault ? domains.length + 1 : domains.length,
    includeFault,
    timestamp: now,
  };
}

/**
 * Inject a dedup replay event.
 * Simulates a duplicate detection in the dedup FSM.
 *
 * @param {string} resourceId - The duplicate resource ID
 * @returns {Promise<object>} Injection result
 */
async function injectDedupReplay(resourceId = 'media-dedup-test') {
  const lineageId = `lineage-dedup-${Date.now()}`;

  observability.transition({
    domain: 'dedup',
    entity: 'resource_tracker',
    previousState: 'TRACKED',
    nextState: 'REPLAY_DETECTED',
    authority: 'event-injector',
    raw: {
      lineageId,
      resourceKey: resourceId,
      detectedAt: Date.now(),
    },
  });

  return { lineageId, resourceId, timestamp: Date.now() };
}

/**
 * Store a lineage marker in Redis for persistence testing.
 *
 * @param {string} key - Lineage key
 * @param {object} value - Value to store
 */
async function storeLineageMarker(key, value) {
  const redis = getRedisClient();
  await redis.set(key, JSON.stringify(value));
  return { key, storedAt: Date.now() };
}

/**
 * Inject an adversarial transition attempting cross-domain authority violation.
 * Used to test membrane attack resistance.
 *
 * @param {object} opts
 * @param {string} opts.membrane - 'publishing' | 'telemetry' | 'reconciliation'
 * @param {string} opts.targetDomain - domain being attacked
 * @param {string} opts.entityId - entity ID of target
 * @returns {object} injection result
 */
function injectAdversarialTransition({ membrane, targetDomain, entityId }) {
  const now = Date.now();
  const authorityMap = {
    publishing: 'publishing-membrane',
    telemetry: 'telemetry-worker',
    reconciliation: 'reconciliation-fsm',
  };
  const authority = authorityMap[membrane] || membrane;

  observability.transition({
    domain: targetDomain,
    entity: 'fsm',
    entityId: entityId || `${targetDomain}-hijacked-${now}`,
    previousState: 'IDLE',
    nextState: membrane === 'telemetry' ? 'CORRUPTED' : 'HIJACKED',
    authority,
    raw: {
      adversarial: true,
      membrane,
      targetDomain,
      attemptedAt: now,
    },
  });

  return { membrane, targetDomain, authority, timestamp: now };
}

/**
 * Inject a transition with an intentionally older timestamp to test
 * out-of-order replay detection.
 *
 * @param {object} opts
 * @param {string} [opts.domain='governance'] - domain
 * @param {string} [opts.entity='fsm'] - entity
 * @param {string} opts.entityId - entity ID
 * @param {string} opts.previousState - previous state
 * @param {string} opts.nextState - next state
 * @param {number} [opts.backDateMs=5000] - how many ms in the past
 * @returns {object} injection result
 */
function injectOutOfOrderEntry({ domain = 'governance', entity = 'fsm', entityId, previousState, nextState, backDateMs = 5000 }) {
  const now = Date.now();
  const staleTimestamp = now - backDateMs;

  observability.transition({
    domain,
    entity,
    entityId: entityId || `${domain}-stale-${now}`,
    previousState,
    nextState,
    authority: 'out-of-order-injector',
    raw: {
      outOfOrder: true,
      emittedAt: staleTimestamp,
      actualAt: now,
    },
  });

  return { entityId, staleTimestamp, now, timestamp: now };
}

/**
 * Inject the exact same transition twice to test duplicate causal chain detection.
 * The runtime should handle this idempotently.
 *
 * @param {object} opts
 * @param {string} [opts.domain='acquisition'] - domain
 * @param {string} [opts.entity='acquisition_intent'] - entity
 * @param {string} opts.entityId - entity ID
 * @param {string} opts.previousState - previous state
 * @param {string} opts.nextState - next state
 * @returns {object} injection result with the two emission timestamps
 */
function injectDuplicateCausalChain({ domain = 'acquisition', entity = 'acquisition_intent', entityId, previousState, nextState }) {
  const now = Date.now();
  const lineageId = `dup-chain-${now}`;

  // First emission
  observability.transition({
    domain,
    entity,
    entityId,
    previousState,
    nextState,
    authority: 'duplicate-chain-injector',
    raw: { lineageId, duplicateEmission: 1 },
  });

  // Second emission — identical
  observability.transition({
    domain,
    entity,
    entityId,
    previousState,
    nextState,
    authority: 'duplicate-chain-injector',
    raw: { lineageId, duplicateEmission: 2 },
  });

  return { entityId, lineageId, timestamp: now };
}

/**
 * Inject a transition with a broken causal chain reference — parentTransitionId
 * points to a traceId that does not exist in the ledger. This simulates a
 * corrupted or maliciously constructed transition where the causal parent
 * reference cannot be resolved.
 *
 * @param {object} opts
 * @param {string} [opts.domain='acquisition'] - domain
 * @param {string} [opts.entity='acquisition_intent'] - entity
 * @param {string} [opts.entityId] - entity ID (defaults to generated)
 * @param {string} opts.previousState - previous state
 * @param {string} opts.nextState - next state
 * @param {string} [opts.brokenParentTransitionId='non-existent-trace-id'] - the broken reference
 * @returns {object} injection result
 */
function injectBrokenCausalChain({ domain = 'acquisition', entity = 'acquisition_intent', entityId, previousState, nextState, brokenParentTransitionId = 'non-existent-trace-id-00000000' }) {
  const now = Date.now();
  const lineageId = `broken-chain-${now}`;
  const traceId = `broken-trace-${now}`;
  const cid = `broken-corr-${now}`;

  observability.transition({
    domain,
    entity,
    entityId: entityId || `${domain}-broken-${now}`,
    previousState,
    nextState,
    authority: 'broken-chain-injector',
    traceId,
    correlationId: cid,
    causationId: null,
    parentTransitionId: brokenParentTransitionId,
    raw: { lineageId, brokenCausalChain: true },
  });

  return { lineageId, traceId, correlationId: cid, brokenParentTransitionId, timestamp: now };
}

/**
 * Inject two conflicting transitions from the same previous state to the same entity.
 * Tests that conflicting FSM transitions are detected as violations.
 *
 * @param {object} opts
 * @param {string} [opts.domain='governance'] - domain
 * @param {string} [opts.entity='fsm'] - entity
 * @param {string} opts.entityId - entity ID
 * @param {string} opts.previousState - previous state (must be same for both)
 * @param {string} opts.nextStateA - first conflicting nextState
 * @param {string} opts.nextStateB - second conflicting nextState
 * @returns {object} injection result
 */
function injectConflictingTransition({ domain = 'governance', entity = 'fsm', entityId, previousState, nextStateA, nextStateB }) {
  const now = Date.now();

  observability.transition({
    domain,
    entity,
    entityId,
    previousState,
    nextState: nextStateA,
    authority: 'conflict-injector',
    raw: { conflictAttempt: 'first', timestamp: now },
  });

  observability.transition({
    domain,
    entity,
    entityId,
    previousState,
    nextState: nextStateB,
    authority: 'conflict-injector',
    raw: { conflictAttempt: 'second', timestamp: now },
  });

  return { entityId, previousState, nextStateA, nextStateB, timestamp: now };
}

module.exports = {
  injectAcquisitionIntent,
  injectLineageEvent,
  injectReconciliationTick,
  injectMixedDomainWave,
  injectDedupReplay,
  storeLineageMarker,
  injectAdversarialTransition,
  injectOutOfOrderEntry,
  injectDuplicateCausalChain,
  injectBrokenCausalChain,
  injectConflictingTransition,
  mockSubstrates,
};
