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

module.exports = {
  injectAcquisitionIntent,
  injectLineageEvent,
  injectReconciliationTick,
  injectMixedDomainWave,
  injectDedupReplay,
  storeLineageMarker,
  mockSubstrates,
};
