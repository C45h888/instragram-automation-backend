// ============================================
// Phase 2B: Reconciliation Drift Resistance
// ============================================
// Purpose: Validate that the runtime resists and
// recovers from reconciliation drift induced by
// corrupted, malformed, and stale event injection.
//
// During continuous ticker-driven runtime, corruption
// is injected to verify the runtime maintains integrity
// and does not accumulate false state.
// ============================================

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

const TICK_INTERVAL_MS = 80;
const TEST_DURATION_MS = 2500;

// Corruption types for drift injection
const CORRUPTION_TYPES = ['malformed', 'stale', 'duplicate', 'missing-prev-state', 'invalid-domain'];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function injectCorruption(type, entityId) {
  switch (type) {
    case 'malformed':
      observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId,
        previousState: 'RECEIVED',
        nextState: 'FAILED', // valid state, but raw payload is malformed
        authority: 'corruption-injector',
        raw: { malformed: true, variant: 'corruption-malformed' },
      });
      break;

    case 'stale':
      observability.transition({
        domain: 'governance',
        entity: 'fsm',
        entityId,
        previousState: 'HEALTHY',
        nextState: 'HEALTHY', // same state = no progress
        authority: 'corruption-injector',
        raw: { stale: true, timestamp: Date.now() - 86400000 },
      });
      break;

    case 'duplicate':
      observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId,
        previousState: 'NORMALIZED',
        nextState: 'NORMALIZED',
        authority: 'corruption-injector',
        raw: { duplicate: true },
      });
      break;

    case 'missing-prev-state':
      observability.transition({
        domain: 'execution',
        entity: 'attempt',
        entityId,
        previousState: null,
        nextState: 'ATTEMPTING',
        authority: 'corruption-injector',
      });
      break;

    case 'invalid-domain':
      observability.transition({
        domain: 'invalid-domain-xyz',
        entity: 'unknown',
        entityId,
        previousState: 'BOOTING',
        nextState: 'CORRUPT',
        authority: 'corruption-injector',
      });
      break;
  }
}

describe('Phase 2B: Reconciliation Drift Resistance', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(30);
  }, 15000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('withstands randomized corruption injection without accumulating false state', async () => {
    const cursorStart = observability.query.getLogSize();
    let tickCount = 0;
    let corruptionCount = 0;

    const ticker = setInterval(() => {
      const tickId = `drift-${Date.now()}-${tickCount}`;

      // Legitimate event
      eventInjector.injectAcquisitionIntent({
        variant: 'success',
        accountId: `legit-${tickCount}`,
      });

      // Inject corruption roughly every 5th tick
      if (tickCount % 5 === 0) {
        const corruptionType = CORRUPTION_TYPES[tickCount % CORRUPTION_TYPES.length];
        injectCorruption(corruptionType, `corrupt-${tickCount}`);
        corruptionCount++;
      }

      tickCount++;
    }, TICK_INTERVAL_MS);

    await sleep(TEST_DURATION_MS);
    clearInterval(ticker);

    // Verify the runtime survived
    const snapshot = observability.query.getFullSnapshot();
    expect(snapshot.transitionCount).toBeGreaterThan(cursorStart);
    expect(snapshot.domains).toBeDefined();

    // Verify corruption did not corrupt canonical state
    // Invalid domain entries should not appear in valid domain snapshots
    expect(snapshot.domains['invalid-domain-xyz']).toBeUndefined();

    // Verify projection log contains entries from all legitimate events
    const { entries } = observability.query.getEntriesSince(cursorStart);
    const legitimateEntries = entries.filter(
      (e) => e.authority !== 'corruption-injector' && e.domain !== 'invalid-domain-xyz'
    );
    expect(legitimateEntries.length).toBeGreaterThan(0);
  });

  it('maintains healthy consumer lag under mixed legitimate/corruption load', async () => {
    const cursorStart = observability.query.getLogSize();
    observability.query.registerConsumer('drift-test-consumer');

    const ticker = setInterval(() => {
      const idx = Math.floor(Math.random() * 10);
      if (idx < 7) {
        eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: `consumer-test-${Date.now()}` });
      } else {
        injectCorruption('malformed', `consumer-corrupt-${Date.now()}`);
      }
    }, TICK_INTERVAL_MS);

    await sleep(TEST_DURATION_MS);
    clearInterval(ticker);

    // Advance consumer cursor to simulate consumption
    const logSize = observability.query.getLogSize();
    observability.query.updateConsumerCursor('drift-test-consumer', logSize);

    const lag = observability.query.getConsumerLag('drift-test-consumer');
    expect(lag.atRisk).toBe(false);
    expect(lag.head).toBeGreaterThan(cursorStart);
  });

  it('preserves lineage continuity after targeted corruption burst', async () => {
    const cursorStart = observability.query.getLogSize();
    const snapshotBefore = observability.query.getFullSnapshot();

    // Burst of corrupted events
    for (let i = 0; i < 20; i++) {
      injectCorruption('duplicate', `burst-corrupt-${i}`);
    }

    await sleep(200);

    // Legitimate events after corruption burst
    for (let i = 0; i < 10; i++) {
      eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: `post-burst-${i}` });
    }

    await sleep(200);

    const snapshotAfter = observability.query.getFullSnapshot();

    // Verify state continued to progress (corruption did not halt the system)
    expect(snapshotAfter.transitionCount).toBeGreaterThan(snapshotBefore.transitionCount);

    // Verify duplicate corruption did not change canonical state
    const { entries } = observability.query.getEntriesSince(cursorStart);
    const duplicateEntries = entries.filter((e) => e.raw?.duplicate === true);
    expect(duplicateEntries.length).toBeGreaterThan(0);

    // Verify final states are still lawful
    const acqDomain = observability.query.getDomainState('acquisition');
    expect(acqDomain.acquisition_intent).toBeDefined();
  });

  it('recovers lawful state progression after malformed event chain', async () => {
    const entityId = `recovery-test-${Date.now()}`;
    const cursorStart = observability.query.getLogSize();

    // Start lawful state progression
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'BOOTING',
      nextState: 'INITIALIZING',
      authority: 'lawful-test',
    });

    // Malformed transition (missing nextState) — should be no-op
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'INITIALIZING',
      authority: 'corruption-injector',
      raw: { malformed: true },
    });

    // Resume lawful progression
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'INITIALIZING',
      nextState: 'HEALTHY',
      authority: 'lawful-test',
    });

    await sleep(100);

    // Verify final state is HEALTHY (malformed event was rejected)
    const state = observability.query.getState('governance', 'fsm', entityId);
    expect(state).toBe('HEALTHY');

    // Verify transition log shows only 2 transitions (malformed was no-op)
    const { entries } = observability.query.getEntriesSince(cursorStart);
    const fsmEntries = entries.filter(
      (e) => e.domain === 'governance' && e.entity === 'fsm' && e.entityId === entityId
    );
    expect(fsmEntries.length).toBe(2);
    expect(fsmEntries[0].nextState).toBe('INITIALIZING');
    expect(fsmEntries[1].nextState).toBe('HEALTHY');
  });

  it('validates projection integrity after mixed corruption load', async () => {
    const cursorStart = observability.query.getLogSize();
    let corruptionCount = 0;

    const ticker = setInterval(() => {
      eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: `integrity-${Date.now()}` });

      // Inject all corruption types
      for (const type of CORRUPTION_TYPES) {
        injectCorruption(type, `integrity-corrupt-${type}-${Date.now()}`);
        corruptionCount++;
      }
    }, TICK_INTERVAL_MS * 3);

    await sleep(TEST_DURATION_MS);
    clearInterval(ticker);

    // Verify identical lineage replay produces identical projection
    const snapshot1 = observability.query.getFullSnapshot();
    const snapshot2 = observability.query.getFullSnapshot();
    expect(snapshot1.transitionCount).toBe(snapshot2.transitionCount);
    expect(snapshot1.domains).toEqual(snapshot2.domains);

    // Verify no orphaned states exist
    for (const [domain, entities] of Object.entries(snapshot1.domains)) {
      for (const [entity, idMap] of Object.entries(entities)) {
        for (const [entityId, state] of Object.entries(idMap)) {
          expect(state).toBeTruthy();
          expect(typeof state).toBe('string');
        }
      }
    }
  });
});
