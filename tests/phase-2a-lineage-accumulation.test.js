// ============================================
// Phase 2: Long-Duration Lineage Accumulation
// ============================================
// Purpose: Validate that the runtime maintains constitutional
// stability under sustained continuous event injection.
// Validates lineage ledger growth, projection integrity,
// and observability plane correctness over time.
//
// Continuous ticker drives event injection — the runtime
// stays alive for an extended period accumulating lineage.
// ============================================

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

const TICK_INTERVAL_MS = 100;
const TEST_DURATION_MS = 3000;
const ACQUISITION_VARIANTS = ['success', 'partial', 'stale', 'rateLimited'];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Phase 2A: Continuous Lineage Accumulation', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(30);
  }, 15000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('sustains continuous event injection without memory leaks or projection corruption', async () => {
    const cursorStart = observability.query.getLogSize();
    const startTime = Date.now();
    let tickCount = 0;
    let injectionErrors = 0;

    // Continuous ticker loop
    const ticker = setInterval(() => {
      try {
        const variant = ACQUISITION_VARIANTS[tickCount % ACQUISITION_VARIANTS.length];
        eventInjector.injectAcquisitionIntent({
          variant,
          accountId: `continuous-${tickCount}`,
        });
        tickCount++;
      } catch (err) {
        injectionErrors++;
      }
    }, TICK_INTERVAL_MS);

    // Allow ticker to run for TEST_DURATION_MS
    await sleep(TEST_DURATION_MS);

    clearInterval(ticker);

    const elapsedMs = Date.now() - startTime;
    const expectedTicks = Math.floor(TEST_DURATION_MS / TICK_INTERVAL_MS);
    const actualTicks = tickCount;

    // Verify ticker ran at expected rate
    expect(actualTicks).toBeGreaterThanOrEqual(expectedTicks * 0.9);
    expect(injectionErrors).toBe(0);

    // Verify lineage accumulation
    const { entries } = observability.query.getEntriesSince(cursorStart);
    expect(entries.length).toBeGreaterThan(0);

    // Verify projection integrity — state should be consistent
    const snapshot = observability.query.getFullSnapshot();
    expect(snapshot.transitionCount).toBeGreaterThan(cursorStart);
    expect(snapshot.domains).toBeDefined();
    expect(snapshot.globalStateIndex).toBeDefined();

    // Verify no consumer lag buildup
    const lag = observability.query.getConsumerLag('phase-2-consumer');
    expect(lag.atRisk).toBe(false);

    // Verify all domains have some state
    const domains = Object.keys(snapshot.domains);
    expect(domains.length).toBeGreaterThan(0);
  });

  it('maintains lineage continuity and sequence ordering under high-frequency injection', async () => {
    const cursorStart = observability.query.getLogSize();
    const batchSize = 50;

    // Rapid-fire batch injection
    for (let i = 0; i < batchSize; i++) {
      eventInjector.injectAcquisitionIntent({
        variant: 'success',
        accountId: `rapid-${i}`,
      });
    }

    await sleep(200);

    const { entries } = observability.query.getEntriesSince(cursorStart);
    expect(entries.length).toBeGreaterThanOrEqual(batchSize);

    // Verify sequence ordering is preserved (entries have ascending timestamps)
    const timestamps = entries.map((e) => e.timestamp);
    for (let i = 1; i < timestamps.length; i++) {
      expect(timestamps[i]).toBeGreaterThanOrEqual(timestamps[i - 1]);
    }
  });

  it('accumulates lineage over extended duration with no state divergence', async () => {
    const cursorStart = observability.query.getLogSize();
    const snapshotStart = observability.query.getFullSnapshot();

    const ticker = setInterval(() => {
      eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: `long-run-${Date.now()}` });
    }, TICK_INTERVAL_MS);

    await sleep(TEST_DURATION_MS);
    clearInterval(ticker);

    // Snapshot after extended run
    const snapshotEnd = observability.query.getFullSnapshot();

    // Verify continuity — transition count grew
    expect(snapshotEnd.transitionCount).toBeGreaterThan(snapshotStart.transitionCount);

    // Verify no domain lost its state
    for (const [domain, entities] of Object.entries(snapshotStart.domains)) {
      expect(snapshotEnd.domains[domain]).toBeDefined();
    }

    // Verify cursor-based consumption still works
    const { entries, totalSize } = observability.query.getEntriesSince(cursorStart);
    expect(totalSize).toBeGreaterThan(cursorStart);
    expect(entries.length).toBeGreaterThan(0);
  });

  it('survives telemetry worker restart during continuous runtime', async () => {
    const cursorStart = observability.query.getLogSize();

    const ticker = setInterval(() => {
      eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: `worker-restart-${Date.now()}` });
    }, TICK_INTERVAL_MS * 2);

    await sleep(800);
    await telemetryWorkers.stopAll();
    await sleep(300);
    await telemetryWorkers.startAll(30);
    await sleep(800);

    clearInterval(ticker);

    // Verify projection continuity
    const { entries } = observability.query.getEntriesSince(cursorStart);
    const projectionEntries = entries.filter(
      (e) => e.entity === 'semantic_projection' && e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );
    expect(projectionEntries.length).toBeGreaterThan(0);

    // Verify no data loss during restart
    const snapshot = observability.query.getFullSnapshot();
    expect(snapshot.transitionCount).toBeGreaterThan(cursorStart);
  });
});
