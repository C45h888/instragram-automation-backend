import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');
const { waitForCursorAdvance } = require('./helpers/sync-barriers');

describe('Phase 4C: Cross-Domain Pressure Stability', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(35);
  }, 20000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('preserves projection continuity and healthy lag under mixed domain pressure', async () => {
    const start = observability.query.getLogSize();
    const consumer = `phase4c-${Date.now()}`;
    observability.query.registerConsumer(consumer);
    const waveId = `phase4c-wave-${Date.now()}`;

    for (let i = 0; i < 20; i++) {
      await eventInjector.injectMixedDomainWave({ waveId, seq: i, includeFault: i % 4 === 0 });
    }

    // Wait for the transition log to advance past the injection start cursor
    await waitForCursorAdvance(start, 4000);

    const { entries } = observability.query.getEntriesSince(start);
    const projectionTransitions = entries.filter((e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION');
    expect(projectionTransitions.length).toBeGreaterThan(0);

    const contamination = entries.filter(
      (e) =>
        e.authority === 'phase-3-wave-injector' &&
        ((e.domain === 'acquisition' && String(e.entityId).startsWith('pub-')) ||
          (e.domain === 'publishing' && String(e.entityId).startsWith('acq-')))
    );
    expect(contamination.length).toBe(0);

    observability.query.updateConsumerCursor(consumer, observability.query.getLogSize());
    const lag = observability.query.getConsumerLag(consumer);
    expect(lag.atRisk).toBe(false);
  });
});
