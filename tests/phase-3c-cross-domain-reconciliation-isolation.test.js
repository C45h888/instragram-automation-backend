import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Phase 3C: Cross-Domain Reconciliation Isolation', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(35);
  }, 15000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('contains reconciliation activity without semantic spillover', async () => {
    const start = observability.query.getLogSize();
    const waveId = `phase3c-${Date.now()}`;

    for (let i = 0; i < 30; i++) {
      const includeFault = i % 4 === 0;
      await eventInjector.injectMixedDomainWave({ waveId, seq: i, includeFault });
      if (includeFault) {
        await eventInjector.injectReconciliationTick();
      }
    }
    await sleep(150);

    const { entries } = observability.query.getEntriesSince(start);
    const recEntries = entries.filter((e) => e.domain === 'reconciliation');
    const nonRecTaggedAsRec = entries.filter(
      (e) =>
        e.domain !== 'reconciliation' &&
        (String(e.entityId || '').startsWith('rec-') || e.entity === 'event_gate')
    );

    expect(recEntries.length).toBeGreaterThan(0);
    expect(nonRecTaggedAsRec.length).toBe(0);

    const lagConsumer = `phase3c-consumer-${Date.now()}`;
    observability.query.registerConsumer(lagConsumer);
    observability.query.updateConsumerCursor(lagConsumer, observability.query.getLogSize());
    const lag = observability.query.getConsumerLag(lagConsumer);
    expect(lag.atRisk).toBe(false);
  });
});
