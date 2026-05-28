import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Phase 3B: Membrane Boundary Integrity', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(40);
  }, 15000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('quarantines disorder windows without contaminating bounded domains', async () => {
    const start = observability.query.getLogSize();
    const waveId = `phase3b-${Date.now()}`;

    for (let i = 0; i < 18; i++) {
      await eventInjector.injectMixedDomainWave({ waveId, seq: i, includeFault: i % 3 === 0 });
    }
    await sleep(120);

    const { entries } = observability.query.getEntriesSince(start);
    const quarantines = entries.filter(
      (e) =>
        e.authority === 'phase-3-wave-injector' &&
        e.domain === 'reconciliation' &&
        e.entity === 'event_gate' &&
        e.nextState === 'QUARANTINED' &&
        e.raw?.waveId === waveId
    );
    expect(quarantines.length).toBeGreaterThan(0);

    // Verify domain separation remains intact in same window.
    const crossTagged = entries.filter(
      (e) =>
        e.authority === 'phase-3-wave-injector' &&
        e.raw?.waveId === waveId &&
        ((e.domain === 'acquisition' && String(e.entityId).startsWith('pub-')) ||
          (e.domain === 'publishing' && String(e.entityId).startsWith('acq-')))
    );
    expect(crossTagged.length).toBe(0);
  });
});
