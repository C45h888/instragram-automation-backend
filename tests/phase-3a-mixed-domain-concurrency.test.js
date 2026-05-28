import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Phase 3A: Mixed-Domain Concurrency', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(40);
  }, 15000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('maintains lawful concurrent progression across bounded domains', async () => {
    const start = observability.query.getLogSize();
    const waveId = `phase3a-${Date.now()}`;
    const totalWaves = 24;

    const emissions = [];
    for (let i = 0; i < totalWaves; i++) {
      const includeFault = i % 6 === 0;
      emissions.push(eventInjector.injectMixedDomainWave({ waveId, seq: i, includeFault }));
    }
    await Promise.all(emissions);
    await sleep(120);

    const { entries } = observability.query.getEntriesSince(start);
    const phaseEntries = entries.filter((e) => e.authority === 'phase-3-wave-injector' && e.raw?.waveId === waveId);
    expect(phaseEntries.length).toBeGreaterThan(0);

    const expectedDomains = ['acquisition', 'engagement', 'publishing', 'scheduling', 'telemetry', 'reconciliation', 'projection'];
    const presentDomains = new Set(phaseEntries.map((e) => e.domain));
    for (const d of expectedDomains) {
      expect(presentDomains.has(d)).toBe(true);
    }

    // Ensure domain-local entity IDs do not leak across boundaries.
    for (const e of phaseEntries) {
      if (e.domain === 'acquisition') expect(String(e.entityId).startsWith('acq-')).toBe(true);
      if (e.domain === 'engagement') expect(String(e.entityId).startsWith('eng-')).toBe(true);
      if (e.domain === 'publishing') expect(String(e.entityId).startsWith('pub-')).toBe(true);
      if (e.domain === 'scheduling') expect(String(e.entityId).startsWith('sch-')).toBe(true);
      if (e.domain === 'telemetry') expect(String(e.entityId).startsWith('tel-')).toBe(true);
      if (e.domain === 'reconciliation' && e.entity === 'fsm') expect(String(e.entityId).startsWith('rec-')).toBe(true);
      if (e.domain === 'projection') expect(String(e.entityId).startsWith('prj-')).toBe(true);
    }
  });
});
