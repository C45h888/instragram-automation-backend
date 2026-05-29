import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stableSignature(entries, waveId) {
  return JSON.stringify(
    entries
      .filter((e) => e.authority === 'phase-3-wave-injector' && e.raw?.waveId === waveId)
      .map((e) => ({ d: e.domain, en: e.entity, p: e.previousState, n: e.nextState, s: e.raw?.seq, f: !!e.raw?.includeFault }))
      .sort((a, b) => (a.s - b.s) || `${a.d}:${a.en}:${a.n}`.localeCompare(`${b.d}:${b.en}:${b.n}`))
  );
}

describe('Phase 4D: Restart Recovery Determinism', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(35);
  }, 20000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('maintains deterministic schedule signature across worker restart windows', async () => {
    const schedule = [false, true, false, false, true, false];
    const runWave = async (waveId) => {
      const cursor = observability.query.getLogSize();
      for (let i = 0; i < schedule.length; i++) {
        await eventInjector.injectMixedDomainWave({ waveId, seq: i, includeFault: schedule[i] });
      }
      await sleep(120);
      const { entries } = observability.query.getEntriesSince(cursor);
      return stableSignature(entries, waveId);
    };

    const sigA = await runWave(`phase4d-a-${Date.now()}`);
    await telemetryWorkers.stopAll();
    await sleep(120);
    await telemetryWorkers.startAll(35);
    const sigB = await runWave(`phase4d-b-${Date.now()}`);
    expect(sigA).toBe(sigB);
  });
});
