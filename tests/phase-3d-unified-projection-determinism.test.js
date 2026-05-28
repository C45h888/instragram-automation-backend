import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stableStringify(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
}

function signatureForWave(entries, waveId) {
  const reduced = entries
    .filter((e) => e.authority === 'phase-3-wave-injector' && e.raw?.waveId === waveId)
    .map((e) => ({
      d: e.domain,
      en: e.entity,
      p: e.previousState,
      n: e.nextState,
      s: e.raw?.seq,
      f: !!e.raw?.includeFault,
    }))
    .sort((a, b) => {
      if (a.s !== b.s) return a.s - b.s;
      return `${a.d}:${a.en}:${a.n}`.localeCompare(`${b.d}:${b.en}:${b.n}`);
    });
  return stableStringify(reduced);
}

describe('Phase 3D: Unified Projection Determinism', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(40);
  }, 15000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('produces deterministic signatures for identical mixed-domain schedules', async () => {
    const runSchedule = [false, false, true, false, true, false, false, true, false, false];

    const runWave = async (waveId) => {
      const cursor = observability.query.getLogSize();
      for (let i = 0; i < runSchedule.length; i++) {
        await eventInjector.injectMixedDomainWave({ waveId, seq: i, includeFault: runSchedule[i] });
      }
      await sleep(120);
      const { entries } = observability.query.getEntriesSince(cursor);
      return signatureForWave(entries, waveId);
    };

    const sigA = await runWave(`phase3d-a-${Date.now()}`);
    const sigB = await runWave(`phase3d-b-${Date.now()}`);

    expect(sigA).toBe(sigB);
  });
});
