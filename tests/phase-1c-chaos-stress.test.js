import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Phase 1C: Controlled Chaos Stress (Constitutional Stability)', () => {
  beforeAll(async () => {
    await observability.init();
  }, 10000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('withstands duplicate burst without regressing terminal canonical state', async () => {
    const resourceId = `chaos-dup-${Date.now()}`;
    const cursorStart = observability.query.getLogSize();

    const burst = Array.from({ length: 25 }, () => eventInjector.injectDedupReplay(resourceId));
    await Promise.all(burst);
    await sleep(120);

    const state = observability.query.getState('dedup', 'resource_tracker', resourceId);
    expect(state).toBe('REPLAY_DETECTED');

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const replayEntries = entries.filter(
      (e) => e.domain === 'dedup' && e.entity === 'resource_tracker' && e.entityId === resourceId
    );
    expect(replayEntries.length).toBeGreaterThanOrEqual(25);
    expect(replayEntries[replayEntries.length - 1].nextState).toBe('REPLAY_DETECTED');
  });

  it('resists malformed/no-op packets during high transition volume', async () => {
    const entityId = `chaos-malformed-${Date.now()}`;
    const cursorStart = observability.query.getLogSize();

    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'BOOTING',
      nextState: 'HEALTHY',
      authority: 'chaos-test',
    });

    for (let i = 0; i < 40; i++) {
      observability.transition({
        domain: 'governance',
        entity: 'fsm',
        entityId,
        previousState: 'HEALTHY',
        authority: 'chaos-malformed-packet',
        raw: { malformed: true, index: i },
      });
    }

    const state = observability.query.getState('governance', 'fsm', entityId);
    expect(state).toBe('HEALTHY');

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const entityEntries = entries.filter(
      (e) => e.domain === 'governance' && e.entity === 'fsm' && e.entityId === entityId
    );
    expect(entityEntries.length).toBe(1);
  });

  it('maintains transition growth and lawful state under delayed mixed substrate injections', async () => {
    const cursorStart = observability.query.getLogSize();
    const injections = [
      () => eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: 'stress-a' }),
      () => eventInjector.injectAcquisitionIntent({ variant: 'partial', accountId: 'stress-b' }),
      () => eventInjector.injectAcquisitionIntent({ variant: 'stale', accountId: 'stress-c' }),
      () => eventInjector.injectAcquisitionIntent({ variant: 'rateLimited', accountId: 'stress-d' }),
    ];

    for (const inject of injections) {
      await inject();
      await sleep(35); // queue-delay style jitter
    }

    await sleep(120);
    const { entries } = observability.query.getEntriesSince(cursorStart);
    expect(entries.length).toBeGreaterThan(0);

    const acqEntries = entries.filter((e) => e.domain === 'acquisition' && e.entity === 'acquisition_intent');
    expect(acqEntries.length).toBeGreaterThan(0);
  });

  it('survives projection worker stop/start turbulence and preserves observability continuity', async () => {
    const cursorStart = observability.query.getLogSize();

    await telemetryWorkers.startAll(30);
    await sleep(120);
    await telemetryWorkers.stopAll();

    // "Worker crash/restart" simulation
    await telemetryWorkers.startAll(30);
    await sleep(120);
    await telemetryWorkers.stopAll();

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const projectionEntries = entries.filter(
      (e) => e.entity === 'semantic_projection' && e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );
    expect(projectionEntries.length).toBeGreaterThan(0);
  });
});
