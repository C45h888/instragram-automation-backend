import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Phase 1A: Observability + Smoke Contracts', () => {
  beforeAll(async () => {
    await observability.init();
  }, 10000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('projects emitted transitions into state and transition log', async () => {
    const entityId = `obs-causal-${Date.now()}`;
    const cursorStart = observability.query.getLogSize();
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'BOOTING',
      nextState: 'HEALTHY',
      authority: 'phase-1a-contract-test',
    });
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'HEALTHY',
      nextState: 'DEGRADED',
      authority: 'phase-1a-contract-test',
    });

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const chainEntries = entries.filter(
      (e) => e.domain === 'governance' && e.entity === 'fsm' && e.entityId === entityId
    );

    expect(chainEntries.length).toBeGreaterThanOrEqual(2);
    expect(chainEntries[chainEntries.length - 1].nextState).toBe('DEGRADED');
    expect(observability.query.getState('governance', 'fsm', entityId)).toBe('DEGRADED');
  });

  it('is deterministic for equivalent substrate-injected transition sequences', async () => {
    const runIdA = `det-a-${Date.now()}`;
    const runIdB = `det-b-${Date.now()}`;
    const cursorStart = observability.query.getLogSize();

    const emitSequence = (idPrefix) => {
      observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId: `${idPrefix}-intent`,
        previousState: null,
        nextState: 'RECEIVED',
        authority: 'mock-substrate',
        raw: { variant: 'success' },
      });
      observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId: `${idPrefix}-intent`,
        previousState: 'RECEIVED',
        nextState: 'NORMALIZED',
        authority: 'normalization-layer',
        raw: { variant: 'success' },
      });
      observability.transition({
        domain: 'governance',
        entity: 'fsm',
        entityId: `${idPrefix}-governance`,
        previousState: 'BOOTING',
        nextState: 'HEALTHY',
        authority: 'mock-governance',
      });
    };

    emitSequence(runIdA);
    emitSequence(runIdB);

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const normalize = (prefix) =>
      entries
        .filter((e) => e.entityId && String(e.entityId).startsWith(prefix))
        .map((e) => `${e.domain}|${e.entity}|${e.previousState}|${e.nextState}|${e.authority}`);

    const seqA = normalize(runIdA);
    const seqB = normalize(runIdB);

    expect(seqA.length).toBe(3);
    expect(seqB.length).toBe(3);
    expect(seqA).toEqual(seqB);
  });

  it('emits semantic projection transitions with replay watermark metadata', async () => {
    const cursorStart = observability.query.getLogSize();
    await telemetryWorkers.startAll(25);
    await sleep(120);
    await telemetryWorkers.stopAll();

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const projectionEntries = entries.filter(
      (e) => e.entity === 'semantic_projection' && e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );

    expect(projectionEntries.length).toBeGreaterThan(0);
    const sample = projectionEntries[0];
    expect(sample.raw).toBeDefined();
    expect(sample.raw.entryType).toBe('SEMANTIC_PROJECTION_TRANSITION');
    expect(sample.raw.sourceTelemetryWindow).toBeDefined();
    expect(typeof sample.raw.sourceTelemetryWindow.lineageStartCursor).toBe('number');
    expect(typeof sample.raw.sourceTelemetryWindow.lineageEndCursor).toBe('number');
  });
});
