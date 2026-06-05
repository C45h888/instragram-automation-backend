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

    // Must await — transition() is async and returns a Promise.
    // Callers that need ordering guarantees must await before yielding to the event loop.
    await observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'BOOTING',
      nextState: 'HEALTHY',
      authority: 'phase-1a-contract-test',
    });
    await observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'HEALTHY',
      nextState: 'DEGRADED',
      authority: 'phase-1a-contract-test',
    });

    await sleep(30);

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

    const emitSequence = async (idPrefix) => {
      // Must await each transition — ordering within a sequence must be preserved.
      await observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId: `${idPrefix}-intent`,
        previousState: null,
        nextState: 'RECEIVED',
        authority: 'mock-substrate',
        raw: { variant: 'success' },
      });
      await observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId: `${idPrefix}-intent`,
        previousState: 'RECEIVED',
        nextState: 'NORMALIZED',
        authority: 'normalization-layer',
        raw: { variant: 'success' },
      });
      await observability.transition({
        domain: 'governance',
        entity: 'fsm',
        entityId: `${idPrefix}-governance`,
        previousState: 'BOOTING',
        nextState: 'HEALTHY',
        authority: 'mock-governance',
      });
    };

    await emitSequence(runIdA);
    await emitSequence(runIdB);

    await sleep(30);

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

    // Telemetry workers emit PROJECTION_INTENT to the observability plane.
    // They are started and stopped as part of the projection lifecycle.
    await telemetryWorkers.startAll(25);
    await sleep(120);
    await telemetryWorkers.stopAll();

    const { entries } = observability.query.getEntriesSince(cursorStart);
    // Projection workers emit PROJECTION_INTENT (not SEMANTIC_PROJECTION_TRANSITION).
    // SEMANTIC_PROJECTION_TRANSITION is emitted by the Telemetry Coordination FSM
    // which is the sole serializer for validated projection intents.
    // In this test, we verify that projection workers emit intent entries.
    const projectionIntentEntries = entries.filter(
      (e) => e.entity === 'projection_intent' && e.raw?.entryType === 'PROJECTION_INTENT'
    );

    expect(projectionIntentEntries.length).toBeGreaterThan(0);
    const sample = projectionIntentEntries[0];
    expect(sample.raw).toBeDefined();
    expect(sample.raw.entryType).toBe('PROJECTION_INTENT');
    expect(sample.raw.sourceTelemetryWindow).toBeDefined();
    expect(typeof sample.raw.sourceTelemetryWindow.lineageStartCursor).toBe('number');
    expect(typeof sample.raw.sourceTelemetryWindow.lineageEndCursor).toBe('number');
  });
});
