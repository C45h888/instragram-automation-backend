import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';

describe('Phase 1C: Constitutional Resilience Verification', () => {
  beforeAll(async () => {
    await observability.init();
  }, 10000);

  afterAll(async () => {
    await observability.stop();
  });

  it('rejects malformed transition input from mutating projected state', async () => {
    const entityId = `malformed-guard-${Date.now()}`;
    const cursorStart = observability.query.getLogSize();

    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'BOOTING',
      nextState: 'HEALTHY',
      authority: 'phase-1c-resilience-test',
    });

    // Invalid event (missing nextState): should no-op
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'HEALTHY',
      authority: 'phase-1c-resilience-test',
      raw: { malformed: true },
    });

    const state = observability.query.getState('governance', 'fsm', entityId);
    expect(state).toBe('HEALTHY');

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const entityEntries = entries.filter(
      (e) => e.domain === 'governance' && e.entity === 'fsm' && e.entityId === entityId
    );
    expect(entityEntries.length).toBe(1);
  });

  it('preserves lawful progression under duplicate transition injection', async () => {
    const entityId = `dedup-lawful-${Date.now()}`;
    const cursorStart = observability.query.getLogSize();

    observability.transition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId,
      previousState: null,
      nextState: 'RECEIVED',
      authority: 'mock-substrate',
    });
    observability.transition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId,
      previousState: 'RECEIVED',
      nextState: 'NORMALIZED',
      authority: 'normalization-layer',
    });
    observability.transition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId,
      previousState: 'RECEIVED',
      nextState: 'NORMALIZED',
      authority: 'normalization-layer',
      raw: { duplicate: true },
    });

    const state = observability.query.getState('acquisition', 'acquisition_intent', entityId);
    expect(state).toBe('NORMALIZED');

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const entityEntries = entries.filter(
      (e) => e.domain === 'acquisition' && e.entity === 'acquisition_intent' && e.entityId === entityId
    );
    expect(entityEntries.length).toBe(3);
    expect(entityEntries[entityEntries.length - 1].nextState).toBe('NORMALIZED');
  });

  it('prevents stale-style regression from mutating canonical state backward', async () => {
    const entityId = `stale-guard-${Date.now()}`;

    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'BOOTING',
      nextState: 'HEALTHY',
      authority: 'governance-fsm',
    });

    // Simulated stale packet: illegal/no-op shape
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'HEALTHY',
      authority: 'stale-substrate-packet',
      raw: { stale: true },
    });

    const state = observability.query.getState('governance', 'fsm', entityId);
    expect(state).toBe('HEALTHY');
  });
});
