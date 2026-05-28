import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
const publishingFsm = require('../control-plane/governance/domains/publishing-fsm');

function signatureFromEntries(entries, prefix) {
  return entries
    .filter((e) => e.entityId && String(e.entityId).startsWith(prefix))
    .map((e) => `${e.domain}|${e.entity}|${e.previousState}|${e.nextState}|${e.authority}`);
}

describe('Phase 1C+: Replay, Repair, and Boundary Legality', () => {
  beforeAll(async () => {
    await observability.init();
    publishingFsm.init('IDLE');
  }, 10000);

  afterAll(async () => {
    await observability.stop();
  });

  it('handles ordering corruption by blocking illegal transition and recovering lawful flow', async () => {
    publishingFsm.init('IDLE');

    // Corrupted order: EMISSION_OBSERVATION before evaluation/emitting should be rejected.
    const blocked = publishingFsm.dispatch({ type: 'EMISSION_OBSERVATION', status: 'ok' }, {
      validate: () => ({ allowed: true }),
    });
    expect(blocked.allowed).toBe(false);
    expect(publishingFsm.getState()).toBe('IDLE');

    // Lawful repair path
    const step1 = publishingFsm.dispatch({ type: 'BUFFER_EVENT_INGESTED', accountId: 'repair-a' }, {
      validate: () => ({ allowed: true }),
    });
    const step2 = publishingFsm.dispatch({ type: 'BUFFER_FLUSH_READY', accountId: 'repair-a', events: [{ id: 1 }] }, {
      validate: () => ({ allowed: true }),
    });
    const step3 = publishingFsm.dispatch({ type: 'EMISSION_OBSERVATION', status: 'ok', metadata: {} }, {
      validate: () => ({ allowed: true }),
    });

    expect(step1.allowed).toBe(true);
    expect(step2.allowed).toBe(true);
    expect(step3.allowed).toBe(true);
    expect(publishingFsm.getState()).toBe('IDLE');
  });

  it('proves replay equality: identical event sequences yield identical projection signatures', async () => {
    const cursorStart = observability.query.getLogSize();
    const runA = `replay-a-${Date.now()}`;
    const runB = `replay-b-${Date.now()}`;

    const emitSeq = (prefix) => {
      observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId: `${prefix}-intent`,
        previousState: null,
        nextState: 'RECEIVED',
        authority: 'replay-test',
      });
      observability.transition({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId: `${prefix}-intent`,
        previousState: 'RECEIVED',
        nextState: 'NORMALIZED',
        authority: 'replay-test',
      });
      observability.transition({
        domain: 'governance',
        entity: 'fsm',
        entityId: `${prefix}-fsm`,
        previousState: 'BOOTING',
        nextState: 'HEALTHY',
        authority: 'replay-test',
      });
    };

    emitSeq(runA);
    emitSeq(runB);

    const { entries } = observability.query.getEntriesSince(cursorStart);
    const seqA = signatureFromEntries(entries, runA);
    const seqB = signatureFromEntries(entries, runB);

    expect(seqA.length).toBe(3);
    expect(seqB.length).toBe(3);
    expect(seqA).toEqual(seqB);
  });

  it('rejects governance boundary abuse on DB_SCAN_EMITTED target', async () => {
    publishingFsm.init('IDLE');

    const malicious = publishingFsm.dispatch({
      type: 'DB_SCAN_EMITTED',
      target: 'users',
      recordId: 'abc-123',
      accountId: 'attacker',
    }, {
      validate: () => ({ allowed: true }),
    });

    expect(malicious.allowed).toBe(false);
    expect(String(malicious.reason || '')).toContain('unknown target');
    expect(publishingFsm.getState()).toBe('IDLE');
  });

  it('enforces constitutional membrane validate() veto even for syntactically valid events', async () => {
    publishingFsm.init('IDLE');

    const denied = publishingFsm.dispatch({
      type: 'BUFFER_EVENT_INGESTED',
      accountId: 'blocked-account',
    }, {
      validate: () => ({ allowed: false, reason: 'membrane policy veto' }),
    });

    expect(denied.allowed).toBe(false);
    expect(denied.reason).toContain('veto');
    expect(publishingFsm.getState()).toBe('IDLE');
  });
});
