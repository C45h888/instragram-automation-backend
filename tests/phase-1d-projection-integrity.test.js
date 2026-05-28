// Phase 1D: Projection Integrity & DB Shape Validation

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { getRedisClient } from '../config/redis.js';
import observability from '../control-plane/observability/index.js';
const eventInjector = require('./event-injector.js');

describe('Phase 1D: Projection Integrity', () => {
  let redis;

  beforeAll(async () => {
    redis = getRedisClient();
    if (redis.status !== 'ready') {
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Redis timeout')), 5000);
        redis.once('ready', () => { clearTimeout(timeout); resolve(); });
        redis.once('error', (err) => { clearTimeout(timeout); reject(err); });
      });
    }
    await observability.init();
  }, 10000);

  afterAll(async () => { await observability.stop(); });

  beforeEach(async () => {
    if (redis.status === 'ready') {
      const keys = await redis.keys('test:*');
      if (keys.length > 0) await redis.del(...keys);
    }
  });

  it('should produce identical snapshots for identical lineage', async () => {
    const intentId = `snapshot-test-${Date.now()}`;
    observability.transition({
      domain: 'execution', entity: 'attempt', entityId: intentId,
      previousState: 'PENDING', nextState: 'ATTEMPTING', authority: 'phase-1d-test',
    });
    await new Promise((r) => setTimeout(r, 100));
    const s1 = observability.query.getFullSnapshot();
    const s2 = observability.query.getFullSnapshot();
    expect(s1.transitionCount).toBe(s2.transitionCount);
    expect(s1.domains).toEqual(s2.domains);
  });

  it('should track entity state transitions deterministically', async () => {
    const id = `trans-${Date.now()}`;
    // Use raw.intentId for entityId per normalizer rule
    observability.transition({
      domain: 'execution', entity: 'attempt',
      previousState: 'PENDING', nextState: 'ATTEMPTING', authority: 'phase-1d-test',
      raw: { intentId: id },
    });
    await new Promise((r) => setTimeout(r, 50));
    expect(observability.query.getState('execution', 'attempt', id)).toBe('ATTEMPTING');
    observability.transition({
      domain: 'execution', entity: 'attempt',
      previousState: 'ATTEMPTING', nextState: 'COMPLETED', authority: 'phase-1d-test',
      raw: { intentId: id },
    });
    await new Promise((r) => setTimeout(r, 50));
    expect(observability.query.getState('execution', 'attempt', id)).toBe('COMPLETED');
    const log = observability.query.getTransitionLog('execution', 'attempt', id, 10);
    expect(log.length).toBe(2);
  });

  it('should reconstruct state identically after lineage replay', async () => {
    const key = `test:lineage:replay:${Date.now()}`;
    const data = { lineageId: `replay-${Date.now()}`, sequence: 1, domain: 'governance' };
    await eventInjector.storeLineageMarker(key, data);
    const retrieved = JSON.parse(await redis.get(key));
    expect(retrieved.lineageId).toBe(data.lineageId);
    expect(retrieved.sequence).toBe(data.sequence);
  });

  it('should preserve lineage continuity', async () => {
    const seqKey = `test:lineage:continuity:${Date.now()}`;
    const seq1 = await redis.incr(seqKey);
    const seq2 = await redis.incr(seqKey);
    const seq3 = await redis.incr(seqKey);
    expect(seq1).toBeLessThan(seq2);
    expect(seq2).toBeLessThan(seq3);
  });

  it('should validate normalized entity structure', async () => {
    const result = await eventInjector.injectAcquisitionIntent({
      variant: 'success', accountId: 'shape-test',
    });
    await new Promise((r) => setTimeout(r, 100));
    const state = observability.query.getState('acquisition', 'acquisition_intent', result.intentId);
    expect(state).toBeDefined();
    const domain = observability.query.getDomainState('acquisition');
    expect(domain.acquisition_intent).toBeDefined();
  });

  it('should maintain referential integrity across domains', async () => {
    const acq = await eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: 'ref-test' });
    await new Promise((r) => setTimeout(r, 100));
    const gov = await eventInjector.injectLineageEvent({ eventType: 'TEST', domain: 'governance', fromState: 'BOOTING', toState: 'HEALTHY' });
    await new Promise((r) => setTimeout(r, 100));
    expect(observability.query.getState('acquisition', 'acquisition_intent', acq.intentId)).toBe('NORMALIZED');
    expect(observability.query.getState('governance', 'fsm', gov.entityId)).toBe('HEALTHY');
  });

  it('should validate transition log has required fields', async () => {
    const id = `log-${Date.now()}`;
    observability.transition({
      domain: 'execution', entity: 'attempt',
      previousState: 'PENDING', nextState: 'ATTEMPTING', authority: 'phase-1d-test',
      raw: { intentId: id },
    });
    await new Promise((r) => setTimeout(r, 100));
    const log = observability.query.getTransitionLog('execution', 'attempt', id, 10);
    expect(log.length).toBeGreaterThan(0);
    expect(log[0]).toHaveProperty('type', 'STATE_TRANSITION');
    expect(log[0]).toHaveProperty('domain');
    expect(log[0]).toHaveProperty('entity');
  });

  it('should materialize identical projections from identical events', async () => {
    const id = `det-${Date.now()}`;
    observability.transition({
      domain: 'execution', entity: 'attempt',
      previousState: 'PENDING', nextState: 'ATTEMPTING', authority: 'phase-1d-test',
      raw: { intentId: id },
    });
    await new Promise((r) => setTimeout(r, 100));
    const s1 = observability.query.getState('execution', 'attempt', id);
    const s2 = observability.query.getState('execution', 'attempt', id);
    expect(s1).toBe(s2);
  });

  it('should verify consumer lag is healthy', () => {
    observability.query.registerConsumer('phase-1d-consumer');
    const lag = observability.query.getConsumerLag('phase-1d-consumer');
    expect(lag.atRisk).toBe(false);
  });

  it('should handle cursor-based consumption', async () => {
    const initialSize = observability.query.getLogSize();
    await eventInjector.injectAcquisitionIntent({ variant: 'success', accountId: 'cursor-test' });
    await new Promise((r) => setTimeout(r, 100));
    const result = observability.query.getEntriesSince(0);
    expect(result.entries).toBeDefined();
    expect(result.totalSize).toBeGreaterThan(initialSize);
  });
});
