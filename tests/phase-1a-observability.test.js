// ============================================
// Phase 1A: Runtime Observability Validation
// ============================================
// Purpose: Validate that the observability plane
// is functioning and can emit/record state transitions.
// This is the foundational layer for all subsequent phases.
//
// Tests:
// 1. Redis connection is healthy
// 2. Observability plane can emit transitions
// 3. Transition projection stores state correctly
// 4. Lineage tracing infrastructure is accessible
// ============================================

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { getRedisClient } from '../config/redis.js';
import observability from '../control-plane/observability/index.js';

describe('Phase 1A: Runtime Observability Infrastructure', () => {
  let redis;

  beforeAll(async () => {
    redis = getRedisClient();
    // Wait for Redis to be ready
    if (redis.status !== 'ready') {
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Redis connection timeout')), 5000);
        redis.once('ready', () => { clearTimeout(timeout); resolve(); });
        redis.once('error', (err) => { clearTimeout(timeout); reject(err); });
      });
    }
    // Initialize observability plane
    await observability.init();
  }, 10000);

  afterAll(async () => {
    await observability.stop();
  });

  describe('Substrate Layer - Redis Health', () => {
    it('should connect to ephemeral Redis and respond to PING', async () => {
      const pong = await redis.ping();
      expect(pong).toBe('PONG');
    });

    it('should report Redis connection status as ready', () => {
      expect(redis.status).toBe('ready');
    });
  });

  describe('Observability Plane - Transition Emission', () => {
    it('should emit a state transition and project it', async () => {
      const testIntentId = `test-obs-001-${Date.now()}`;

      // Note: entityId must be passed via raw.intentId due to normalizer override rule
      observability.transition({
        domain: 'execution',
        entity: 'attempt',
        previousState: 'PENDING',
        nextState: 'ATTEMPTING',
        authority: 'phase-1a-test',
        raw: { intentId: testIntentId, test: true },
      });

      // Allow time for projection
      await new Promise((r) => setTimeout(r, 100));

      // Verify the state was projected (entityId comes from raw.intentId via normalizer)
      const state = observability.query.getState('execution', 'attempt', testIntentId);
      expect(state).toBe('ATTEMPTING');
    });

    it('should record state transitions in the transition log', async () => {
      const testIntentId = `test-log-001-${Date.now()}`;

      observability.transition({
        domain: 'publishing',
        entity: 'pipeline',
        entityId: testIntentId,
        previousState: 'IDLE',
        nextState: 'RUNNING',
        authority: 'phase-1a-test',
      });

      await new Promise((r) => setTimeout(r, 100));

      const log = observability.query.getTransitionLog('publishing', 'pipeline', testIntentId, 5);
      expect(log.length).toBeGreaterThan(0);
      expect(log[log.length - 1].nextState).toBe('RUNNING');
    });

    it('should track FSM states within a domain', async () => {
      const testFsmId = `test-fsm-domain-001-${Date.now()}`;

      observability.transition({
        domain: 'governance',
        entity: 'fsm',
        entityId: testFsmId,
        previousState: 'BOOTING',
        nextState: 'HEALTHY',
        authority: 'phase-1a-test',
      });

      await new Promise((r) => setTimeout(r, 100));

      const domainState = observability.query.getDomainState('governance');
      expect(domainState.fsm).toBeDefined();
      expect(domainState.fsm[testFsmId]).toBe('HEALTHY');
    });
  });

  describe('Observability Plane - Full Snapshot', () => {
    it('should generate a full snapshot of all projected state', async () => {
      const snapshot = observability.query.getFullSnapshot();

      expect(snapshot).toBeDefined();
      expect(snapshot.domains).toBeDefined();
      expect(snapshot.globalStateIndex).toBeDefined();
      expect(snapshot.transitionCount).toBeGreaterThan(0);
    });

    it('should provide log size for cursor-based consumption', () => {
      const logSize = observability.query.getLogSize();
      expect(typeof logSize).toBe('number');
      expect(logSize).toBeGreaterThan(0);
    });
  });

  describe('Lineage Infrastructure', () => {
    it('should store lineage markers in Redis', async () => {
      const lineageKey = `test:lineage:marker:${Date.now()}`;
      const lineageValue = JSON.stringify({
        domain: 'acquisition',
        sequence: 1,
        timestamp: Date.now(),
      });

      await redis.set(lineageKey, lineageValue);
      const retrieved = await redis.get(lineageKey);

      expect(JSON.parse(retrieved)).toEqual(JSON.parse(lineageValue));
    });

    it('should track sequence ordering for lineage continuity', async () => {
      const seqKey = 'test:lineage:sequence';
      const seq1 = await redis.incr(seqKey);
      const seq2 = await redis.incr(seqKey);

      expect(seq2).toBe(seq1 + 1);
    });
  });

  describe('Membrane Validation Logging', () => {
    it('should log membrane transitions through observability', async () => {
      const membraneId = `test-membrane-${Date.now()}`;

      observability.transition({
        domain: 'governance',
        entity: 'membrane',
        entityId: membraneId,
        previousState: 'PASSIVE',
        nextState: 'ACTIVE',
        authority: 'orchestration-membrane',
      });

      await new Promise((r) => setTimeout(r, 100));

      const state = observability.query.getState('governance', 'membrane', membraneId);
      expect(state).toBe('ACTIVE');
    });
  });
});
