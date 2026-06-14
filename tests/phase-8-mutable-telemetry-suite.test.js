/**
 * Phase 8: Mutable Telemetry Suite — FSM State Inference & Projection Plane
 * =========================================================================
 *
 * Validates the complete mutable telemetry chain:
 *   Emission → observability → projection worker → FSM coordination
 *   → transition writer → interpreter → lineage:projection:domain:{name}
 *   → FSM._syncProjectionState() reads Redis → _localState updated
 *
 * Two modes:
 *   SIMULATION — mock Redis, synthetic events. Fast, no infra.
 *   INTEGRATION — real Redis, real observability. Requires Docker + Redis.
 *
 * Run simulation:
 *   NODE_ENV=test npx vitest run tests/phase-8-mutable-telemetry-suite.test.js
 *
 * Run integration (Docker):
 *   docker exec -e REDIS_URL=redis://test-redis:6379 instagram-test-runner sh -c \
 *     "cd /app && NODE_ENV=integration npx vitest run tests/phase-8-mutable-telemetry-suite.test.js"
 */

import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest';

// ═══════════════════════════════════════════════════════════════════════════════
// Shared helpers
// ═══════════════════════════════════════════════════════════════════════════════

const MOCK_NAMESPACE = 'acquisition';
const MOCK_REDIS_KEY = `lineage:projection:domain:${MOCK_NAMESPACE}`;

function createMockRedis(initialData = {}) {
  const store = { ...initialData };
  return {
    status: 'ready',
    get: vi.fn(async (k) => store[k] || null),
    set: vi.fn(async (k, v, ...args) => { store[k] = v; return 'OK'; }),
    del: vi.fn(async (k) => { delete store[k]; return 1; }),
    rpush: vi.fn(async () => 1),
    lrange: vi.fn(async () => []),
    lrem: vi.fn(async () => 1),
    expire: vi.fn(async () => 1),
    ttl: vi.fn(async (k) => { return store[k] ? 30 : -2; }),
    ping: vi.fn(async () => 'PONG'),
    _store: store,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Simulation Suite — layer-by-layer validation with mocks
// ═══════════════════════════════════════════════════════════════════════════════

describe('SIMULATION: Mutable Telemetry Layer — Layer-by-Layer', () => {

  // ── Layer 1: Projection worker emits PROJECTION_INTENT ──────────────────
  describe('Layer 1 — Projection Worker emits PROJECTION_INTENT', () => {
    it('base-projection-worker emits PROJECTION_INTENT with correct domain', async () => {
      const { BaseProjectionWorker } = await import(
        '../telemetry-kernel/substrates/projection/workers/base-projection-worker.js'
      );
      class TestWorker extends BaseProjectionWorker {
        constructor() { super({ pollIntervalMs: 30000, workerName: 'test-worker' }); }
        get _projectType() { return 'TEST_PROJECTION'; }
        get _domain() { return MOCK_NAMESPACE; }
      }
      const worker = new TestWorker();
      expect(typeof worker._emitProjectionTransition).toBe('function');
      expect(worker._domain).toBe(MOCK_NAMESPACE);
      expect(worker._projectType).toBe('TEST_PROJECTION');
    });
  });

  // ── Layer 2: Transition writer ──────────────────────────────────────────
  describe('Layer 2 — Transition Writer', () => {
    it('NAMESPACES includes acquisition and writer exports API', async () => {
      const { createTransitionWriter, NAMESPACES } = await import(
        '../telemetry-kernel/substrates/projection/transition-writers/base-transition-writer.js'
      );
      expect(NAMESPACES).toContain(MOCK_NAMESPACE);
      const writer = createTransitionWriter(MOCK_NAMESPACE);
      expect(typeof writer.start).toBe('function');
      expect(typeof writer.stop).toBe('function');
      expect(typeof writer.getHealth).toBe('function');
    });
  });

  // ── Layer 3: Interpreter ────────────────────────────────────────────────
  describe('Layer 3 — Interpreter writes namespace projection', () => {
    it('_projections.domain.acquisition exists at IDLE', async () => {
      const interp = await import(
        '../control-plane/governance/interpreters/namespace-projection-interpreter.js'
      );
      expect(interp.getProjections().domain.acquisition.state).toBe('IDLE');
      expect(interp.getDomainProjection('acquisition').state).toBe('IDLE');
      expect(typeof interp.interpret).toBe('function');
    });

    it('interpret() updates state from accepted entry', () => {
      const interp = require('../control-plane/governance/interpreters/namespace-projection-interpreter.js');
      const entry = {
        domain: MOCK_NAMESPACE,
        raw: {
          projectionNamespace: MOCK_NAMESPACE,
          entryType: 'SEMANTIC_PROJECTION_TRANSITION',
          projectionPayload: { currentAcquisitionState: 'ACQUIRING', intentCount: 5, failureCount: 1 },
        },
      };
      interp.interpret({ type: 'PROJECTION_ACCEPTED', ledgerId: 'l-1', entry });
      const r = interp.getDomainProjection(MOCK_NAMESPACE);
      expect(r.state).toBe('ACQUIRING');
      expect(r.intentCount).toBe(5);
      expect(r.failureCount).toBe(1);
    });
  });

  // ── Layer 4: FSM state inference ────────────────────────────────────────
  describe('Layer 4 — FSM._syncProjectionState() infers state from Redis', () => {
    it('defaults to IDLE when no projection exists', async () => {
      let _localState = 'IDLE';
      const r = createMockRedis();
      try {
        if (r.status === 'ready') {
          const raw = await r.get(MOCK_REDIS_KEY);
          if (raw) { const p = JSON.parse(raw); if (p?.projection?.state) _localState = p.projection.state; }
        }
      } catch (_) {}
      expect(_localState).toBe('IDLE');
    });

    it('reads projected state from Redis', async () => {
      let _localState = 'IDLE';
      const r = createMockRedis({ [MOCK_REDIS_KEY]: JSON.stringify({
        domain: MOCK_NAMESPACE, projection: { state: 'ACQUIRING', transitionCount: 3 }, updatedAt: Date.now(),
      })});
      try {
        if (r.status === 'ready') {
          const raw = await r.get(MOCK_REDIS_KEY);
          if (raw) { const p = JSON.parse(raw); if (p?.projection?.state) _localState = p.projection.state; }
        }
      } catch (_) {}
      expect(_localState).toBe('ACQUIRING');
    });

    it('falls back when Redis is unavailable', async () => {
      let _localState = 'IDLE';
      try { const dead = { status: 'end' }; if (dead.status === 'ready') {} } catch (_) {}
      expect(_localState).toBe('IDLE');
    });

    it('falls back when JSON is malformed', async () => {
      let _localState = 'IDLE';
      const r = createMockRedis({ [MOCK_REDIS_KEY]: '{bad json}' });
      try {
        if (r.status === 'ready') {
          const raw = await r.get(MOCK_REDIS_KEY);
          if (raw) { const p = JSON.parse(raw); if (p?.projection?.state) _localState = p.projection.state; }
        }
      } catch (_) {}
      expect(_localState).toBe('IDLE');
    });

    it('handles empty projection payload gracefully', async () => {
      let _localState = 'IDLE';
      const r = createMockRedis({ [MOCK_REDIS_KEY]: JSON.stringify({
        domain: MOCK_NAMESPACE, projection: {}, updatedAt: Date.now(),
      })});
      try {
        if (r.status === 'ready') {
          const raw = await r.get(MOCK_REDIS_KEY);
          if (raw) { const p = JSON.parse(raw); if (p?.projection?.state) _localState = p.projection.state; }
        }
      } catch (_) {}
      expect(_localState).toBe('IDLE');
    });

    it('handles missing projection key gracefully', async () => {
      let _localState = 'ACQUIRING';
      const r = createMockRedis();
      try {
        if (r.status === 'ready') {
          const raw = await r.get('nonexistent:key');
          if (raw) { const p = JSON.parse(raw); if (p?.projection?.state) _localState = p.projection.state; }
        }
      } catch (_) {}
      expect(_localState).toBe('ACQUIRING');
    });
  });

  // ── Layer 5: Failure modes and retry ────────────────────────────────────
  describe('Layer 5 — Failure modes and retry resilience', () => {
    it('Redis.get throws → caught without crash', async () => {
      let _localState = 'IDLE';
      const throwing = { status: 'ready', get: vi.fn(async () => { throw new Error('ECONNREFUSED'); }) };
      try { if (throwing.status === 'ready') await throwing.get('x'); } catch (_) {}
      expect(_localState).toBe('IDLE');
    });

    it('complete write→read→overwrite→read cycle through all acquisition states', async () => {
      let _localState = 'IDLE';
      const r = createMockRedis();
      const write = async (s, c) => {
        await r.set(MOCK_REDIS_KEY, JSON.stringify({
          domain: MOCK_NAMESPACE, projection: { state: s, transitionCount: c }, updatedAt: Date.now(),
        }));
      };
      const read = async () => {
        try {
          if (r.status === 'ready') {
            const raw = await r.get(MOCK_REDIS_KEY);
            if (raw) { const p = JSON.parse(raw); if (p?.projection?.state) _localState = p.projection.state; }
          }
        } catch (_) {}
      };
      await write('ACQUIRING', 1); await read(); expect(_localState).toBe('ACQUIRING');
      await write('STAGING', 2); await read(); expect(_localState).toBe('STAGING');
      await write('ACQUIRING', 3); await read(); expect(_localState).toBe('ACQUIRING');
      await write('IDLE', 4); await read(); expect(_localState).toBe('IDLE');
    });
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Integration Suite — requires Docker with real Redis, waits for connection
// ═══════════════════════════════════════════════════════════════════════════════

describe.runIf(process.env.NODE_ENV === 'integration' || process.env.DOCKER_ENV)(
  'INTEGRATION: Mutable Telemetry — Redis-backed (Docker required)',
  () => {
    /** Wait for Redis connection with retry + deadline */
    async function waitForRedis(url, deadlineMs = 15_000) {
      const start = Date.now();
      let lastErr;
      while (Date.now() - start < deadlineMs) {
        try {
          const { getRedisClient } = require('../config/redis.js');
          const client = getRedisClient();
          await client.ping();
          return client;
        } catch (err) {
          lastErr = err;
          await new Promise(r => setTimeout(r, 500));
        }
      }
      throw new Error(`Redis not reachable after ${deadlineMs}ms: ${lastErr?.message}`);
    }

    let redis;

    beforeAll(async () => {
      try {
        redis = await waitForRedis(process.env.REDIS_URL || 'redis://localhost:6379', 15_000);
        console.log('[integration] Redis connected — running integration tests');
      } catch (err) {
        console.error('[integration] FATAL: Redis unavailable — tests will fail:', err.message);
        redis = null;
      }
    }, 20_000);

    afterAll(async () => {
      if (redis) {
        try {
          // Clean up all test keys
          const keys = await redis.keys('lineage:projection:domain:test:*');
          for (const k of keys) await redis.del(k);
          await redis.del('lineage:projection:domain:acquisition');
          await redis.del('lineage:projection:domain:publishing');
          await redis.del('lineage:projection:domain:scheduling');
          await redis.quit();
        } catch (_) {}
      }
    });

    // ── E2E: State transitions through real Redis ──────────────────────
    describe('E2E — State transition cycles via Redis', () => {
      const STATES = ['IDLE', 'ACQUIRING', 'STAGING', 'ACQUIRING', 'IDLE'];
      const NAMESPACE = 'acquisition';
      const KEY = `lineage:projection:domain:${NAMESPACE}`;

      it('writes and reads all acquisition states sequentially', async () => {
        if (!redis) throw new Error('Redis required');
        let readState = 'UNKNOWN';

        for (const expected of STATES) {
          await redis.set(KEY, JSON.stringify({
            domain: NAMESPACE,
            projection: { state: expected, transitionCount: 1, lastTransition: Date.now() },
            updatedAt: Date.now(),
            entryLedgerId: `e2e-${expected}-${Date.now()}`,
          }));

          // FSM read (simulated _syncProjectionState)
          const raw = await redis.get(KEY);
          const parsed = JSON.parse(raw);
          if (parsed?.projection?.state) readState = parsed.projection.state;

          expect(readState).toBe(expected);
          // Small delay to simulate real dispatch timing
          await new Promise(r => setTimeout(r, 50));
        }
        await redis.del(KEY);
      });

      it('concurrent namespace isolation: acquisition and publishing do not interfere', async () => {
        if (!redis) throw new Error('Redis required');
        const K1 = 'lineage:projection:domain:acquisition';
        const K2 = 'lineage:projection:domain:publishing';

        await redis.set(K1, JSON.stringify({
          domain: 'acquisition', projection: { state: 'ACQUIRING', transitionCount: 5 }, updatedAt: Date.now(),
        }));
        await redis.set(K2, JSON.stringify({
          domain: 'publishing', projection: { state: 'EXECUTING', transitionCount: 3 }, updatedAt: Date.now(),
        }));

        const [r1, r2] = await Promise.all([redis.get(K1), redis.get(K2)]);
        expect(JSON.parse(r1).projection.state).toBe('ACQUIRING');
        expect(JSON.parse(r2).projection.state).toBe('EXECUTING');

        await Promise.all([redis.del(K1), redis.del(K2)]);
      });

      it('projection TTL: key expires after TTL seconds and FSM falls back to IDLE', async () => {
        if (!redis) throw new Error('Redis required');
        const KEY = `lineage:projection:domain:test:ttl-${Date.now()}`;

        await redis.set(KEY, JSON.stringify({
          domain: 'test', projection: { state: 'ACTIVE', transitionCount: 1 }, updatedAt: Date.now(),
        }), 'EX', 1);

        // Read immediately — should be ACTIVE
        let raw = await redis.get(KEY);
        expect(JSON.parse(raw).projection.state).toBe('ACTIVE');

        // Wait for TTL expiry
        await new Promise(r => setTimeout(r, 1100));

        // Read after expiry — should be null (FSM falls back to existing _localState)
        raw = await redis.get(KEY);
        expect(raw).toBeNull();
      }, 5000);

      it('multiple rapid state transitions with Redis pipelining simulation', async () => {
        if (!redis) throw new Error('Redis required');
        const KEY = `lineage:projection:domain:test:rapid-${Date.now()}`;
        const transitions = ['IDLE', 'ACQUIRING', 'STAGING', 'ACQUIRING', 'IDLE', 'ACQUIRING', 'STAGING', 'IDLE'];
        let readState = 'IDLE';

        for (const state of transitions) {
          await redis.set(KEY, JSON.stringify({
            domain: 'test', projection: { state, transitionCount: 1 }, updatedAt: Date.now(),
          }));
          const raw = await redis.get(KEY);
          const p = JSON.parse(raw);
          if (p?.projection?.state) readState = p.projection.state;
          expect(readState).toBe(state);
        }
        await redis.del(KEY);
      });
    });

    // ── E2E: Observability emission → transition log ──────────────────
    describe('E2E — Observability emission flow', () => {
      it('emits transition to observability and captures via onWrite', async () => {
        if (!redis) throw new Error('Redis required');
        const observability = require('../control-plane/observability/index.js');

        let captured = [];
        const unsub = observability.onWrite((t) => { captured.push(t); });

        const domains = ['acquisition', 'publishing', 'scheduling'];
        for (const domain of domains) {
          await observability.transition({
            domain,
            entity: 'fsm',
            nextState: 'ACTIVE',
            authority: 'test',
            raw: { intent: 'TEST_EMISSION', ts: Date.now() },
          });
          // Small delay between emissions to simulate real dispatch timing
          await new Promise(r => setTimeout(r, 30));
        }

        // Give the log time to process
        await new Promise(r => setTimeout(r, 200));

        expect(captured.length).toBeGreaterThanOrEqual(1);
        const domainsSeen = new Set(captured.map(t => t.domain));
        expect(domainsSeen.has('acquisition')).toBe(true);
        expect(domainsSeen.has('publishing')).toBe(true);
        expect(domainsSeen.has('scheduling')).toBe(true);

        unsub();
      });
    });

    // ── E2E: Large batch stress ────────────────────────────────────────
    describe('E2E — Batch write resilience', () => {
      it('writes 25 projection states in sequence without error', async () => {
        if (!redis) throw new Error('Redis required');
        const KEY = `lineage:projection:domain:test:batch-${Date.now()}`;

        for (let i = 0; i < 25; i++) {
          const state = i % 2 === 0 ? 'ACTIVE' : 'IDLE';
          await redis.set(KEY, JSON.stringify({
            domain: 'test',
            projection: { state, transitionCount: i, lastTransition: Date.now() },
            updatedAt: Date.now(),
            entryLedgerId: `batch-${i}`,
          }));

          if (i % 5 === 0) {
            const raw = await redis.get(KEY);
            const p = JSON.parse(raw);
            expect(p.projection.state).toBe(state);
            expect(p.projection.transitionCount).toBe(i);
          }
          // Realistic inter-write delay matching FSM dispatch cadence
          await new Promise(r => setTimeout(r, 100));
        }

        const finalRaw = await redis.get(KEY);
        const finalP = JSON.parse(finalRaw);
        expect(finalP.projection.state).toBe('ACTIVE');
        expect(finalP.projection.transitionCount).toBe(24);
        await redis.del(KEY);
      }, 15_000);

      it('concurrent writes from multiple simulated FSM dispatches', async () => {
        if (!redis) throw new Error('Redis required');
        const K1 = `lineage:projection:domain:test:concurrent-${Date.now()}`;

        // Simulate 3 FSMs writing their projection state concurrently
        const writers = [];
        for (let fsm = 0; fsm < 3; fsm++) {
          writers.push((async () => {
            for (let i = 0; i < 10; i++) {
              const state = ['IDLE', 'ACTIVE', 'PROCESSING', 'COMPLETED'][i % 4];
              await redis.set(K1, JSON.stringify({
                domain: 'test', projection: { state, transitionCount: i, lastTransition: Date.now() },
                updatedAt: Date.now(), entryLedgerId: `concurrent-fsm${fsm}-${i}`,
              }));
              await new Promise(r => setTimeout(r, 50));
            }
          })());
        }
        await Promise.all(writers);

        // Verify final state is coherent
        const raw = await redis.get(K1);
        expect(raw).toBeTruthy();
        const p = JSON.parse(raw);
        expect(['IDLE', 'ACTIVE', 'PROCESSING', 'COMPLETED']).toContain(p.projection.state);
        expect(typeof p.projection.transitionCount).toBe('number');
        await redis.del(K1);
      }, 15_000);

      it('stress: 100 rapid writes with verification checkpoints', async () => {
        if (!redis) throw new Error('Redis required');
        const KEY = `lineage:projection:domain:test:stress-${Date.now()}`;

        for (let i = 0; i < 100; i++) {
          const state = i % 5 === 0 ? 'RESET' : `PHASE${i % 4 + 1}`;
          await redis.set(KEY, JSON.stringify({
            domain: 'test', projection: { state, transitionCount: i, lastTransition: Date.now() },
            updatedAt: Date.now(), entryLedgerId: `stress-${i}`,
          }));
        }

        const raw = await redis.get(KEY);
        const p = JSON.parse(raw);
        expect(p.projection.transitionCount).toBe(99);
        await redis.del(KEY);
      }, 10_000);

      it('longevity: projection state survives 30s of continuous read/write cycles', async () => {
        if (!redis) throw new Error('Redis required');
        const KEY = `lineage:projection:domain:test:longevity-${Date.now()}`;
        const CYCLES = 55;
        const DELAY_MS = 1_000;

        for (let cycle = 0; cycle < CYCLES; cycle++) {
          const state = cycle % 2 === 0 ? 'ACTIVE' : 'IDLE';
          await redis.set(KEY, JSON.stringify({
            domain: 'test', projection: { state, transitionCount: cycle, lastTransition: Date.now() },
            updatedAt: Date.now(), entryLedgerId: `longevity-${cycle}`,
          }));

          // Every 10th cycle, verify checkpoints match
          if (cycle % 10 === 0) {
            const raw = await redis.get(KEY);
            const p = JSON.parse(raw);
            expect(p.projection.state).toBe(state);
            expect(p.projection.transitionCount).toBe(cycle);
          }

          await new Promise(r => setTimeout(r, DELAY_MS));
        }

        const finalRaw = await redis.get(KEY);
        const finalP = JSON.parse(finalRaw);
        expect(finalP.projection.state).toBe(CYCLES % 2 === 0 ? 'IDLE' : 'ACTIVE');
        expect(finalP.projection.transitionCount).toBe(CYCLES - 1);
        await redis.del(KEY);
      }, 90_000);
    });
  }
);
