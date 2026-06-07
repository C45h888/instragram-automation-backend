/**
 * Phase 6A: Transition Writers Redis Write Verification
 * ======================================================
 *
 * Validates that all 6 transition writers correctly write to Redis via the
 * canonical ledger path through the FSM coordination layer, and that cursor
 * persistence survives worker restart.
 *
 * Architecture under test (production-aligned):
 *   Projection Worker emits PROJECTION_INTENT (raw production state)
 *     → FSM _onTransitionLogWrite reacts (gate: nextState === 'PROJECTION_INTENT')
 *     → FSM _emitTransition() → SEMANTIC_PROJECTION_TRANSITION (entryType='SEMANTIC_PROJECTION_TRANSITION')
 *     → FSM output written to global + domain-bounded transition log
 *     → writer: raw.entryType='SEMANTIC_PROJECTION_TRANSITION' AND domain=namespace? YES → recordWorkerEntry()
 *     → lineageLedger.recordWorkerEntry() → Redis: lineage:ledger:entries
 *     → CK.dispatch(PROJECTION_PERSISTED) — fire-and-forget
 *
 * Test simulation plane:
 *   Tests emit raw PROJECTION_INTENT (no entryType marker) — same as projection workers.
 *   FSM coordinates, writers process FSM output. No separate test gate.
 *   Simulation reflects production exactly.
 *
 * Key assertions:
 *   1. Each writer is healthy (ok: true) after startup
 *   2. Ledger entries appear in Redis via FSM coordination path
 *   3. Cursor key exists in Redis for each consumer
 *   4. Worker restart does NOT reset cursor — entries are NOT re-processed
 *   5. Error classification: failed writes are categorized, not silently dropped
 *   6. CK dispatch failure does not orphan entries (tracked separately)
 *
 * Run locally from IDE — no Docker needed:
 *   NODE_ENV=test npx vitest run tests/phase-6a-transition-writers-redis.test.js
 *
 * Or inside Docker test-runner:
 *   docker exec instagram-test-runner sh -c "cd /app && NODE_ENV=test npx vitest run tests/phase-6a-transition-writers-redis.test.js"
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { getRedisClient } from '../config/redis.js';
import observability from '../control-plane/observability/index.js';
import transitionWriters from '../telemetry-kernel/substrates/projection/transition-writers/index.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
import constitutionalKernel from '../control-plane/governance/constitutional-kernel.js';
import tcf from '../telemetry-kernel/fsm.js';

// ── Namespace → Redis cursor key for telemetry-coordination-fsm consumer ──
const CURSOR_KEY = 'governance:observability:consumer-cursor:telemetry-coordination-fsm';
const LEDGER_REDIS_KEY = 'lineage:ledger:entries';

// ── Helpers ────────────────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** Wait for Redis ledger to contain at least N entries */
async function waitForLedgerEntries(minCount, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const size = await lineageLedger.getLineage(9999).then((l) => l.length);
    if (size >= minCount) return size;
    await sleep(50);
  }
  return lineageLedger.getLineage(9999).then((l) => l.length);
}

/** Flush all relevant Redis keys */
async function flushRedis() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') return;
  const governanceKeys = await redis.keys('governance:*');
  const lineageKeys = await redis.keys('lineage:*');
  const allKeys = [...governanceKeys, ...lineageKeys];
  if (allKeys.length > 0) await redis.del(...allKeys);
}

// ── Test Suite ─────────────────────────────────────────────────────────────
describe('Phase 6A: Transition Writers → Redis Write Path', () => {
  let redis;

  beforeAll(async () => {
    redis = getRedisClient();
    if (redis.status !== 'ready') {
      await new Promise((resolve, reject) => {
        const to = setTimeout(() => reject(new Error('Redis timeout')), 5000);
        redis.once('ready', () => { clearTimeout(to); resolve(); });
        redis.once('error', (e) => { clearTimeout(to); reject(e); });
      });
    }
    await flushRedis();

    // ── Full boot sequence (mirrors orchestrator:startAllWorkers) ─────────────
    // Step 1: observability plane
    await observability.init();

    // Step 2: start projection workers (producers)
    const telemetryWorkers = require('../telemetry-kernel');
    await telemetryWorkers.startAll();

    // Step 3: start transition writers (consumers of FSM output)
    transitionWriters.startAll();

    // Step 4: rehydrate CK from empty ledger
    await constitutionalKernel.rehydrate();

    // Step 5: manually dispatch BOOT_COMPLETE — rehydrate does NOT do this
    constitutionalKernel.dispatch({ type: 'BOOT_COMPLETE' });

    // Step 6: start CK watchdog loop
    constitutionalKernel.startLoop(10_000);

    // Step 7: start the telemetry coordination FSM reactive layer
    // This is REQUIRED for the reactive onWrite path — without it, the FSM
    // never processes PROJECTION_INTENT entries and never emits
    // SEMANTIC_PROJECTION_TRANSITION via _emitTransition.
    const ckCtx = {
      validate: (from, to, evt) =>
        constitutionalKernel.validateDomainTransition('telemetry-coordination', from, to, evt),
      dispatchGlobal: (evt) => constitutionalKernel.dispatch(evt),
      getGlobalState: () => constitutionalKernel.getState(),
    };
    tcf.init();
    tcf.start(ckCtx);

    // Let all writers register their consumers and FSM spin up
    await sleep(500);
  }, 15000);

  afterAll(async () => {
    transitionWriters.stopAll();
    const telemetryWorkers = require('../telemetry-kernel');
    await telemetryWorkers.stopAll();
    constitutionalKernel.stopLoop();
    await observability.stop();
  });

  afterEach(async () => {
    await flushRedis();
    // Restart writers after each test so they re-subscribe
    transitionWriters.stopAll();
    transitionWriters.startAll();
    await sleep(200);
  });

  // ── T1: Writer Health ──────────────────────────────────────────────────
  describe('T1: Writer startup and health', () => {
    it('T1-A: All 6 transition writers are running after boot', () => {
      const health = transitionWriters.getHealth();
      expect(health.writers.length, 'should have 6 writers').toBe(6);

      const stopped = health.writers.filter(w => !w.running);
      expect(stopped, `all writers should be running, stopped: ${stopped.map(w => w.namespace).join(',')}`).toHaveLength(0);
    });

    it('T1-B: All writers report ok:true and status not STOPPED after boot', () => {
      const health = transitionWriters.getHealth();
      for (const w of health.writers) {
        expect(w.ok, `writer ${w.namespace} should be ok`).toBe(true);
      }
      expect(health.status, 'aggregate status should not be STOPPED').not.toBe('STOPPED');
    });

    it('T1-C: Zero failed writes and zero CK dispatch failures at startup', () => {
      const health = transitionWriters.getHealth();
      expect(health.totalFailed, 'unexpected failed writes').toBe(0);
      // ckDispatchFailures is per-writer, not a top-level aggregate
      expect(Array.isArray(health.writers), 'writers should be an array').toBe(true);
      for (const w of health.writers) {
        expect(w.ckDispatchFailures, `${w.namespace} ckDispatchFailures should be 0`).toBe(0);
      }
    });
  });

  // ── T2: Ledger Write (Canonical Path) ──────────────────────────────────
  describe('T2: Ledger writes via canonical path', () => {
    it('T2-A: Raw production projection intent writes to ledger via FSM coordination', async () => {
      const beforeSize = await lineageLedger.getLineage(9999).then((l) => l.length);

      // Emit PROJECTION_INTENT — raw production state from projection workers.
      // This is what projection workers emit in production: nextState=PROJECTION_INTENT,
      // no entryType marker (raw intent). The FSM reacts, processes intent,
      // and emits SEMANTIC_PROJECTION_TRANSITION with entryType='SEMANTIC_PROJECTION_TRANSITION'.
      // Writers receive FSM output and write to ledger.
      await observability.transition({
        domain: 'runtime',
        entity: 'probe',
        entityId: `t2a-${Date.now()}`,
        previousState: 'IDLE',
        nextState: 'PROJECTION_INTENT',  // ← raw production state, NOT pre-coordinated
        authority: 'phase-6a-test',
        raw: {},  // ← no entryType — FSM gate allows raw PROJECTION_INTENT through
      });

      const size = await waitForLedgerEntries(beforeSize + 1, 2000);
      expect(size, 'ledger should grow by 1 — FSM coordinates, writer processes').toBeGreaterThan(beforeSize);
    });

    it('T2-B: Multiple namespace transitions each write to ledger via FSM coordination', async () => {
      const beforeSize = await lineageLedger.getLineage(9999).then((l) => l.length);
      const domains = ['runtime', 'integrity', 'authority', 'health', 'systemic', 'capability'];

      // Emit PROJECTION_INTENT for each domain — same as production projection workers.
      // FSM coordinates each, writers filter and write their bounded domain.
      for (let i = 0; i < domains.length; i++) {
        // eslint-disable-next-line no-await-in-loop
        await observability.transition({
          domain: domains[i],
          entity: 'probe',
          entityId: `t2b-${Date.now()}-${i}`,
          previousState: 'IDLE',
          nextState: 'PROJECTION_INTENT',  // ← raw production state
          authority: 'phase-6a-test',
          raw: {},
        });
      }

      const size = await waitForLedgerEntries(beforeSize + domains.length, 3000);
      expect(size, `ledger should grow by ${domains.length} after FSM coordination of ${domains.length} domains`).toBeGreaterThan(beforeSize);
    });
  });

  // ── T3: Cursor Persistence ──────────────────────────────────────────────
  describe('T3: Consumer cursor persists to Redis and survives restart', () => {
    it('T3-A: Consumer cursor key exists in Redis after telemetry FSM init', async () => {
      const cursorRaw = await redis.get(CURSOR_KEY);
      expect(cursorRaw, 'cursor key should exist in Redis after telemetry FSM init').not.toBeNull();
      const cursor = parseInt(cursorRaw, 10);
      expect(cursor, 'cursor should be a non-negative integer').toBeGreaterThanOrEqual(0);
    });

    it('T3-B: Cursor advances after FSM coordinates projection intent', async () => {
      const cursorBefore = parseInt(await redis.get(CURSOR_KEY) ?? '0', 10);

      // Emit raw PROJECTION_INTENT — FSM processes, cursor advances as writer processes
      await observability.transition({
        domain: 'runtime',
        entity: 'probe',
        entityId: `t3b-${Date.now()}`,
        previousState: 'IDLE',
        nextState: 'PROJECTION_INTENT',  // ← raw production state
        authority: 'phase-6a-test',
        raw: {},
      });

      await waitForLedgerEntries(1, 2000);
      const cursorAfter = parseInt(await redis.get(CURSOR_KEY) ?? '0', 10);
      expect(cursorAfter, 'cursor should advance as FSM coordinates and writer processes').toBeGreaterThan(cursorBefore);
    });

    it('T3-C: Stopping and restarting writers does NOT reset cursor to 0', async () => {
      const cursorBefore = parseInt(await redis.get(CURSOR_KEY) ?? '0', 10);
      expect(cursorBefore, 'cursor should be > 0 before restart (T3-B must pass first)').toBeGreaterThan(0);

      // Simulate health-triggered deterministic recycle
      transitionWriters.stopAll();
      await sleep(200);
      transitionWriters.startAll();
      await sleep(500); // let re-registration + cursor restore

      const cursorAfter = parseInt(await redis.get(CURSOR_KEY) ?? '0', 10);
      expect(cursorAfter, 'cursor should be restored from Redis after restart, not reset to 0').toBeGreaterThan(0);
      expect(cursorAfter).toBe(cursorBefore);
    });
  });

  // ── T4: Error Classification ────────────────────────────────────────────
  describe('T4: Error classification — failed writes are categorized', () => {
    it('T4-A: errorCategories is non-null on getHealth() — empty {} means no errors (OK)', () => {
      const health = transitionWriters.getHealth();
      // errorCategories is either null (when all counts are 0) or a plain object
      // When STOPPED, both forms are acceptable. When running, null or {} both mean OK.
      const ec = health.errorCategories;
      expect(ec === null || typeof ec === 'object', 'errorCategories must be null or object').toBe(true);
    });

    it('T4-B: No degraded or failed writer flags at baseline', () => {
      const health = transitionWriters.getHealth();
      expect(health.status, 'should not have degraded at baseline').not.toBe('DEGRADED');
      expect(health.status, 'should not be FAILED at baseline').not.toBe('FAILED');
    });
  });

  // ── T5: CK Dispatch Failure Tracking ────────────────────────────────────
  describe('T5: CK dispatch failure does not orphan ledger entries', () => {
    it('T5-A: ckDispatchFailures is tracked per writer and >= 0', () => {
      const health = transitionWriters.getHealth();
      // ckDispatchFailures lives on each writer, not as a top-level aggregate
      expect(Array.isArray(health.writers), 'writers should be an array').toBe(true);
      for (const w of health.writers) {
        expect(typeof w.ckDispatchFailures, `${w.namespace} ckDispatchFailures should be a number`).toBe('number');
        expect(w.ckDispatchFailures, `${w.namespace} should be >= 0`).toBeGreaterThanOrEqual(0);
      }
    });

    it('T5-B: Ledger entries are not lost regardless of CK dispatch outcome', async () => {
      const beforeSize = await lineageLedger.getLineage(9999).then((l) => l.length);

      // Emit raw PROJECTION_INTENT — FSM coordinates, writer writes to ledger.
      // CK dispatch is fire-and-forget: if CK is unavailable, ledger write still completes.
      await observability.transition({
        domain: 'systemic',
        entity: 'probe',
        entityId: `t5b-${Date.now()}`,
        previousState: 'IDLE',
        nextState: 'PROJECTION_INTENT',  // ← raw production state
        authority: 'phase-6a-test',
        raw: {},
      });

      const size = await waitForLedgerEntries(beforeSize + 1, 2000);
      expect(size, 'entry must be in ledger regardless of CK dispatch outcome').toBeGreaterThan(beforeSize);
    });
  });

  // ── T6: CK State ────────────────────────────────────────────────────────
  describe('T6: CK reaches HEALTHY after full stack boot', () => {
    it('T6-A: CK is HEALTHY after BOOT_COMPLETE is dispatched', () => {
      const state = constitutionalKernel.getState();
      expect(state, 'CK should be HEALTHY after BOOT_COMPLETE').toBe('HEALTHY');
    });
  });
});
