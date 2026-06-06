/**
 * Phase-0: Architecture Validation Tests
 * ========================================
 *
 * Fast diagnostic tests (< 30s each) to isolate failure points in the
 * projection worker → transition log → transition writer chain.
 *
 * No simulator, no full boot, no soak. Each test targets one layer.
 *
 * Execution order:
 *   A1 → A2 → A3  (worker layer)
 *   B1 → B2 → B3  (writer layer)
 *   C1 → C2       (FSM reactive path)
 *
 * Total runtime target: < 60 seconds.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';

// ── Module loading ────────────────────────────────────────────────────────────
// Paths relative to /app/tests/ → ../ goes to /app/

import * as observability from '../control-plane/observability/index.js';
import * as lineageLedger from '../control-plane/governance/lineage-ledger.js';
import * as telemetryKernel from '../telemetry-kernel/index.js';
import * as transitionWriters from '../telemetry-kernel/substrates/projection/transition-writers/index.js';
import * as configRedis from '../config/redis.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

async function flushRedisKeys(pattern) {
  const redis = configRedis.getRedisClient();
  if (!redis || redis.status !== 'ready') return;
  try {
    const keys = await redis.keys(pattern);
    if (keys.length > 0) await redis.del(...keys);
  } catch (_) {}
}

async function flushAllTestKeys() {
  await flushRedisKeys('governance:*');
  await flushRedisKeys('lineage:transitionLog:*');
  await flushRedisKeys('lineage:ledger:*');
}

async function countLedgerEntries() {
  const redis = configRedis.getRedisClient();
  if (!redis || redis.status !== 'ready') return 0;
  const keys = await redis.keys('lineage:ledger:*');
  return keys.length;
}

async function readDomainPartition(domain, start = 0, count = -1) {
  const redis = configRedis.getRedisClient();
  if (!redis || redis.status !== 'ready') return [];
  const key = `lineage:transitionLog:domain:${domain}`;
  const end = count === -1 ? -1 : start + count - 1;
  const items = await redis.lrange(key, start, end);
  return items.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
}

// ── Phase A: Worker Layer ────────────────────────────────────────────────────

describe('PHASE A — Projection Worker Layer', () => {

  beforeAll(async () => {
    await flushAllTestKeys();
    await observability.init();
    // Start projection workers via the canonical substrate API
    telemetryKernel.startProjections(20); // 20ms poll interval
  });

  afterAll(async () => {
    telemetryKernel.stopProjections();
    await observability.stop();
  });

  // ── A1: Single worker emits PROJECTION_INTENT ─────────────────────────────

  it('A1 — projection worker emits PROJECTION_INTENT to domain partition', async () => {
    await flushAllTestKeys();

    // Workers auto-emit on poll interval — wait for at least one cycle
    await new Promise(r => setTimeout(r, 80));

    // Query the runtime domain partition
    const entries = await readDomainPartition('runtime');

    expect(entries.length).toBeGreaterThan(0),
      `Expected ≥1 runtime entries, got ${entries.length}`;

    const hasProjectionIntent = entries.some(e => e.nextState === 'PROJECTION_INTENT');
    expect(hasProjectionIntent).toBe(true),
      'Entry must have nextState === PROJECTION_INTENT';

    const intent = entries.find(e => e.nextState === 'PROJECTION_INTENT');
    expect(intent?.authority).toBeTruthy();
  });

  // ── A2: All 5 workers emit independently ─────────────────────────────────

  it('A2 — all 5 projection workers emit to their respective domain partitions', async () => {
    await flushAllTestKeys();

    await new Promise(r => setTimeout(r, 100));

    const domains = ['runtime', 'integrity', 'authority', 'health', 'systemic'];
    const results = await Promise.all(domains.map(d => readDomainPartition(d)));

    for (let i = 0; i < domains.length; i++) {
      expect(results[i].length).toBeGreaterThan(0),
        `domain ${domains[i]} has no entries`;
    }

    // Workers must NOT emit FSM output (SEMANTIC_PROJECTION_TRANSITION)
    for (let i = 0; i < domains.length; i++) {
      const hasFsmOutput = results[i].some(
        e => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
      );
      expect(hasFsmOutput).toBe(false),
        `${domains[i]} contains FSM output — workers must not emit FSM transitions`;
    }
  });

  // ── A3: Workers do NOT write to lineage ledger directly ─────────────────

  it('A3 — workers write PROJECTION_INTENT to transition log, NOT to lineage ledger', async () => {
    await flushAllTestKeys();

    await new Promise(r => setTimeout(r, 100));

    // Workers wrote to domain partition
    const runtimeEntries = await readDomainPartition('runtime');
    expect(runtimeEntries.length).toBeGreaterThan(0),
      'Workers must write PROJECTION_INTENT to transition log';

    // But lineage ledger should have zero entries (workers don't write to ledger)
    const ledgerCount = await countLedgerEntries();
    expect(ledgerCount).toBe(0),
      'Workers must not write directly to lineage:ledger:* — only FSM does via transition-writers';
  });

});

// ── Phase B: Transition Writer Layer ─────────────────────────────────────────

describe('PHASE B — Transition Writer Layer', () => {

  beforeAll(async () => {
    // Phase A's afterAll closed Redis and cleared _transitionLog via stop().
    // Create fresh Redis client and wait for ready state.
    configRedis.getRedisClient();
    await configRedis.awaitRedisReady();
    await flushAllTestKeys();

    // Hard reset: clear ALL in-memory projection state from Phase A.
    // This prevents Phase A entries (divergence, etc.) from being rehydrated into Phase B.
    await observability.reset();
    await observability.init();
    transitionWriters.startAll();
  });

  afterAll(async () => {
    transitionWriters.stopAll();
    await observability.stop();
  });

  // ── B1: Writers consume SEMANTIC_PROJECTION_TRANSITION from onWrite ──────

  it('B1 — transition writer consumes SEMANTIC_PROJECTION_TRANSITION via onWrite and writes to ledger', async () => {
    await flushAllTestKeys();

    // Emit a SEMANTIC_PROJECTION_TRANSITION directly via observability.
    // NOTE: nextState must NOT be 'PROJECTION_INTENT' — that triggers the normalizer's
    // projection_intent rule which overwrites entryType to 'PROJECTION_INTENT',
    // causing the writer filter (entryType !== 'SEMANTIC_PROJECTION_TRANSITION') to reject it.
    // Use nextState='PROCESSING' (no rule match, base normalization preserves raw.entryType).
    // The entry is written to the global log only (not domain partition) but onWrite fires
    // from _transitionLog.push() in project() — writer processes it from there.
    await observability.transition({
      domain: 'runtime',
      entity: 'test-projection',
      entityId: 'test-1',
      previousState: 'IDLE',
      nextState: 'PROCESSING',
      authority: 'telemetry-coordination-fsm',
      traceId: 'test-trace-b1',
      raw: {
        entryType: 'SEMANTIC_PROJECTION_TRANSITION',
        projectionNamespace: 'runtime',
        projectionType: 'health-check',
        projectionPayload: { healthy: true },
        confidence: 0.99,
        integrityScore: 0.95,
      },
    });

    // DIAGNOSTIC: verify the transition was actually written to the in-memory _transitionLog.
    // If this fails, the problem is in the transition-emitter or projection layer, not the writer.
    const logSize = observability.query.getLogSize();
    expect(logSize).toBeGreaterThan(0),
      `observability plane must have written the transition to _transitionLog — got size ${logSize}`;

    // DIAGNOSTIC: read the last entry from _transitionLog via getEntriesSince(includeIndex).
    // getEntriesSince returns { entries, nextCursor, totalSize } — not a raw array.
    const result = observability.query.getEntriesSince(0); // includeIndex=0 → start from beginning
    expect(result).toBeDefined();
    expect(result.entries).toBeDefined();
    expect(Array.isArray(result.entries)).toBe(true),
      `getEntriesSince(0).entries must be an array — got ${typeof result.entries}`;
    expect(result.entries.length).toBeGreaterThan(0),
      `getEntriesSince(0).entries must have at least 1 entry — got ${result.entries.length}`;
    const lastEntry = result.entries[result.entries.length - 1];
    expect(lastEntry).toBeDefined();
    expect(lastEntry.raw).toBeDefined(),
      `lastEntry.raw must exist — got ${JSON.stringify(lastEntry)}`;
    expect(lastEntry.raw.entryType).toBe('SEMANTIC_PROJECTION_TRANSITION'),
      `entryType in _transitionLog must be SEMANTIC_PROJECTION_TRANSITION, got ${lastEntry.raw?.entryType}`;

    // DIAGNOSTIC: directly fire onWrite with a known-good entry to confirm writer fires.
    // This bypasses the transition() path entirely and directly proves the writer works.
    let directHookFired = false;
    const unsub = observability.onWrite((entry) => {
      if (entry.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION') {
        directHookFired = true;
      }
    });
    await observability.transition({
      domain: 'health',
      entity: 'direct-hook-test',
      entityId: 'direct-1',
      previousState: null,
      nextState: 'DIRECT_HOOK_TEST',
      authority: 'test',
      raw: { entryType: 'SEMANTIC_PROJECTION_TRANSITION', projectionNamespace: 'health' },
    });
    unsub(); // clean up test hook
    expect(directHookFired).toBe(true),
      `Direct hook test: onWrite must fire for SEMANTIC_PROJECTION_TRANSITION — got ${directHookFired}`;

    // Give writer time to process and write to ledger
    await new Promise(r => setTimeout(r, 100));

    // Await the write chain to eliminate timing race
    await transitionWriters.awaitPendingWrite('runtime');

    // Ledger should have one entry
    const ledgerCount = await countLedgerEntries();
    expect(ledgerCount).toBeGreaterThan(0),
      `Transition writer must write SEMANTIC_PROJECTION_TRANSITION to lineage:ledger:* — got ${ledgerCount} entries`;
  });

  // ── B2: Writers filter by domain — runtime writer ignores integrity ───────

  it('B2 — domain-bounded writer only consumes its own domain', async () => {
    await flushAllTestKeys();

    // Emit an integrity-domain transition — runtime writer should ignore it
    await observability.transition({
      domain: 'integrity',
      entity: 'test-projection',
      entityId: 'test-2',
      previousState: 'IDLE',
      nextState: 'PROCESSING',
      authority: 'telemetry-coordination-fsm',
      traceId: 'test-trace-b2',
      raw: {
        entryType: 'SEMANTIC_PROJECTION_TRANSITION',
        projectionNamespace: 'integrity',
        projectionType: 'health-check',
        projectionPayload: { healthy: true },
        confidence: 0.99,
        integrityScore: 0.95,
      },
    });

    await new Promise(r => setTimeout(r, 100));

    // Ledger should be empty — runtime writer doesn't consume integrity domain
    const ledgerCount = await countLedgerEntries();
    expect(ledgerCount).toBe(0),
      'Runtime writer must not consume integrity-domain SEMANTIC_PROJECTION_TRANSITION';
  });

  // ── B3: Writers do NOT consume PROJECTION_INTENT ─────────────────────────

  it('B3 — writers ignore PROJECTION_INTENT entries (not SEMANTIC_PROJECTION_TRANSITION)', async () => {
    await flushAllTestKeys();

    // Emit a PROJECTION_INTENT — workers emit these, not FSM output
    await observability.transition({
      domain: 'runtime',
      entity: 'test-projection',
      entityId: 'test-3',
      previousState: 'IDLE',
      nextState: 'PROJECTION_INTENT',
      authority: 'projection-worker',
      traceId: 'test-trace-b3',
      raw: {
        projectionNamespace: 'runtime',
        projectionType: 'health-check',
        projectionPayload: { healthy: true },
        confidence: 0.99,
        integrityScore: 0.95,
      },
    });

    await new Promise(r => setTimeout(r, 100));

    // Ledger must be empty — writers filter out non-SEMANTIC_PROJECTION_TRANSITION
    const ledgerCount = await countLedgerEntries();
    expect(ledgerCount).toBe(0),
      'Writers must not consume PROJECTION_INTENT — only SEMANTIC_PROJECTION_TRANSITION is written to ledger';
  });

});

// ── Phase C: FSM Reactive Path ────────────────────────────────────────────────

describe('PHASE C — FSM Reactive Path (integration)', () => {

  beforeAll(async () => {
    await flushAllTestKeys();
    await observability.init();
    // Start projection workers so FSM has something to react to
    telemetryKernel.startProjections(20);
    transitionWriters.startAll();
  });

  afterAll(async () => {
    telemetryKernel.stopProjections();
    transitionWriters.stopAll();
    await observability.stop();
  });

  // ── C1: FSM reactive path — worker PROJECTION_INTENT triggers onWrite → FSM ──

  it('C1 — FSM reacts to PROJECTION_INTENT via onWrite and emits SEMANTIC_PROJECTION_TRANSITION', async () => {
    await flushAllTestKeys();

    // Get the FSM from the kernel
    const fsm = telemetryKernel.fsm;

    // Initialize FSM with a mock CK context that supports dispatchGlobal
    const mockCtx = {
      dispatchGlobal: (event) => {
        // Mock — events dispatched to FSM's dispatch() are tested directly
      },
    };

    await fsm.init();
    fsm.start(mockCtx);

    // Wait for workers to emit PROJECTION_INTENT — FSM onWrite should react
    await new Promise(r => setTimeout(r, 200));

    fsm.stop();

    // Check: transition log should now contain FSM output (SEMANTIC_PROJECTION_TRANSITION)
    // because FSM's _onTransitionLogWrite detected PROJECTION_INTENT and called _emitTransition
    const runtimeEntries = await readDomainPartition('runtime');
    const hasFsmOutput = runtimeEntries.some(
      e => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );
    expect(hasFsmOutput).toBe(true),
      'FSM must emit SEMANTIC_PROJECTION_TRANSITION after reacting to PROJECTION_INTENT via onWrite';
  });

  // ── C2: FSM processes via CK-dispatched PROCESS_INTENTS ─────────────────

  it('C2 — FSM dispatch(PROCESS_INTENTS) reads from domain partition and emits transitions', async () => {
    await flushAllTestKeys();

    const fsm = telemetryKernel.fsm;
    const mockCtx = { dispatchGlobal: () => {} };

    fsm.init(mockCtx);
    fsm.start(mockCtx);

    // Emit some PROJECTION_INTENT entries directly
    for (const ns of ['runtime', 'health']) {
      await observability.transition({
        domain: ns,
        entity: 'test-entity',
        entityId: 'test-id',
        previousState: 'IDLE',
        nextState: 'PROJECTION_INTENT',
        authority: 'projection-worker',
        raw: { projectionNamespace: ns },
      });
    }

    await new Promise(r => setTimeout(r, 100));

    // Dispatch PROCESS_INTENTS to FSM — FSM should read intents from domain partitions
    await fsm.dispatch({ type: 'PROCESS_INTENTS' });

    // Wait for FSM to process
    await new Promise(r => setTimeout(r, 200));

    fsm.stop();

    // FSM should have emitted SEMANTIC_PROJECTION_TRANSITION for the valid intents
    const runtimeEntries = await readDomainPartition('runtime');
    const hasFsmOutput = runtimeEntries.some(
      e => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );
    expect(hasFsmOutput).toBe(true),
      'FSM dispatch(PROCESS_INTENTS) must emit SEMANTIC_PROJECTION_TRANSITION after reading intents';
  });

});
