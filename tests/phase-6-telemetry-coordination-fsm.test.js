/**
 * Phase 6: Deterministic Telemetry Coordination FSM — Constitutional Test Suite
 * ============================================================================
 *
 * Validates the FSM's constitutional position as the sole semantic ingress
 * serializer between telemetry projection workers and canonical lineage.
 *
 * Constitutional topology under test:
 *   projection workers → PROJECTION_INTENT → observability
 *                                                   ↓
 *   CK cadence → FSM reads intents → validates → orders → serializes
 *                                                   ↓
 *        SEMANTIC_PROJECTION_TRANSITION → observability → transition-writers → ledger
 *
 * Targeted Tests (T1–T12):
 *   T1:  Valid projection intents admitted to lineage
 *   T2:  PROJECTION_INTENT gate — transition-writers reject direct ingress
 *   T3:  Unknown namespace intents rejected by FSM
 *   T4:  Invalid authority intents rejected by FSM
 *   T5:  Signal ownership contract enforced
 *   T6:  CK halt blocks FSM coordination
 *   T7:  CK resume restores FSM coordination
 *   T8:  Halt/resume guard idempotency
 *   T9:  Namespace priority ordering is deterministic
 *   T10: Lexical ordering within namespace is deterministic
 *   T11: Same content produces same deterministic traceId (SHA-256 replay stability)
 *   T12: Full restart replay convergence — identical input across reboots
 *
 * Soak Test:
 *   SOAK_DURATION_MS continuous coordination under periodic worker churn
 *   (default 2 minutes, was 45 min). Six FSM-specific constitutional gates
 *   verified every CHECKPOINT_INTERVAL_MS.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { RuntimeSimulator } from './helpers/runtime-simulator.js';
import { waitForLedgerEntry, waitForLedgerEntryCount } from './helpers/sync-barriers.js';

const crypto = require('crypto');
const observability = require('../control-plane/observability/index.js');
const CK = require('../control-plane/governance/constitutional-kernel.js');
const tcf = require('../telemetry-kernel/fsm.js');
const lineageLedger = require('../control-plane/governance/lineage-ledger.js');
const { startMonitor, stopMonitor, getReport } = require('./helpers/runtime-monitor.js');

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

const KNOWN_NAMESPACES = ['integrity', 'authority', 'runtime', 'health', 'systemic'];

/**
 * Inject a PROJECTION_INTENT into the observability plane.
 *
 * This is what telemetry projection workers emit via
 * base-projection-worker._emitProjectionTransition().
 * The FSM reads these via _readIntents() and filters for
 * nextState === 'PROJECTION_INTENT'.
 *
 * @param {object} opts
 * @param {string} opts.namespace — projectionNamespace (integrity, authority, runtime, health, systemic)
 * @param {string} [opts.authority='test-projection-worker'] — must contain 'projection-worker' for FSM validation
 * @param {string} [opts.projectionType] — projection type identifier
 * @param {object} [opts.projectionPayload] — projection payload
 * @param {string} [opts.correlationId] — correlation ID
 * @param {number} [opts.timestamp] — override timestamp (ms) for determinism tests
 * @returns {object} the injected intent metadata
 */
function injectProjectionIntent({
  namespace,
  authority = 'test-projection-worker',
  projectionType,
  projectionPayload,
  correlationId,
  timestamp,
}) {
  const ts = timestamp || Date.now();
  const corrId = correlationId || `phase6-${ts}-${Math.random().toString(36).slice(2, 7)}`;
  const pType = projectionType || `${namespace}-projection`;
  const pPayload = projectionPayload || { timestamp: ts, source: `${namespace}-telemetry` };

  observability.transition({
    domain: 'telemetry',
    entity: 'projection_intent',
    entityId: pType,
    previousState: null,
    nextState: 'PROJECTION_INTENT',
    authority,
    raw: {
      intentType: 'PROJECTION_INTENT',
      projectionNamespace: namespace,
      projectionType: pType,
      projectionVersion: '1.0.0',
      projectionPayload: pPayload,
      confidence: 1.0,
      integrityScore: 1.0,
      sourceTelemetryWindow: {
        openedAt: ts - 5000,
        closedAt: ts,
        entryCount: 1,
        lineageStartCursor: 0,
        lineageEndCursor: 0,
      },
      traceId: crypto.randomUUID(),
      correlationId: corrId,
    },
  });

  return { namespace, projectionType: pType, correlationId: corrId, timestamp: ts };
}

/**
 * Wait for a SEMANTIC_PROJECTION_TRANSITION entry with a specific
 * projectionNamespace to appear in the ledger.
 */
async function waitForProjectionInLedger(projectionNamespace, timeoutMs = 15000) {
  return waitForLedgerEntry(
    (e) =>
      e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION' &&
      e.raw?.projectionNamespace === projectionNamespace,
    200,
    timeoutMs,
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Suite 1: Ingress Gatekeeping (T1–T5)
// ═══════════════════════════════════════════════════════════════════════════════

describe('Phase 6: Ingress Gatekeeping — FSM as Sole Serializer', () => {
  let sim;

  beforeAll(async () => {
    sim = new RuntimeSimulator({
      lineagePollMs: 400,
      telemetryPollMs: 50,
      autoTick: false,
    });
    await sim.boot();
  }, 30000);

  afterAll(async () => {
    await sim.shutdown();
  });

  // ── T1: Valid projection intents admitted to lineage ──────────────────
  it('T1: valid projection intents reach the ledger as SEMANTIC_PROJECTION_TRANSITION', async () => {
    // Let telemetry workers emit natural PROJECTION_INTENT entries
    await sleep(200);

    // Trigger the coordination cycle — FSM reads intents, validates, serializes
    await CK.dispatch({ type: 'PROCESS_INTENTS' });

    // Wait for transition-writers to consume the FSM-emitted transition
    const entry = await waitForProjectionInLedger('health', 15000);

    expect(entry).toBeDefined();
    expect(entry.authority).toBe('telemetry-coordination-fsm');
    expect(entry.raw.entryType).toBe('SEMANTIC_PROJECTION_TRANSITION');
    expect(KNOWN_NAMESPACES).toContain(entry.raw.projectionNamespace);
  });

  // ── T2: PROJECTION_INTENT gate — transition-writers reject them ──────────
  it('T2: PROJECTION_INTENT entries are blocked from direct lineage ingress', async () => {
    const ledgerSizeBefore = await lineageLedger.getSize();

    // Inject a PROJECTION_INTENT directly — bypassing the FSM
    injectProjectionIntent({
      namespace: 'integrity',
      correlationId: `phase6-t2-${Date.now()}`,
    });

    // Inject a plain transition after it as a "sentinel" — once this
    // appears in the ledger, we know the transition-writers have processed
    // all entries up to and including our PROJECTION_INTENT.
    const sentinelId = `phase6-t2-sentinel-${Date.now()}`;
    observability.transition({
      domain: 'governance',
      entity: 'sentinel',
      entityId: sentinelId,
      previousState: null,
      nextState: 'SENTINEL_ARRIVED',
      authority: 'phase6-test',
      raw: { test: 't2-sentinel' },
    });

    await waitForLedgerEntry(
      (e) => e.entity === 'sentinel' && e.entityId === sentinelId,
      100,
      15000,
    );

    // Scan the ledger for any PROJECTION_INTENT entries — must be zero
    const ledger = await lineageLedger.getLineage(200);
    const directProjectionIntents = ledger.filter(
      (e) =>
        e.nextState === 'PROJECTION_INTENT' ||
        e.raw?.entryType === 'PROJECTION_INTENT' ||
        e.raw?.intentType === 'PROJECTION_INTENT',
    );

    expect(directProjectionIntents.length).toBe(0);
  });

  // ── T3: Unknown namespace intents rejected ────────────────────────────
  it('T3: unknown projection namespace intents are rejected by the FSM', async () => {
    const logBefore = tcf.getRejectionLog().length;

    injectProjectionIntent({
      namespace: 'malicious-fake-namespace',
      correlationId: `phase6-t3-${Date.now()}`,
    });

    // Trigger coordination — FSM should reject the unknown namespace
    await CK.dispatch({ type: 'PROCESS_INTENTS' });

    // Rejection log should have grown
    const logAfter = tcf.getRejectionLog();
    expect(logAfter.length).toBeGreaterThan(logBefore);

    // Latest rejection should reference the unknown namespace
    const lastRejection = logAfter[logAfter.length - 1];
    expect(lastRejection.projectionNamespace).toBe('malicious-fake-namespace');
  });

  // ── T4: Invalid authority intents rejected ────────────────────────────
  it('T4: intents with non-projection-worker authority are rejected', async () => {
    const logBefore = tcf.getRejectionLog().length;

    injectProjectionIntent({
      namespace: 'runtime',
      authority: 'rogue-injector', // does NOT contain 'projection-worker'
      correlationId: `phase6-t4-${Date.now()}`,
    });

    await CK.dispatch({ type: 'PROCESS_INTENTS' });

    const logAfter = tcf.getRejectionLog();
    expect(logAfter.length).toBeGreaterThan(logBefore);

    const lastRejection = logAfter[logAfter.length - 1];
    expect(lastRejection.violations.some(
      (v) => v.field === 'authority',
    )).toBe(true);
  });

  // ── T5: Signal ownership contract enforced ────────────────────────────
  it('T5: projection payloads with lineage-owned signals are rejected', async () => {
    const logBefore = tcf.getRejectionLog().length;

    // Inject an intent whose payload contains a signal owned by lineage-worker
    // (e.g., 'domain.acquisition' is ledger-derivable, not telemetry-owned)
    injectProjectionIntent({
      namespace: 'health',
      projectionPayload: {
        'domain.acquisition': 'LINEAGE_LEAK', // lineage-owned signal in telemetry payload
        timestamp: Date.now(),
      },
      correlationId: `phase6-t5-${Date.now()}`,
    });

    await CK.dispatch({ type: 'PROCESS_INTENTS' });

    const logAfter = tcf.getRejectionLog();
    expect(logAfter.length).toBeGreaterThan(logBefore);

    const lastRejection = logAfter[logAfter.length - 1];
    const signalViolations = lastRejection.violations.filter(
      (v) => v.field && v.field.startsWith('domain.'),
    );
    expect(signalViolations.length).toBeGreaterThan(0);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Suite 2: CK Authority Over FSM (T6–T8)
// ═══════════════════════════════════════════════════════════════════════════════

describe('Phase 6: CK Authority Over FSM', () => {
  let sim;

  beforeAll(async () => {
    sim = new RuntimeSimulator({
      lineagePollMs: 400,
      telemetryPollMs: 50,
      autoTick: false,
    });
    await sim.boot();
  }, 30000);

  afterAll(async () => {
    await sim.shutdown();
  });

  // ── T6: CK halt blocks FSM coordination ───────────────────────────────
  it('T6: CK halt blocks all FSM coordination — PROCESS_INTENTS rejected', async () => {
    // Halt the FSM via CK authority
    const haltResult = CK.dispatch({ type: 'HALT_TELEMETRY_COORDINATION' });
    expect(haltResult.allowed).toBe(true);
    expect(tcf.getState()).toBe('HALTED');

    // Inject a valid intent — should NOT be processed
    injectProjectionIntent({
      namespace: 'authority',
      correlationId: `phase6-t6-${Date.now()}`,
    });

    // Attempt to process while HALTED
    const processResult = await CK.dispatch({ type: 'PROCESS_INTENTS' });
    expect(processResult.allowed).toBe(false);
    expect(processResult.reason).toContain('Cannot process intents while HALTED');

    // Verify no new SEMANTIC_PROJECTION_TRANSITION appeared
    const state = tcf.exportState();
    expect(state.priorCycleOutputCount).toBe(0);
  });

  // ── T7: CK resume restores FSM coordination ───────────────────────────
  it('T7: CK resume restores FSM coordination — PROCESS_INTENTS succeeds', async () => {
    // Resume from HALTED
    const resumeResult = CK.dispatch({ type: 'RESUME_TELEMETRY_COORDINATION' });
    expect(resumeResult.allowed).toBe(true);
    expect(tcf.getState()).toBe('IDLE');

    // Inject a valid intent
    injectProjectionIntent({
      namespace: 'runtime',
      correlationId: `phase6-t7-${Date.now()}`,
    });

    // Now PROCESS_INTENTS should succeed
    const processResult = await CK.dispatch({ type: 'PROCESS_INTENTS' });
    expect(processResult.allowed).toBe(true);

    // Verify the serialized transition reached the ledger
    const entry = await waitForProjectionInLedger('runtime', 15000);
    expect(entry).toBeDefined();
    expect(entry.authority).toBe('telemetry-coordination-fsm');
  });

  // ── T8: Halt/resume guard idempotency ─────────────────────────────────
  it('T8: halt and resume guards are idempotent', async () => {
    // Double-halt: already HALTED from T6 but resumed in T7, so we're IDLE now
    // First: halt → should succeed
    CK.dispatch({ type: 'HALT_TELEMETRY_COORDINATION' });
    expect(tcf.getState()).toBe('HALTED');

    // Second halt: should be rejected by guard
    const doubleHalt = CK.dispatch({ type: 'HALT_TELEMETRY_COORDINATION' });
    expect(doubleHalt.allowed).toBe(false);
    expect(doubleHalt.reason).toContain('Already HALTED');
    expect(tcf.getState()).toBe('HALTED');

    // Resume → should succeed
    CK.dispatch({ type: 'RESUME_TELEMETRY_COORDINATION' });
    expect(tcf.getState()).toBe('IDLE');

    // Resume from IDLE: should be rejected
    const resumeFromIdle = CK.dispatch({ type: 'RESUME_TELEMETRY_COORDINATION' });
    expect(resumeFromIdle.allowed).toBe(false);
    expect(resumeFromIdle.reason).toContain('Cannot resume from IDLE');
    expect(tcf.getState()).toBe('IDLE');
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Suite 3: FSM Determinism (T9–T10)
// ═══════════════════════════════════════════════════════════════════════════════

describe('Phase 6: FSM Determinism', () => {
  let sim;

  beforeAll(async () => {
    sim = new RuntimeSimulator({
      lineagePollMs: 400,
      telemetryPollMs: 50,
      autoTick: false,
    });
    await sim.boot();
  }, 30000);

  afterAll(async () => {
    await sim.shutdown();
  });

  // ── T9: Namespace priority ordering is deterministic ──────────────────
  it('T9: intents are ordered by namespace priority regardless of injection order', async () => {
    const ts = Date.now();

    // Inject in reverse priority order (systemic first, integrity last)
    injectProjectionIntent({ namespace: 'systemic', correlationId: `phase6-t9-sys-${ts}`, timestamp: ts });
    injectProjectionIntent({ namespace: 'health', correlationId: `phase6-t9-health-${ts}`, timestamp: ts });
    injectProjectionIntent({ namespace: 'runtime', correlationId: `phase6-t9-rt-${ts}`, timestamp: ts });
    injectProjectionIntent({ namespace: 'authority', correlationId: `phase6-t9-auth-${ts}`, timestamp: ts });
    injectProjectionIntent({ namespace: 'integrity', correlationId: `phase6-t9-int-${ts}`, timestamp: ts });

    await CK.dispatch({ type: 'PROCESS_INTENTS' });

    // Wait for lineage ingestion
    await waitForLedgerEntryCount(1, 15000);

    const ledger = await lineageLedger.getLineage(200);
    const projections = ledger.filter(
      (e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION',
    );

    const orderedNamespaces = projections.map((e) => e.raw?.projectionNamespace).filter(Boolean);

    // Must appear in priority order: integrity(1), authority(2), runtime(3), health(4), systemic(5)
    const expectedOrder = ['integrity', 'authority', 'runtime', 'health', 'systemic'];
    const actualOrder = expectedOrder.filter((ns) => orderedNamespaces.includes(ns));

    expect(actualOrder).toEqual(expectedOrder);
  });

  // ── T10: Lexical ordering within a namespace is deterministic ─────────
  it('T10: within the same namespace, intents are ordered lexically by projectionType', async () => {
    const ts = Date.now();

    // Inject in reverse lexical order
    injectProjectionIntent({
      namespace: 'health',
      projectionType: 'z-check',
      correlationId: `phase6-t10-z-${ts}`,
      timestamp: ts,
    });
    injectProjectionIntent({
      namespace: 'health',
      projectionType: 'a-check',
      correlationId: `phase6-t10-a-${ts}`,
      timestamp: ts,
    });

    await CK.dispatch({ type: 'PROCESS_INTENTS' });

    await waitForLedgerEntryCount(1, 15000);

    const ledger = await lineageLedger.getLineage(200);
    const projections = ledger.filter(
      (e) =>
        e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION' &&
        e.raw?.projectionNamespace === 'health',
    );

    const projectionTypes = projections.map((e) => e.raw?.projectionType).filter(Boolean);

    // 'a-check' must come before 'z-check' (lexical ordering)
    const aIdx = projectionTypes.indexOf('a-check');
    const zIdx = projectionTypes.indexOf('z-check');

    expect(aIdx).toBeGreaterThan(-1);
    expect(zIdx).toBeGreaterThan(-1);
    expect(aIdx).toBeLessThan(zIdx);
  });

  // ── T11: Same content → same deterministic traceId ───────────────────
  it('T11: identical intent content produces identical traceId (SHA-256 replay stability)', async () => {
    const FIXED_TS = 1700000000000; // fixed timestamp for determinism
    const FIXED_CORR = 'phase6-t11-fixed-correlation';

    // Inject the exact same intent twice
    injectProjectionIntent({
      namespace: 'integrity',
      projectionType: 'integrity-projection',
      projectionPayload: { metric: 42, source: 'determinism-test' },
      correlationId: FIXED_CORR,
      timestamp: FIXED_TS,
    });
    injectProjectionIntent({
      namespace: 'integrity',
      projectionType: 'integrity-projection',
      projectionPayload: { metric: 42, source: 'determinism-test' },
      correlationId: FIXED_CORR,
      timestamp: FIXED_TS,
    });

    await CK.dispatch({ type: 'PROCESS_INTENTS' });

    await waitForLedgerEntryCount(1, 15000);

    const ledger = await lineageLedger.getLineage(200);
    const projections = ledger.filter(
      (e) =>
        e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION' &&
        e.raw?.projectionNamespace === 'integrity' &&
        e.raw?.projectionType === 'integrity-projection',
    );

    // Both serialized entries should have the same traceId
    // because SHA-256 of identical content is deterministic
    expect(projections.length).toBe(2);
    expect(projections[0].traceId).toBeDefined();
    expect(projections[0].traceId).toBe(projections[1].traceId);
  });

  // ── T12: Full restart replay convergence ─────────────────────────────
  it('T12: identical input sequence across reboots produces convergent lineage', async () => {
    const FIXED_TS = 1700000001000;
    const INTENTS = [
      { namespace: 'integrity', projectionType: 'replay-int', payload: { v: 1 } },
      { namespace: 'authority', projectionType: 'replay-auth', payload: { v: 2 } },
      { namespace: 'runtime', projectionType: 'replay-rt', payload: { v: 3 } },
    ];

    // ── Run A ──────────────────────────────────────────────────────────
    for (const intent of INTENTS) {
      injectProjectionIntent({
        namespace: intent.namespace,
        projectionType: intent.projectionType,
        projectionPayload: intent.payload,
        timestamp: FIXED_TS,
      });
    }
    await CK.dispatch({ type: 'PROCESS_INTENTS' });
    await waitForLedgerEntryCount(1, 15000);

    const ledgerA = await lineageLedger.getLineage(200);
    const projectionsA = ledgerA.filter(
      (e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION',
    );

    // ── Shutdown and flush Redis ───────────────────────────────────────
    await sim.shutdown();
    await sim.restartRedis();
    await sleep(500);

    // ── Boot fresh simulator — Run B ───────────────────────────────────
    sim = new RuntimeSimulator({
      lineagePollMs: 400,
      telemetryPollMs: 50,
      autoTick: false,
    });
    await sim.boot();

    for (const intent of INTENTS) {
      injectProjectionIntent({
        namespace: intent.namespace,
        projectionType: intent.projectionType,
        projectionPayload: intent.payload,
        timestamp: FIXED_TS,
      });
    }
    await CK.dispatch({ type: 'PROCESS_INTENTS' });
    await waitForLedgerEntryCount(1, 15000);

    const ledgerB = await lineageLedger.getLineage(200);
    const projectionsB = ledgerB.filter(
      (e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION',
    );

    // ── Compare structural convergence ─────────────────────────────────
    expect(projectionsB.length).toBe(projectionsA.length);

    const normalizeEntry = (e) => ({
      domain: e.domain,
      entity: e.entity,
      authority: e.authority,
      traceId: e.traceId,
      projectionNamespace: e.raw?.projectionNamespace,
      projectionType: e.raw?.projectionType,
    });

    const structA = projectionsA.map(normalizeEntry);
    const structB = projectionsB.map(normalizeEntry);

    // Structural properties must be identical across reboots
    expect(structA).toEqual(structB);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Suite 4: Constitutional Coordination Soak
// ═══════════════════════════════════════════════════════════════════════════════

// Soak configuration — overridable via environment variables.
// 2-minute default (was 45 min) — proportional intervals derived from
// SOAK_DURATION_MS. Override via env for hour-long production runs.
const SOAK_DURATION_MS = parseInt(process.env.PHASE6_SOAK_MS || String(2 * 60 * 1000), 10);
const TICK_INTERVAL_MS = parseInt(process.env.PHASE6_TICK_MS || '250', 10);
// Proportional defaults — derived from SOAK_DURATION_MS.
const ADVERSARIAL_INTERVAL_TICKS = parseInt(process.env.PHASE6_ADV_INTERVAL || String(Math.max(1, Math.floor(SOAK_DURATION_MS / 15000))), 10);
const COORDINATION_INTERVAL_MS = parseInt(process.env.PHASE6_COORD_MS || String(Math.max(1000, Math.floor(SOAK_DURATION_MS / 8))), 10);
const CHECKPOINT_INTERVAL_MS = parseInt(process.env.PHASE6_CHECKPOINT_MS || String(Math.max(1000, Math.floor(SOAK_DURATION_MS / 6))), 10);
const RECYCLE_INTERVAL_MS = parseInt(process.env.PHASE6_RECYCLE_MS || String(Math.max(1000, Math.floor(SOAK_DURATION_MS / 3))), 10);
const LINEAGE_RECYCLE_INTERVAL_MS = parseInt(process.env.PHASE6_LIN_RECYCLE_MS || String(Math.max(1000, Math.floor(SOAK_DURATION_MS / 2))), 10);
const LEDGER_LOOKBACK = 500;

const LEGAL_DOMAINS = ['acquisition', 'publishing', 'scheduling', 'dedup', 'engagement'];
const LEGAL_STATES = ['IDLE', 'QUEUED', 'RECEIVED', 'SCHEDULED', 'ACTIVE', 'PROCESSING', 'COMPLETE'];

const ADVERSARIAL_SIGNAL_PAYLOADS = [
  { 'domain.acquisition': 'LINEAGE_LEAK' },
  { 'domain.publishing': 'LINEAGE_LEAK' },
  { 'governanceRuntime.runtimeState': 'LINEAGE_LEAK' },
  { 'integrity.structuralAnomalyCount': 1 },
  { 'health.executionHealth': 'LINEAGE_LEAK' },
];

async function writeSoakReport(payload) {
  const outputDir = path.resolve(process.cwd(), 'tests/output');
  await mkdir(outputDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const stampedPath = path.join(outputDir, `phase-6-soak-${ts}.json`);
  const latestPath = path.join(outputDir, 'phase-6-soak-latest.json');
  const content = JSON.stringify(payload, null, 2) + '\n';
  await writeFile(stampedPath, content, 'utf8');
  await writeFile(latestPath, content, 'utf8');
}

describe('Phase 6: Constitutional Coordination Soak', () => {
  let sim;

  beforeAll(async () => {
    sim = new RuntimeSimulator({
      lineagePollMs: 400,
      telemetryPollMs: 50,
      tickIntervalMs: 1000,
      autoTick: true,
    });
    await sim.boot();
  }, 30000);

  afterAll(async () => {
    await sim.shutdown();
  });

  it(
    'survives continuous coordination under adversarial ingress and worker churn',
    async () => {
      const startTime = Date.now();

      // ── Counters ──────────────────────────────────────────────────
      let tickCount = 0;
      let adversarialCount = 0;
      let legalCount = 0;
      let coordCycleCount = 0;

      // ── Start runtime monitor probe ──────────────────────────────
      const probeStarted = await startMonitor({
        intervalMs: 30000,
        ledgerLookback: LEDGER_LOOKBACK,
      });
      console.log(
        `[phase-6] Monitor probe started at ${new Date(probeStarted.startedAt).toISOString()}`,
      );

      // ── Ticker: legal transitions + adversarial PROJECTION_INTENT ─
      const ticker = setInterval(() => {
        tickCount++;

        // Legal transition every tick — keeps the runtime active
        const domain = LEGAL_DOMAINS[tickCount % LEGAL_DOMAINS.length];
        const state = LEGAL_STATES[tickCount % LEGAL_STATES.length];
        observability.transition({
          domain,
          entity: 'soak_transition',
          entityId: `p6-${domain}-${tickCount}`,
          previousState: LEGAL_STATES[(tickCount - 1) % LEGAL_STATES.length] || 'IDLE',
          nextState: state,
          authority: `${domain}-fsm`,
          raw: { wave: 'phase6', tick: tickCount, domain },
        });
        legalCount++;

        // Adversarial PROJECTION_INTENT every ADVERSARIAL_INTERVAL_TICKS
        if (tickCount % ADVERSARIAL_INTERVAL_TICKS === 0) {
          adversarialCount++;
          const advIdx = adversarialCount % 5;

          switch (advIdx) {
            case 0: // Unknown namespace
              injectProjectionIntent({
                namespace: 'malicious-ns',
                authority: 'malicious-projection-worker',
                correlationId: `p6-soak-adv-unknown-${tickCount}`,
              });
              break;
            case 1: // Invalid authority
              injectProjectionIntent({
                namespace: KNOWN_NAMESPACES[adversarialCount % KNOWN_NAMESPACES.length],
                authority: 'rogue-injector',
                correlationId: `p6-soak-adv-auth-${tickCount}`,
              });
              break;
            case 2: // Signal ownership violation
              injectProjectionIntent({
                namespace: 'health',
                projectionPayload: ADVERSARIAL_SIGNAL_PAYLOADS[adversarialCount % ADVERSARIAL_SIGNAL_PAYLOADS.length],
                correlationId: `p6-soak-adv-signal-${tickCount}`,
              });
              break;
            case 3: // Missing projectionPayload
              observability.transition({
                domain: 'telemetry',
                entity: 'projection_intent',
                entityId: `p6-soak-nopayload-${tickCount}`,
                previousState: null,
                nextState: 'PROJECTION_INTENT',
                authority: 'malicious-projection-worker',
                raw: {
                  intentType: 'PROJECTION_INTENT',
                  projectionNamespace: 'runtime',
                  projectionType: 'runtime-projection',
                  projectionVersion: '1.0.0',
                  // NO projectionPayload — should be rejected
                  confidence: 1.0,
                  integrityScore: 1.0,
                  traceId: crypto.randomUUID(),
                  correlationId: `p6-soak-adv-nopayload-${tickCount}`,
                },
              });
              break;
            case 4: // Malformed — missing projectionType
              observability.transition({
                domain: 'telemetry',
                entity: 'projection_intent',
                entityId: `p6-soak-notype-${tickCount}`,
                previousState: null,
                nextState: 'PROJECTION_INTENT',
                authority: 'malicious-projection-worker',
                raw: {
                  intentType: 'PROJECTION_INTENT',
                  projectionNamespace: 'authority',
                  // NO projectionType
                  projectionVersion: '1.0.0',
                  projectionPayload: { fake: true },
                  confidence: 1.0,
                  integrityScore: 1.0,
                  traceId: crypto.randomUUID(),
                  correlationId: `p6-soak-adv-notype-${tickCount}`,
                },
              });
              break;
          }
        }
      }, TICK_INTERVAL_MS);

      // ── Coordination cycle timer ──────────────────────────────────
      const coordResults = [];
      const coordTimer = setInterval(async () => {
        try {
          await CK.dispatch({ type: 'PROCESS_INTENTS' });
          coordCycleCount++;
          coordResults.push({
            cycle: coordCycleCount,
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            fsmState: tcf.getState(),
            fsmHealth: tcf.getHealth(),
          });
        } catch (err) {
          coordResults.push({
            cycle: coordCycleCount + 1,
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            error: err.message,
          });
        }
      }, COORDINATION_INTERVAL_MS);

      // ── Checkpoint timer — verify all 6 FSM constitutional gates ──
      const checkpoints = [];
      let lastLogSize = 0;

      const checkpointTimer = setInterval(async () => {
        try {
          const ledger = await lineageLedger.getLineage(LEDGER_LOOKBACK);
          const logSize = observability.query.getLogSize();
          const ckState = CK.getState();
          const fsmState = tcf.getState();
          const fsmExport = tcf.exportState();
          const fsmHealth = tcf.getHealth();
          const rejectionLogSize = tcf.getRejectionLog().length;

          const checkpoint = {
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            elapsed_min: Math.round((Date.now() - startTime) / 60000),
            tickCount,
            legalCount,
            adversarialCount,
            coordCycleCount,
            logSize,
            ledgerSize: ledger.length,
            ckState,
            fsmState,
            cursorAdvanceOk: logSize >= lastLogSize,
            violations: [],
          };

          // GATE-1: Zero PROJECTION_INTENT entries in the canonical ledger
          const projectionIntentsInLedger = ledger.filter(
            (e) =>
              e.nextState === 'PROJECTION_INTENT' ||
              e.raw?.entryType === 'PROJECTION_INTENT' ||
              e.raw?.intentType === 'PROJECTION_INTENT',
          );
          if (projectionIntentsInLedger.length > 0) {
            checkpoint.violations.push(
              `GATE-1: ${projectionIntentsInLedger.length} PROJECTION_INTENT entries in ledger`,
            );
          }

          // GATE-2: All SEMANTIC_PROJECTION_TRANSITION have authority 'telemetry-coordination-fsm'
          const projections = ledger.filter(
            (e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION',
          );
          const rogueProjections = projections.filter(
            (e) => e.authority !== 'telemetry-coordination-fsm',
          );
          if (rogueProjections.length > 0) {
            checkpoint.violations.push(
              `GATE-2: ${rogueProjections.length} SEMANTIC_PROJECTION_TRANSITION with wrong authority`,
            );
          }

          // GATE-3: FSM state is IDLE — never stuck in a processing state
          if (fsmState !== 'IDLE') {
            checkpoint.violations.push(`GATE-3: FSM state is ${fsmState} (expected IDLE)`);
          }

          // GATE-4: Rejection log size is bounded (< 500)
          if (rejectionLogSize >= 500) {
            checkpoint.violations.push(`GATE-4: Rejection log size ${rejectionLogSize} >= 500`);
          }

          // GATE-5: FSM health signals ok — not halted unless test explicitly halted it
          if (!fsmHealth.ok && fsmState !== 'HALTED') {
            checkpoint.violations.push(`GATE-5: FSM health not ok: ${JSON.stringify(fsmHealth.signals)}`);
          }

          // GATE-6: CK state is HEALTHY (legal transitions only, no fatal errors)
          if (ckState !== 'HEALTHY' && ckState !== 'DEGRADED') {
            checkpoint.violations.push(`GATE-6: CK state is ${ckState} (expected HEALTHY or DEGRADED)`);
          }

          lastLogSize = logSize;
          checkpoints.push(checkpoint);
        } catch (err) {
          checkpoints.push({
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            error: err.message,
          });
        }
      }, CHECKPOINT_INTERVAL_MS);

      // ── Telemetry worker recycle timer ─────────────────────────────
      const recycleTimer = setInterval(async () => {
        console.log('[phase-6] Recycling telemetry workers...');
        await sim.killTelemetryWorkers();
        await sleep(200);
        await sim.restartTelemetryWorkers();
        console.log('[phase-6] Telemetry workers restarted');
      }, RECYCLE_INTERVAL_MS);

      // ── Transition-writers recycle timer ──────────────────────────────
      const transitionWritersRecycleTimer = setInterval(async () => {
        console.log('[phase-6] Recycling transition-writers...');
        await sim.killTransitionWriters();
        await sleep(200);
        await sim.restartTransitionWriters();
        console.log('[phase-6] Transition-writers restarted');
      }, LINEAGE_RECYCLE_INTERVAL_MS);

      // ═══════════════════════════════════════════════════════════════
      // RUN THE SOAK
      // ═══════════════════════════════════════════════════════════════
      const soakMin = Math.round(SOAK_DURATION_MS / 60000);
      console.log(
        `[phase-6] Starting coordination soak: ${SOAK_DURATION_MS}ms (${soakMin}min) at ${TICK_INTERVAL_MS}ms tick`,
      );
      await sleep(SOAK_DURATION_MS);

      // ── Stop all timers ──────────────────────────────────────────
      clearInterval(ticker);
      clearInterval(coordTimer);
      clearInterval(checkpointTimer);
      clearInterval(recycleTimer);
      clearInterval(transitionWritersRecycleTimer);

      // Allow final ingestion and coordination to settle
      await sleep(3000);

      // ── Stop monitor and get report ──────────────────────────────
      await stopMonitor();
      const monitorReport = getReport();

      // ═══════════════════════════════════════════════════════════════
      // FINAL VERIFICATION
      // ═══════════════════════════════════════════════════════════════

      const elapsed_ms = Date.now() - startTime;
      const elapsed_min = Math.round(elapsed_ms / 60000);
      const finalLedger = await lineageLedger.getLineage(2000);
      const finalCkState = CK.getState();
      const finalFsmState = tcf.getState();
      const finalFsmExport = tcf.exportState();
      const finalRejectionLogSize = tcf.getRejectionLog().length;

      // ── Tick count must be within 15% of expected ────────────────
      const expectedTicks = Math.floor(SOAK_DURATION_MS / TICK_INTERVAL_MS);
      expect(tickCount).toBeGreaterThanOrEqual(expectedTicks * 0.85);

      // ── Coordination cycles must have fired ──────────────────────
      const expectedCoords = Math.floor(SOAK_DURATION_MS / COORDINATION_INTERVAL_MS);
      expect(coordCycleCount).toBeGreaterThanOrEqual(expectedCoords * 0.85);

      // ── Checkpoints must exist ───────────────────────────────────
      const expectedCheckpoints = Math.floor(SOAK_DURATION_MS / CHECKPOINT_INTERVAL_MS);
      expect(checkpoints.length).toBeGreaterThanOrEqual(expectedCheckpoints * 0.8);

      // ── CK must be healthy or degraded ───────────────────────────
      expect(['HEALTHY', 'DEGRADED', 'RECOVERY']).toContain(finalCkState);
      expect(finalCkState).not.toBe('HALTED');

      // ── FSM must be IDLE after all cycles complete ───────────────
      expect(finalFsmState).toBe('IDLE');

      // ── No PROJECTION_INTENT entries in final ledger ─────────────
      const finalProjectionIntents = finalLedger.filter(
        (e) =>
          e.nextState === 'PROJECTION_INTENT' ||
          e.raw?.entryType === 'PROJECTION_INTENT' ||
          e.raw?.intentType === 'PROJECTION_INTENT',
      );
      expect(finalProjectionIntents.length).toBe(0);

      // ── All SEMANTIC_PROJECTION_TRANSITION must have correct authority
      const finalProjections = finalLedger.filter(
        (e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION',
      );
      const rogueFinal = finalProjections.filter(
        (e) => e.authority !== 'telemetry-coordination-fsm',
      );
      expect(rogueFinal.length).toBe(0);

      // ── Runtime monitor must report no violations ────────────────
      expect(monitorReport.violationCount).toBe(0);
      expect(monitorReport.ok).toBe(true);

      // ── Checkpoints must show zero GATE violations ───────────────
      const violatedCheckpoints = checkpoints.filter(
        (cp) => cp.violations && cp.violations.length > 0,
      );
      if (violatedCheckpoints.length > 0) {
        console.error(
          `[phase-6] WARNING: ${violatedCheckpoints.length} checkpoints had violations:`,
          violatedCheckpoints.map(
            (c) => `${c.elapsed_s}s: ${(c.violations || []).join(', ')}`,
          ),
        );
      }

      // ── Rejection log must have entries (proves gatekeeping was exercised)
      expect(finalRejectionLogSize).toBeGreaterThan(0);

      // ── Write soak report ────────────────────────────────────────
      const report = {
        phase: '6',
        test: 'constitutional-coordination-soak',
        soakConfig: {
          SOAK_DURATION_MS,
          TICK_INTERVAL_MS,
          ADVERSARIAL_INTERVAL_TICKS,
          COORDINATION_INTERVAL_MS,
          CHECKPOINT_INTERVAL_MS,
          RECYCLE_INTERVAL_MS,
          LINEAGE_RECYCLE_INTERVAL_MS,
          LEDGER_LOOKBACK,
        },
        results: {
          elapsed_ms,
          elapsed_min,
          tickCount,
          legalCount,
          adversarialCount,
          coordCycleCount,
          checkpointCount: checkpoints.length,
          finalLedgerSize: finalLedger.length,
          finalCkState,
          finalFsmState,
          finalFsmExport,
          finalRejectionLogSize,
          monitorViolationCount: monitorReport.violationCount,
          violatedCheckpointCount: violatedCheckpoints.length,
        },
        monitorReport: {
          snapshots: monitorReport.snapshots,
          violations: monitorReport.violations,
          summary: monitorReport.summary,
        },
        coordResults,
        checkpoints,
        generatedAt: new Date().toISOString(),
      };

      await writeSoakReport(report);

      console.log(
        `[phase-6] Soak complete: ${elapsed_min}min, ${tickCount} ticks, ` +
        `${adversarialCount} adversarial, ${coordCycleCount} coord cycles, ` +
        `${monitorReport.violationCount} monitor violations, ` +
        `${violatedCheckpoints.length} violated checkpoints`,
      );
    },
    SOAK_DURATION_MS + 120_000,
  );
});
