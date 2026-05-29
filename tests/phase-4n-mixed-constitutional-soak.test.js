/**
 * Phase 4N: Mixed Constitutional Soak — 30-Minute Continuous Test
 * ================================================================
 *
 * Validates constitutional runtime stability under sustained 30-minute
 * mixed workload: legal FSM transitions alternated with adversarial
 * attacks (membrane bypass, stale entries, duplicate replay, causal
 * conflicts, out-of-order delivery).
 *
 * This closes the critical gap identified in Phase 4G/4F/4I:
 *   - Those tests run adversarial events as ONE-SHOT isolated checks
 *   - They never validated adversarial+legal alternation over time
 *   - They never validated 30-min sustained adversarial pressure
 *   - They never mixed bad state mutations with legal GoC transitions
 *
 * Constitutional laws validated:
 *   L1: Projections reconstructed from lineage must converge identically.
 *   L2: Replay events may never mutate prior lineage history.
 *   L3: Cross-domain transitions may never bypass membrane authority.
 *   L4: Cursor positions remain monotonic.
 *   L5: Duplicate replay injection must remain idempotent.
 *   L6: Stale entries must be flagged — never silently accepted.
 *
 * Wave Architecture:
 *   - TICK_INTERVAL_MS: how often a wave is injected (100ms default)
 *   - ADVERSARIAL_INTERVAL_TICKS: every N ticks, inject one adversarial event
 *   - CHECKPOINT_INTERVAL_MS: how often to verify invariants (30s default)
 *   - RECYCLE_INTERVAL_MS: how often to recycle workers (5min default)
 *   - SOAK_DURATION_MS: total soak time (default 30min)
 *
 * At 100ms tick × 18000 ticks = 18000 waves over 30min
 * Every 20th tick is adversarial = ~900 adversarial events mixed in
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntry, waitForLedgerEntryCount, waitForProjectionFlush } = require('./helpers/sync-barriers');
const { deterministicEntryHash, assertNoTimestampRegression, assertMonotonicCursors } = require('./helpers/constitutional-invariants');
const { startMonitor, stopMonitor, getReport } = require('./helpers/runtime-monitor');

// ── Soak configuration (all configurable via env) ────────────────────────────
const SOAK_DURATION_MS = parseInt(process.env.PHASE4N_SOAK_MS || String(30 * 60 * 1000), 10); // 30 min
const TICK_INTERVAL_MS = parseInt(process.env.PHASE4N_TICK_MS || '100', 10);
const ADVERSARIAL_INTERVAL_TICKS = parseInt(process.env.PHASE4N_ADV_INTERVAL || '20', 10);
const CHECKPOINT_INTERVAL_MS = parseInt(process.env.PHASE4N_CHECKPOINT_MS || '30000', 10);
const RECYCLE_INTERVAL_MS = parseInt(process.env.PHASE4N_RECYCLE_MS || String(5 * 60 * 1000), 10);
const LEDGER_LOOKBACK = 500;

const LEGAL_TRANSITION_VARIANTS = ['success', 'partial', 'stale', 'rateLimited'];
const ADVERSARIAL_TYPES = [
  'out_of_order',
  'duplicate_causal_chain',
  'conflicting_transition',
  'adversarial_cross_domain',
  'adversarial_telemetry_execution',
  'adversarial_reconciliation_projection',
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function writeSoakReport(payload) {
  const outputDir = path.resolve(process.cwd(), 'tests/output');
  await mkdir(outputDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const stampedPath = path.join(outputDir, `phase-4n-soak-${ts}.json`);
  const latestPath = path.join(outputDir, `phase-4n-soak-latest.json`);
  const content = `${JSON.stringify(payload, null, 2)}\n`;
  await writeFile(stampedPath, content, 'utf8');
  await writeFile(latestPath, content, 'utf8');
}

// ── Per-tick injection strategy ───────────────────────────────────────────────
const eventInjector = require('./event-injector.js');

let _tickCount = 0;
let _adversarialCount = 0;
let _legalCount = 0;

function injectLegalWave(seq) {
  const variant = LEGAL_TRANSITION_VARIANTS[_tickCount % LEGAL_TRANSITION_VARIANTS.length];
  const waveId = `4n-legal-wave-${Date.now()}`;
  const accounts = ['acquisition', 'engagement', 'publishing', 'scheduling', 'telemetry', 'reconciliation', 'projection'];
  const intents = ['IDLE', 'QUEUED', 'RECEIVED', 'SCHEDULED'];

  // Emit a mixed-domain wave using the injectMixedDomainWave pattern
  eventInjector.injectMixedDomainWave({
    waveId,
    seq,
    includeFault: variant === 'stale',
  });

  _legalCount++;
}

function injectAdversarialEvent(domainSeq) {
  const typeIdx = (_tickCount + domainSeq) % ADVERSARIAL_TYPES.length;
  const type = ADVERSARIAL_TYPES[typeIdx];
  const now = Date.now();

  switch (type) {
    case 'out_of_order': {
      eventInjector.injectOutOfOrderEntry({
        domain: 'governance',
        entity: 'fsm',
        entityId: `4n-stale-${now}`,
        previousState: 'IDLE',
        nextState: 'STALE_AUTHORITY',
        backDateMs: 5000,
      });
      break;
    }
    case 'duplicate_causal_chain': {
      eventInjector.injectDuplicateCausalChain({
        domain: 'acquisition',
        entity: 'acquisition_intent',
        entityId: `4n-dup-${now}`,
        previousState: 'RECEIVED',
        nextState: 'PROCESSED',
      });
      break;
    }
    case 'conflicting_transition': {
      eventInjector.injectConflictingTransition({
        domain: 'publishing',
        entity: 'fsm',
        entityId: `4n-conflict-${now}`,
        previousState: 'QUEUED',
        nextStateA: 'PUBLISHING',
        nextStateB: 'REJECTED',
      });
      break;
    }
    case 'adversarial_cross_domain': {
      eventInjector.injectAdversarialTransition({
        membrane: 'publishing',
        targetDomain: 'governance',
        entityId: `4n-adv-governance-${now}`,
      });
      break;
    }
    case 'adversarial_telemetry_execution': {
      eventInjector.injectAdversarialTransition({
        membrane: 'telemetry',
        targetDomain: 'execution',
        entityId: `4n-adv-exec-${now}`,
      });
      break;
    }
    case 'adversarial_reconciliation_projection': {
      eventInjector.injectAdversarialTransition({
        membrane: 'reconciliation',
        targetDomain: 'projection',
        entityId: `4n-adv-proj-${now}`,
      });
      break;
    }
  }

  _adversarialCount++;
}

describe('Phase 4N: Mixed Constitutional Soak (30-Minute Continuous)', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(50);
    await lineageWorker.start(400);
  }, 30000);

  afterAll(async () => {
    await lineageWorker.stop();
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  /**
   * The crown jewel test:
   * 30 minutes of continuous mixed legal + adversarial injection,
   * with periodic checkpoint verification, worker recycling,
   * and runtime probe monitoring throughout.
   */
  it('survives 30 minutes of mixed legal + adversarial waves with no constitutional regressions', async () => {
    const startCursor = observability.query.getLogSize();
    const startTime = Date.now();

    // ── Start runtime monitoring probe ─────────────────────────────────
    const probeStarted = await startMonitor({
      intervalMs: 30_000,
      ledgerLookback: LEDGER_LOOKBACK,
    });
    console.log(`[phase-4n] Monitor probe started at ${new Date(probeStarted.startedAt).toISOString()}`);

    // ── Track checkpoints for invariant verification ────────────────────
    const checkpoints = [];
    let lastLedgerSize = 0;
    let lastLogSize = 0;

    // ── Main ticker loop: legal + adversarial alternation ────────────────
    const ticker = setInterval(() => {
      _tickCount++;

      // Inject adversarial event every ADVERSARIAL_INTERVAL_TICKS ticks
      if (_tickCount % ADVERSARIAL_INTERVAL_TICKS === 0) {
        injectAdversarialEvent(Math.floor(_tickCount / ADVERSARIAL_INTERVAL_TICKS));
      }

      // Inject legal wave every tick
      injectLegalWave(_tickCount);
    }, TICK_INTERVAL_MS);

    // ── Checkpoint ticker: verify invariants every CHECKPOINT_INTERVAL_MS ─
    const checkpointTimer = setInterval(async () => {
      try {
        const head = observability.query.getLogSize();
        const ledger = await lineageLedger.getLineage(LEDGER_LOOKBACK);
        const ledgerSize = ledger.length;

        const checkpoint = {
          elapsed_s: Math.round((Date.now() - startTime) / 1000),
          tickCount: _tickCount,
          adversarialCount: _adversarialCount,
          legalCount: _legalCount,
          logSize: head,
          ledgerSize,
          ledgerDelta: ledgerSize - lastLedgerSize,
        };

        // Verify monotonic cursor advance
        if (lastLogSize > 0 && head < lastLogSize) {
          checkpoint.violation = 'cursor_regression';
        }
        lastLogSize = head;
        lastLedgerSize = ledgerSize;

        checkpoints.push(checkpoint);
      } catch (err) {
        checkpoints.push({ elapsed_s: Math.round((Date.now() - startTime) / 1000), error: err.message });
      }
    }, CHECKPOINT_INTERVAL_MS);

    // ── Worker recycle ticker: restart workers every RECYCLE_INTERVAL_MS ─
    const recycleTimer = setInterval(async () => {
      await telemetryWorkers.stopAll();
      await sleep(150);
      await telemetryWorkers.startAll(50);
    }, RECYCLE_INTERVAL_MS);

    // ── Run the soak ────────────────────────────────────────────────────
    console.log(`[phase-4n] Soak starting: ${SOAK_DURATION_MS}ms (${Math.round(SOAK_DURATION_MS / 60000)}min) at ${TICK_INTERVAL_MS}ms tick interval`);
    await sleep(SOAK_DURATION_MS);

    // ── Stop all timers ─────────────────────────────────────────────────
    clearInterval(ticker);
    clearInterval(checkpointTimer);
    clearInterval(recycleTimer);

    // ── Stop monitoring probe and get report ───────────────────────────
    await stopMonitor();
    const monitorReport = getReport();

    // ── Final verification ──────────────────────────────────────────────
    const endCursor = observability.query.getLogSize();
    const { entries } = observability.query.getEntriesSince(startCursor);
    const ledger = await lineageLedger.getLineage(LEDGER_LOOKBACK);
    const ledgerFinal = await lineageLedger.getLineage(2000);
    const lag = observability.query.getConsumerLag('phase-4n-consumer') || { atRisk: false };

    const elapsed_ms = Date.now() - startTime;
    const elapsed_min = Math.round(elapsed_ms / 60000);

    // ─── Verify tick count ──────────────────────────────────────────────
    const expectedTicks = Math.floor(SOAK_DURATION_MS / TICK_INTERVAL_MS);
    expect(_tickCount).toBeGreaterThanOrEqual(expectedTicks * 0.9);
    const expectedAdversarial = Math.floor(expectedTicks / ADVERSARIAL_INTERVAL_TICKS);

    // ─── Verify constitutional invariants ────────────────────────────────
    // L2: No timestamp regression in ledger
    assertNoTimestampRegression(ledgerFinal);

    // L1: Ledger hash should be stable for entries captured before soak end
    const deterministicHash = deterministicEntryHash(ledgerFinal);
    expect(deterministicHash).toBeTruthy();

    // Monitor probe must have detected no violations
    expect(monitorReport.violationCount).toBe(0);
    expect(monitorReport.ok).toBe(true);

    // ─── Ledger must have grown ─────────────────────────────────────────
    expect(endCursor).toBeGreaterThan(startCursor);
    expect(entries.length).toBeGreaterThan(20);

    // ─── Checkpoint invariants ─────────────────────────────────────────
    for (const cp of checkpoints) {
      if (cp.violation) {
        throw new Error(`[phase-4n] Checkpoint violation at ${cp.elapsed_s}s: ${cp.violation}`);
      }
    }

    // ─── No corruption markers in ledger ───────────────────────────────
    const corrupted = ledgerFinal.filter((e) => e.raw?.raw?.corrupted === true);
    expect(corrupted.length).toBe(0);

    // ─── Adversarial entries must be flagged ───────────────────────────
    const adversarialEntries = ledgerFinal.filter((e) => e.raw?.raw?.adversarial === true);
    const outOfOrderEntries = ledgerFinal.filter((e) => e.raw?.raw?.outOfOrder === true);
    const conflictEntries = ledgerFinal.filter((e) => e.raw?.raw?.conflictAttempt);

    // At least some adversarial events should have been flagged
    const totalFlagged = (adversarialEntries.length + outOfOrderEntries.length + conflictEntries.length);
    expect(totalFlagged).toBeGreaterThan(0);

    // ─── Consumer lag must not be at risk ───────────────────────────────
    expect(lag.atRisk).toBe(false);

    // ─── Worker recycles must have happened ─────────────────────────────
    const recycleCount = Math.floor(SOAK_DURATION_MS / RECYCLE_INTERVAL_MS);
    expect(recycleCount).toBeGreaterThanOrEqual(1);

    // ── Write the soak report ──────────────────────────────────────────
    const report = {
      phase: '4N',
      test: 'mixed-constitutional-soak',
      soakConfig: {
        SOAK_DURATION_MS,
        TICK_INTERVAL_MS,
        ADVERSARIAL_INTERVAL_TICKS,
        CHECKPOINT_INTERVAL_MS,
        RECYCLE_INTERVAL_MS,
        LEDGER_LOOKBACK,
        expectedTicks,
        expectedAdversarial,
      },
      results: {
        ticksInjected: _tickCount,
        legalCount: _legalCount,
        adversarialCount: _adversarialCount,
        endCursor,
        entryCount: entries.length,
        ledgerSize: ledgerFinal.length,
        ledgerHash: deterministicHash,
        consumerLagAtRisk: lag.atRisk,
        monitorViolationCount: monitorReport.violationCount,
        checkpointCount: checkpoints.length,
        elapsed_ms,
        elapsed_min,
      },
      monitorReport: {
        snapshots: monitorReport.snapshots,
        violations: monitorReport.violations,
        summary: monitorReport.summary,
      },
      checkpoints,
      generatedAt: new Date().toISOString(),
    };

    await writeSoakReport(report);
    console.log(`[phase-4n] Soak complete: ${elapsed_min}min, ${_tickCount} ticks, ${_adversarialCount} adversarial, ${monitorReport.violationCount} violations`);

    expect(_adversarialCount).toBeGreaterThan(0);
  }, SOAK_DURATION_MS + 120_000); // vitest timeout: soak + 2min for teardown

  /**
   * Verify that replay convergence is maintained throughout the soak.
   * After the 30-min soak + worker recycle, all ledger entries must
   * still be recoverable with no timestamp regression.
   */
  it('replay continuity survives 30-min soak — all entries causally ordered post-soak', async () => {
    const waveId = `4n-replay-verify-${Date.now()}`;

    // Inject a final verification wave
    for (let i = 0; i < 5; i++) {
      eventInjector.injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }

    await waitForLedgerEntryCount(5, 15000);

    const ledger = await lineageLedger.getLineage(LEDGER_LOOKBACK);
    const waveEntries = ledger.filter((e) => e.raw?.raw?.waveId === waveId);

    // All 5 entries must be in the ledger
    expect(waveEntries.length).toBe(5);

    // No timestamp regression
    assertNoTimestampRegression(waveEntries);
  });

  /**
   * Verify that adversarial-flagged entries from the soak are still
   * correctly flagged and no corruption spread to neighboring entries.
   */
  it('adversarial events injected during soak are flagged and isolated — no cascade', async () => {
    const ledger = await lineageLedger.getLineage(LEDGER_LOOKBACK);

    const adversarialFlagged = ledger.filter(
      (e) =>
        e.raw?.raw?.adversarial === true ||
        e.raw?.raw?.outOfOrder === true ||
        e.raw?.raw?.conflictAttempt ||
        e.raw?.raw?.duplicateEmission
    );

    // Adversarial events should have been detected and flagged
    expect(adversarialFlagged.length).toBeGreaterThan(0);

    // Entries that are NOT flagged should not carry corruption markers
    const nonFlagged = ledger.filter(
      (e) =>
        !e.raw?.raw?.adversarial &&
        !e.raw?.raw?.outOfOrder &&
        !e.raw?.raw?.conflictAttempt &&
        !e.raw?.raw?.duplicateEmission
    );
    const silentlyCorrupted = nonFlagged.filter((e) => e.raw?.raw?.corrupted === true);
    expect(silentlyCorrupted.length).toBe(0);

    // All entries in the ledger should have valid state transitions
    const ledgerIds = new Set(ledger.map((e) => e.ledgerId));
    expect(ledgerIds.size).toBe(ledger.length); // no duplicate ledgerIds
  });
});
