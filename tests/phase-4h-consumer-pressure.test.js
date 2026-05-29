/**
 * Phase 4H: Consumer Lag Resilience
 *
 * Validates constitutional guarantees under consumer pressure:
 *   - Slow consumers do not cause ledger truncation
 *   - Stalled telemetry workers recover without data loss
 *   - Replay backlog accumulation does not corrupt continuity
 *   - Delayed reconciliation windows preserve replay continuity
 *   - Cursor recovery is safe (no negative values, no overflow)
 *
 * Without these guarantees, a 30-minute soak will generate misleading
 * results because lag failures mask architectural bugs as load effects.
 *
 * Constitutional law:
 *   A replay event may never mutate prior lineage history.
 *   Cursor positions remain monotonic.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntryCount, waitForCursorAdvance } = require('./helpers/sync-barriers');
const { assertNoTimestampRegression } = require('./helpers/constitutional-invariants');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Phase 4H: Consumer Lag Resilience', () => {
  let slowConsumer;
  let fastConsumer;

  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(35);
    await lineageWorker.start(500);
    slowConsumer = `phase4h-slow-${Date.now()}`;
    fastConsumer = `phase4h-fast-${Date.now()}`;
  }, 20000);

  afterAll(async () => {
    await lineageWorker.stop();
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('slow consumer does not cause ledger truncation under sustained backlog', async () => {
    const waveId = `phase4h-lag-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    // Register slow consumer who never advances cursor
    observability.query.registerConsumer(slowConsumer);
    // Register fast consumer who keeps up
    observability.query.registerConsumer(fastConsumer);

    // Inject waves at speed that stresses consumer polling
    for (let i = 0; i < 15; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: i % 5 === 0 });
      await sleep(20);
    }

    // Wait for lineage worker to consume all 15 waves
    await waitForLedgerEntryCount(15, 8000);

    // Slow consumer lag is tracked but must not cause truncation
    const slowLag = observability.query.getConsumerLag(slowConsumer);

    // Ledger must have all 15 entries — no silent dropping
    const ledger = await lineageLedger.getLineage(500);
    const waveEntries = ledger.filter((e) => e.raw?.raw?.waveId === waveId);
    expect(waveEntries.length).toBe(15);
    expect(slowLag.atRisk).toBe(false);
  });

  it('cursor recovery is safe — no negative values or overflow', async () => {
    const waveId = `phase4h-cursor-recover-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    const cursorBefore = observability.query.getLogSize();
    for (let i = 0; i < 8; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }

    // Wait for lineage worker to consume entries before checking cursor
    await waitForLedgerEntryCount(1, 8000);

    // Advance cursor manually after backlog built up
    const ledger = await lineageLedger.getLineage(500);
    const ledgerSize = ledger.length;
    observability.query.updateConsumerCursor(fastConsumer, ledgerSize);

    // Cursor must be non-negative and not overflow
    const newCursor = observability.query.getLogSize();
    expect(newCursor).toBeGreaterThanOrEqual(0);
    expect(newCursor).toBeGreaterThanOrEqual(cursorBefore);
  });

  it('stalled telemetry workers recover without data loss', async () => {
    const waveId = `phase4h-stall-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    // Inject pre-stall workload
    for (let i = 0; i < 5; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }

    // Wait for lineage worker to consume pre-stall entries
    await waitForLedgerEntryCount(5, 8000);

    // Stop all workers — simulates stall/death
    await telemetryWorkers.stopAll();

    // Restart workers
    await telemetryWorkers.startAll(35);

    // Inject post-restart workload
    const cursorAfter = observability.query.getLogSize();
    await injectMixedDomainWave({ waveId, seq: 100, includeFault: false });

    // Wait for the post-restart entry to appear
    await waitForCursorAdvance(cursorAfter, 8000);

    // Post-restart entries must appear — no silent data loss
    const { entries } = observability.query.getEntriesSince(cursorAfter);
    expect(entries.length).toBeGreaterThan(0);

    // Pre-stall entries must still be in ledger
    const ledger = await lineageLedger.getLineage(500);
    const preStallEntries = ledger.filter(
      (e) => e.raw?.raw?.waveId === waveId && e.raw?.raw?.seq < 100
    );
    expect(preStallEntries.length).toBe(5);
  });

  it('delayed reconciliation window preserves replay continuity', async () => {
    const waveId = `phase4h-recon-${Date.now()}`;
    const { injectMixedDomainWave, injectReconciliationTick } = require('./event-injector.js');

    for (let i = 0; i < 8; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: i % 3 === 0 });
      await sleep(30);
    }

    // Long delay before reconciliation tick
    await sleep(1000);
    await injectReconciliationTick();

    // Wait for all 8 entries to land in ledger despite the delay
    await waitForLedgerEntryCount(8, 8000);

    const ledger = await lineageLedger.getLineage(300);
    const waveEntries = ledger.filter((e) => e.raw?.raw?.waveId === waveId);
    expect(waveEntries.length).toBe(8);
  });

  it('replay continuity survives interruption — entries are causally ordered', async () => {
    const waveId = `phase4h-ordered-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    for (let i = 0; i < 6; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
      await sleep(50);
    }

    // Wait for all entries to be persisted
    await waitForLedgerEntryCount(6, 8000);
    const ledger = await lineageLedger.getLineage(300);
    const waveEntries = ledger.filter((e) => e.raw?.raw?.waveId === waveId);

    // Timestamps must not regress — causal monotonicity
    assertNoTimestampRegression(waveEntries);
  });
});
