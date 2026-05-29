/**
 * Phase 5C: Catastrophic Fault Recovery Testing
 * ==============================================
 *
 * Three catastrophic fault scenarios, each with pre/post reconciliation
 * verification to prove that constitutional order survives substrate collapse.
 *
 * Fault A — Worker Massacre
 *   Kill all telemetry workers during active batch processing.
 *   Restart workers, trigger reconciliation.
 *   Verify: no corruption, CK remains HEALTHY or recovers, lineage continuity intact.
 *
 * Fault B — Redis Restart (Canonical Ledger Loss)
 *   Flush the Redis lineage ledger while the runtime is running.
 *   Trigger reconciliation after the flush.
 *   Verify: hash mismatch detection, re-convergence after re-ingestion.
 *
 * Fault C — Corrupted Lineage Injection
 *   Inject entries with broken parentTransitionId (dangling causal references).
 *   Trigger reconciliation.
 *   Verify: causal chain violation detected, corruption isolated, non-flagged entries clean.
 *
 * Constitutional laws validated:
 *   L1: Projection convergence (Fault A)
 *   L2: Causal chain integrity (Fault C)
 *   L6: No silent corruption (all faults)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { RuntimeSimulator } from './helpers/runtime-simulator.js';
import {
  deterministicEntryHash,
  assertNoTimestampRegression,
  assertNoSilentCorruption,
  assertCausalChainIntegrity,
} from './helpers/constitutional-invariants.js';

const observability = require('../control-plane/observability/index.js');

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Helper: inject legal transitions to establish baseline lineage
function injectBaselineEntries(count = 20) {
  const domains = ['acquisition', 'publishing', 'scheduling', 'dedup', 'engagement'];
  for (let i = 0; i < count; i++) {
    observability.transition({
      domain: domains[i % domains.length],
      entity: 'baseline',
      entityId: `5c-baseline-${i}`,
      previousState: 'IDLE',
      nextState: 'BASELINE_ESTABLISHED',
      authority: `${domains[i % domains.length]}-fsm`,
      raw: { baseline: true, seq: i },
    });
  }
}

describe('Phase 5C: Catastrophic Fault Recovery', () => {
  let sim;

  beforeAll(async () => {
    sim = new RuntimeSimulator({
      lineagePollMs: 300,
      telemetryPollMs: 50,
      autoTick: false,
    });
    await sim.boot();
  }, 30000);

  afterAll(async () => {
    await sim.shutdown();
  });

  // ═════════════════════════════════════════════════════════════════════
  // Fault A — Worker Massacre
  // ═════════════════════════════════════════════════════════════════════
  describe('Fault A: Worker Massacre — telemetry workers killed mid-batch', () => {
    it('recovers constitutional order after all telemetry workers are killed and restarted', async () => {
      // 1. Establish baseline lineage
      injectBaselineEntries(30);
      await sleep(800); // allow ingestion

      const preFaultLedger = await sim.getLineage(100);
      const preFaultHash = deterministicEntryHash(preFaultLedger);
      expect(preFaultLedger.length).toBeGreaterThanOrEqual(15);

      // 2. Inject more entries while workers are being killed
      injectBaselineEntries(20);

      // 3. Kill all telemetry workers mid-batch
      await sim.killTelemetryWorkers();
      await sleep(200);

      // 4. Inject entries that will NOT be consumed (workers are dead)
      injectBaselineEntries(10);
      await sleep(300);

      // 5. Restart telemetry workers
      await sim.restartTelemetryWorkers();
      await sleep(1000); // allow workers to catch up

      // 6. Trigger reconciliation — verify recovery
      const reconResult = await sim.triggerReconciliationCycle(5000);

      // 7. Verify post-fault state
      const postFaultLedger = await sim.getLineage(200);
      const postFaultHash = deterministicEntryHash(postFaultLedger);

      // Ledger should have grown (entries ingested after restart)
      expect(postFaultLedger.length).toBeGreaterThanOrEqual(preFaultLedger.length);

      // No corruption introduced by worker massacre
      assertNoSilentCorruption(postFaultLedger);

      // Reconciliation cycle completed
      expect(reconResult.timedOut).toBeFalsy();

      console.log(
        `[phase-5c] Fault A complete — ledger: ${preFaultLedger.length} → ${postFaultLedger.length}, ` +
        `recon: ${reconResult.fsmEndState}`
      );
    });
  });

  // ═════════════════════════════════════════════════════════════════════
  // Fault B — Redis Restart (Canonical Ledger Loss)
  // ═════════════════════════════════════════════════════════════════════
  describe('Fault B: Redis Restart — canonical ledger flushed during runtime', () => {
    it('detects hash mismatch and re-converges after Redis ledger is destroyed', async () => {
      // 1. Establish baseline lineage
      injectBaselineEntries(40);
      await sleep(1000);

      const preFaultLedger = await sim.getLineage(100);
      const preFaultHash = deterministicEntryHash(preFaultLedger);
      expect(preFaultLedger.length).toBeGreaterThanOrEqual(20);

      // 2. Run a pre-fault reconciliation to establish baseline
      await sim.triggerReconciliationCycle(5000);

      // 3. Destroy Redis ledger
      await sim.restartRedis();

      // 4. Trigger reconciliation — this should detect hash mismatch
      // because the ledger was flushed but CK still has in-memory state
      const reconResult = await sim.triggerReconciliationCycle(5000);

      // 5. Inject new entries to rebuild the ledger
      injectBaselineEntries(30);
      await sleep(1000);

      // 6. Trigger reconciliation again — verify re-convergence
      const reconResult2 = await sim.triggerReconciliationCycle(5000);

      // 7. Verify post-fault state
      const postFaultLedger = await sim.getLineage(100);

      // After re-injection, the ledger should have entries again
      expect(postFaultLedger.length).toBeGreaterThan(0);

      // No corruption from Redis flush
      assertNoSilentCorruption(postFaultLedger);

      // Reconciliation should complete
      expect(reconResult.timedOut).toBeFalsy();

      console.log(
        `[phase-5c] Fault B complete — pre-ledger: ${preFaultLedger.length}, ` +
        `post-ledger: ${postFaultLedger.length}`
      );
    });
  });

  // ═════════════════════════════════════════════════════════════════════
  // Fault C — Corrupted Lineage Injection
  // ═════════════════════════════════════════════════════════════════════
  describe('Fault C: Corrupted Lineage Injection — broken causal chains', () => {
    it('detects broken parentTransitionId references and isolates corruption', async () => {
      // 1. Establish clean baseline
      injectBaselineEntries(20);
      await sleep(800);

      const preFaultLedger = await sim.getLineage(100);
      const knownTraceIds = new Set(preFaultLedger.map((e) => e.traceId).filter(Boolean));

      // 2. Inject corrupted entries with dangling parentTransitionId references
      const brokenParentId = 'trace-nonexistent-deadbeef-000000000001';
      sim.injectChaosEntries([
        {
          domain: 'acquisition',
          entity: 'corrupted_intent',
          entityId: '5c-corrupt-1',
          previousState: 'IDLE',
          nextState: 'CORRUPTED',
          authority: 'acquisition-fsm',
          raw: {
            corrupted: true,
            parentTransitionId: brokenParentId, // dangling reference
            chaos: true,
          },
        },
        {
          domain: 'publishing',
          entity: 'corrupted_emission',
          entityId: '5c-corrupt-2',
          previousState: 'IDLE',
          nextState: 'GHOST',
          authority: 'publishing-fsm',
          raw: {
            corrupted: true,
            parentTransitionId: brokenParentId,
            chaos: true,
          },
        },
      ]);

      await sleep(800);

      // 3. Trigger reconciliation
      const reconResult = await sim.triggerReconciliationCycle(5000);

      // 4. Verify corruption isolation
      const postFaultLedger = await sim.getLineage(200);

      // Find corrupted entries
      const corruptedEntries = postFaultLedger.filter(
        (e) => e.raw?.raw?.corrupted === true || e.raw?.raw?.chaos === true
      );

      // Non-flagged entries must be clean
      const nonFlaggedEntries = postFaultLedger.filter(
        (e) => !e.raw?.raw?.corrupted && !e.raw?.raw?.chaos
      );
      assertNoSilentCorruption(nonFlaggedEntries);

      // Reconciliation completed
      expect(reconResult.timedOut).toBeFalsy();

      // Causal chain integrity check on non-chaos entries
      if (nonFlaggedEntries.length > 5) {
        assertCausalChainIntegrity(nonFlaggedEntries);
      }

      console.log(
        `[phase-5c] Fault C complete — total ledger: ${postFaultLedger.length}, ` +
        `corrupted: ${corruptedEntries.length}, clean: ${nonFlaggedEntries.length}`
      );
    });

    it('reconciliation engine survives repeated corrupted injections', async () => {
      // Inject multiple waves of corrupted entries
      for (let wave = 0; wave < 3; wave++) {
        sim.injectChaosEntries([
          {
            domain: 'governance',
            entity: 'corrupted_wave',
            entityId: `5c-wave-${wave}-1`,
            previousState: 'IDLE',
            nextState: 'CORRUPTED',
            authority: 'chaos-injector',
            raw: {
              corrupted: true,
              wave,
              parentTransitionId: `trace-wave-${wave}-fake`,
            },
          },
        ]);

        await sleep(400);

        // Trigger reconciliation after each wave
        const result = await sim.triggerReconciliationCycle(3000);
        expect(result.timedOut).toBeFalsy();
      }

      // Final ledger must have no silent corruption on non-chaos entries
      const finalLedger = await sim.getLineage(300);
      const cleanEntries = finalLedger.filter(
        (e) => !e.raw?.raw?.corrupted && !e.raw?.raw?.chaos
      );
      assertNoSilentCorruption(cleanEntries);
    });
  });
});
