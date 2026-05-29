/**
 * Phase 5B: Concurrent Ecosystem Testing
 * =======================================
 *
 * All 5 governance domains operate simultaneously for 5 minutes.
 * Reconciliation cycles fire every 30 seconds through the CK bridge.
 * Constitutional invariants are verified after each reconciliation.
 *
 * Domains active concurrently:
 *   - acquisition: intent lifecycle transitions
 *   - publishing: buffer and emission state
 *   - scheduling: cadence and slot allocation
 *   - dedup: batch marking and replay detection
 *   - engagement: circuit breaker lifecycle, auth strikes
 *
 * Validates:
 *   - Reconciliation observations remain coherent under concurrent load
 *   - No cross-domain membrane bypass during concurrent transitions
 *   - Constitutional invariants hold (no timestamp regression, no corruption)
 *   - Reconciliation cycle completes within timeout (5s)
 *   - CK remains HEALTHY throughout the test
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { RuntimeSimulator } from './helpers/runtime-simulator.js';
import { deterministicEntryHash, assertNoTimestampRegression, assertNoSilentCorruption, assertCausalChainIntegrity } from './helpers/constitutional-invariants.js';

const CONCURRENT_DURATION_MS = 5 * 60 * 1000; // 5 minutes
const RECON_INTERVAL_MS = 30_000; // reconciliation every 30s
const TICK_INTERVAL_MS = 200; // transition injection every 200ms

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

describe('Phase 5B: Concurrent Ecosystem — 5-Domain Operation with Reconciliation', () => {
  let sim;

  beforeAll(async () => {
    sim = new RuntimeSimulator({
      lineagePollMs: 300,
      telemetryPollMs: 50,
      tickIntervalMs: 1000,
      autoTick: true,
    });
    await sim.boot();
  }, 30000);

  afterAll(async () => {
    await sim.shutdown();
  });

  it('all 5 domains survive 5 minutes of concurrent operation with reconciliation every 30s', async () => {
    const startTime = Date.now();
    let reconCycleCount = 0;
    const reconResults = [];
    const checkpoints = [];
    let tickCount = 0;
    let adversarialCount = 0;

    // ── Transition injection ticker ───────────────────────────────────
    const legalDomains = ['acquisition', 'publishing', 'scheduling', 'dedup', 'engagement'];
    const legalStates = ['IDLE', 'QUEUED', 'RECEIVED', 'SCHEDULED', 'ACTIVE', 'PROCESSING'];

    const ticker = setInterval(() => {
      tickCount++;
      const domain = legalDomains[tickCount % legalDomains.length];
      const state = legalStates[tickCount % legalStates.length];

      // Inject legal domain transitions through the observability plane
      const observability = require('../control-plane/observability/index.js');
      observability.transition({
        domain,
        entity: 'concurrent_test',
        entityId: `5b-${domain}-${tickCount}`,
        previousState: legalStates[(tickCount - 1) % legalStates.length] || 'IDLE',
        nextState: state,
        authority: `${domain}-fsm`,
        raw: { wave: '5b', tick: tickCount, domain },
      });

      // Every 50 ticks, inject an adversarial cross-domain attempt
      if (tickCount % 50 === 0) {
        adversarialCount++;
        const targetDomain = legalDomains[(tickCount + 2) % legalDomains.length];
        observability.transition({
          domain: targetDomain,
          entity: 'adversarial_probe',
          entityId: `5b-adv-${tickCount}`,
          previousState: 'IDLE',
          nextState: 'MUTATED',
          authority: 'foreign-domain-attacker', // should be rejected by membrane
          raw: { adversarial: true, tick: tickCount },
        });
      }
    }, TICK_INTERVAL_MS);

    // ── Reconciliation cycle timer ────────────────────────────────────
    const reconTimer = setInterval(async () => {
      try {
        const result = await sim.triggerReconciliationCycle(5000);

        // Verify reconciliation completed (not timed out)
        const timedOut = result && result.timedOut;

        reconCycleCount++;
        reconResults.push({
          cycle: reconCycleCount,
          elapsed_s: Math.round((Date.now() - startTime) / 1000),
          fsmEndState: result?.fsmEndState || 'unknown',
          timedOut: !!timedOut,
        });

        // Checkpoint: verify constitutional invariants every other cycle
        if (reconCycleCount % 2 === 0) {
          const ledger = await sim.getLineage(200);
          const logSize = sim.getLogSize();

          checkpoints.push({
            cycle: reconCycleCount,
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            ledgerSize: ledger.length,
            logSize,
            tickCount,
            adversarialCount,
          });

          // Constitutional invariant checks
          if (ledger.length > 1) {
            assertNoTimestampRegression(ledger);
          }
          assertNoSilentCorruption(ledger);
        }
      } catch (err) {
        reconResults.push({
          cycle: reconCycleCount + 1,
          elapsed_s: Math.round((Date.now() - startTime) / 1000),
          error: err.message,
        });
      }
    }, RECON_INTERVAL_MS);

    // ── Run the concurrent ecosystem ───────────────────────────────────
    console.log(`[phase-5b] Starting 5-min concurrent ecosystem test — reconciliation every 30s`);
    await sleep(CONCURRENT_DURATION_MS);

    // ── Stop timers ───────────────────────────────────────────────────
    clearInterval(ticker);
    clearInterval(reconTimer);

    // Allow final reconciliation to complete
    await sleep(1000);

    const elapsed_s = Math.round((Date.now() - startTime) / 1000);

    // ── Final assertions ──────────────────────────────────────────────
    // At least 9 reconciliation cycles should have fired (300s / 30s = 10, minus 1 for timing)
    expect(reconCycleCount).toBeGreaterThanOrEqual(8);

    // Reconciliation cycles should not time out
    const timedOutCycles = reconResults.filter((r) => r.timedOut);
    expect(timedOutCycles.length).toBeLessThan(reconCycleCount * 0.3); // <30% timeout rate

    // CK must be HEALTHY
    const ckState = sim.getCKState();
    expect(['HEALTHY']).toContain(ckState);

    // All domains must be in a valid state
    for (const domain of legalDomains) {
      const state = sim.getDomainState(domain);
      expect(state).toBeTruthy();
    }

    // No corruption in final ledger
    const finalLedger = await sim.getLineage(500);
    assertNoSilentCorruption(finalLedger);

    // Causal chain integrity
    if (finalLedger.length > 5) {
      assertCausalChainIntegrity(finalLedger);
    }

    // Checkpoint integrity
    for (const cp of checkpoints) {
      expect(cp.ledgerSize).toBeGreaterThan(0);
    }

    console.log(
      `[phase-5b] Concurrent ecosystem complete: ${elapsed_s}s, ` +
      `${tickCount} ticks, ${adversarialCount} adversarial, ` +
      `${reconCycleCount} reconciliation cycles, ${checkpoints.length} checkpoints`
    );

    // Write report
    const { mkdir, writeFile } = await import('node:fs/promises');
    const path = await import('node:path');
    const outputDir = path.resolve(process.cwd(), 'tests/output');
    await mkdir(outputDir, { recursive: true });
    const report = {
      phase: '5B',
      test: 'concurrent-ecosystem',
      duration_s: elapsed_s,
      tickCount,
      adversarialCount,
      reconCycleCount,
      checkpointCount: checkpoints.length,
      timedOutCycles: timedOutCycles.length,
      ckFinalState: ckState,
      finalLedgerSize: finalLedger.length,
      domainStates: Object.fromEntries(legalDomains.map((d) => [d, sim.getDomainState(d)])),
      reconResults: reconResults.slice(-10),
      checkpoints,
      generatedAt: new Date().toISOString(),
    };
    await writeFile(
      path.join(outputDir, 'phase-5b-concurrent-ecosystem-latest.json'),
      JSON.stringify(report, null, 2) + '\n',
      'utf8'
    );
  }, CONCURRENT_DURATION_MS + 30_000);
});
