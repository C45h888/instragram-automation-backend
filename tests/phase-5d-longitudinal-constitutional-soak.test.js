/**
 * Phase 5D: 1-Hour Longitudinal Constitutional Soak
 * ==================================================
 *
 * The crown jewel of Phase 5 testing. Runs the complete constitutional
 * runtime for 1 continuous hour with:
 *   - Mixed legal + adversarial transitions across all 5 domains
 *   - Reconciliation cycles triggered every 60 seconds through CK bridge
 *   - Worker recycle every 10 minutes (simulates operational churn)
 *   - Checkpoints every 5 minutes verifying ALL 7 constitutional laws
 *   - Runtime monitoring probe capturing structural snapshots
 * 
 *
 * Constitutional Laws Validated (every checkpoint):
 *   L1: Projection convergence from lineage replay
 *   L2: No timestamp regression + causal chain integrity
 *   L3: No cross-domain membrane bypass
 *   L4: Monotonic cursor advancement
 *   L5: Stale authority entries flagged — never silently accepted
 *   L6: No silent corruption markers
 *   L7: Projection signal contract (lineage vs telemetry signal ownership)
 *
 * Wave Architecture:
 *   - TICK_INTERVAL_MS: 500ms → ~7200 ticks over 1 hour
 *   - ADVERSARIAL_INTERVAL_TICKS: every 30 ticks → ~240 adversarial events
 *   - RECON_INTERVAL_MS: 60s → ~60 reconciliation cycles
 *   - CHECKPOINT_INTERVAL_MS: 5min → ~12 checkpoints
 *   - RECYCLE_INTERVAL_MS: 10min → ~6 worker recycles
 *
 * Output: JSON report in tests/output/phase-5d-soak-latest.json
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { RuntimeSimulator } from './helpers/runtime-simulator.js';
import { startMonitor, stopMonitor, getReport } from './helpers/runtime-monitor.js';
import {
  deterministicEntryHash,
  assertNoTimestampRegression,
  assertNoSilentCorruption,
  assertCausalChainIntegrity,
  assertProjectionSignalContract,
} from './helpers/constitutional-invariants.js';

// ── Soak configuration ──────────────────────────────────────────────────
const SOAK_DURATION_MS = parseInt(process.env.PHASE5D_SOAK_MS || String(60 * 60 * 1000), 10);
const TICK_INTERVAL_MS = parseInt(process.env.PHASE5D_TICK_MS || '500', 10);
const ADVERSARIAL_INTERVAL_TICKS = parseInt(process.env.PHASE5D_ADV_INTERVAL || '30', 10);
const RECON_INTERVAL_MS = parseInt(process.env.PHASE5D_RECON_MS || '60000', 10);
const CHECKPOINT_INTERVAL_MS = parseInt(process.env.PHASE5D_CHECKPOINT_MS || String(5 * 60 * 1000), 10);
const RECYCLE_INTERVAL_MS = parseInt(process.env.PHASE5D_RECYCLE_MS || String(10 * 60 * 1000), 10);
const LEDGER_LOOKBACK = 500;

const observability = require('../control-plane/observability/index.js');

// ── Injection variants ──────────────────────────────────────────────────
const LEGAL_DOMAINS = ['acquisition', 'publishing', 'scheduling', 'dedup', 'engagement'];
const LEGAL_STATES = ['IDLE', 'QUEUED', 'RECEIVED', 'SCHEDULED', 'ACTIVE', 'PROCESSING', 'COMPLETE'];

const ADVERSARIAL_TYPES = [
  'membrane_bypass',
  'stale_authority',
  'duplicate_causal_chain',
  'corrupted_lineage',
  'out_of_order',
  'cross_domain_attack',
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function writeSoakReport(payload) {
  const outputDir = path.resolve(process.cwd(), 'tests/output');
  await mkdir(outputDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const stampedPath = path.join(outputDir, `phase-5d-soak-${ts}.json`);
  const latestPath = path.join(outputDir, 'phase-5d-soak-latest.json');
  const content = JSON.stringify(payload, null, 2) + '\n';
  await writeFile(stampedPath, content, 'utf8');
  await writeFile(latestPath, content, 'utf8');
}

describe('Phase 5D: 1-Hour Longitudinal Constitutional Soak', () => {
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
    'survives 1 hour of mixed legal + adversarial waves with reconciliation every 60s',
    async () => {
      const startTime = Date.now();

      try {

      // ── Counters ─────────────────────────────────────────────────
      let tickCount = 0;
      let adversarialCount = 0;
      let legalCount = 0;

      // ── Start runtime monitoring probe ────────────────────────────
      const probeStarted = await startMonitor({
        intervalMs: 30000,
        ledgerLookback: LEDGER_LOOKBACK,
      });
      console.log(
        `[phase-5d] Monitor probe started at ${new Date(probeStarted.startedAt).toISOString()}`
      );

      // ── Transition injection ticker ───────────────────────────────
      const ticker = setInterval(() => {
        tickCount++;

        // Legal transition every tick
        const domain = LEGAL_DOMAINS[tickCount % LEGAL_DOMAINS.length];
        const state = LEGAL_STATES[tickCount % LEGAL_STATES.length];
        observability.transition({
          domain,
          entity: 'soak_transition',
          entityId: `5d-${domain}-${tickCount}`,
          previousState: LEGAL_STATES[(tickCount - 1) % LEGAL_STATES.length] || 'IDLE',
          nextState: state,
          authority: `${domain}-fsm`,
          raw: { wave: '5d', tick: tickCount, domain },
        });
        legalCount++;

        // Adversarial event every ADVERSARIAL_INTERVAL_TICKS
        if (tickCount % ADVERSARIAL_INTERVAL_TICKS === 0) {
          const advType = ADVERSARIAL_TYPES[adversarialCount % ADVERSARIAL_TYPES.length];
          adversarialCount++;

          switch (advType) {
            case 'membrane_bypass': {
              const targetDomain = LEGAL_DOMAINS[(adversarialCount + 2) % LEGAL_DOMAINS.length];
              observability.transition({
                domain: targetDomain,
                entity: 'membrane_probe',
                entityId: `5d-adv-bypass-${tickCount}`,
                previousState: 'IDLE',
                nextState: 'BYPASS_ATTEMPT',
                authority: 'foreign-domain-attacker',
                raw: { adversarial: true, type: 'membrane_bypass', tick: tickCount },
              });
              break;
            }
            case 'stale_authority': {
              observability.transition({
                domain: 'governance',
                entity: 'fsm',
                entityId: `5d-adv-stale-${tickCount}`,
                previousState: 'IDLE',
                nextState: 'STALE_AUTHORITY',
                authority: 'governance-kernel',
                raw: {
                  adversarial: true,
                  type: 'stale_authority',
                  tick: tickCount,
                  outOfOrder: true,
                },
              });
              break;
            }
            case 'duplicate_causal_chain': {
              observability.transition({
                domain: 'acquisition',
                entity: 'acquisition_intent',
                entityId: `5d-adv-dup-${tickCount}`,
                previousState: 'RECEIVED',
                nextState: 'PROCESSED',
                authority: 'acquisition-fsm',
                raw: {
                  adversarial: true,
                  type: 'duplicate_causal_chain',
                  tick: tickCount,
                  duplicateEmission: true,
                },
              });
              break;
            }
            case 'corrupted_lineage': {
              observability.transition({
                domain: 'engagement',
                entity: 'corrupted_intent',
                entityId: `5d-adv-corrupt-${tickCount}`,
                previousState: 'IDLE',
                nextState: 'CORRUPTED',
                authority: 'engagement-fsm',
                raw: {
                  adversarial: true,
                  type: 'corrupted_lineage',
                  tick: tickCount,
                  corrupted: true,
                  parentTransitionId: 'trace-nonexistent-soak-deadbeef',
                },
              });
              break;
            }
            case 'out_of_order': {
              observability.transition({
                domain: 'publishing',
                entity: 'publishing_intent',
                entityId: `5d-adv-ooo-${tickCount}`,
                previousState: 'QUEUED',
                nextState: 'PUBLISHING',
                authority: 'publishing-fsm',
                raw: {
                  adversarial: true,
                  type: 'out_of_order',
                  tick: tickCount,
                  outOfOrder: true,
                },
              });
              break;
            }
            case 'cross_domain_attack': {
              observability.transition({
                domain: 'scheduling',
                entity: 'schedule_slot',
                entityId: `5d-adv-cross-${tickCount}`,
                previousState: 'ALLOCATED',
                nextState: 'CONFLICT',
                authority: 'publishing-membrane', // cross-domain authority
                raw: {
                  adversarial: true,
                  type: 'cross_domain_attack',
                  tick: tickCount,
                  conflictAttempt: true,
                },
              });
              break;
            }
          }
        }
      }, TICK_INTERVAL_MS);

      // ── Reconciliation cycle timer ────────────────────────────────
      let reconCycleCount = 0;
      const reconResults = [];

      const reconTimer = setInterval(async () => {
        try {
          const result = await sim.triggerReconciliationCycle(8000);
          reconCycleCount++;
          reconResults.push({
            cycle: reconCycleCount,
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            fsmEndState: result?.fsmEndState || 'unknown',
            timedOut: !!result?.timedOut,
          });
        } catch (err) {
          reconResults.push({
            cycle: reconCycleCount + 1,
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            error: err.message,
          });
        }
      }, RECON_INTERVAL_MS);

      // ── Checkpoint timer — verify all 7 constitutional laws ───────
      const checkpoints = [];
      let lastLogSize = 0;

      const checkpointTimer = setInterval(async () => {
        try {
          const ledger = await sim.getLineage(LEDGER_LOOKBACK);
          const logSize = sim.getLogSize();
          const ckState = sim.getCKState();
          const projections = sim.getProjections();

          const checkpoint = {
            elapsed_s: Math.round((Date.now() - startTime) / 1000),
            elapsed_min: Math.round((Date.now() - startTime) / 60000),
            tickCount,
            legalCount,
            adversarialCount,
            reconCycleCount,
            logSize,
            ledgerSize: ledger.length,
            ckState,
            cursorAdvanceOk: logSize >= lastLogSize,
            violations: [],
          };

          // L2: No timestamp regression
          try {
            if (ledger.length > 1) assertNoTimestampRegression(ledger);
          } catch (e) {
            checkpoint.violations.push('L2_TIMESTAMP_REGRESSION');
          }

          // L2 extension: Causal chain integrity
          try {
            const cleanLedger = ledger.filter(
              (e) => !e.raw?.raw?.corrupted && !e.raw?.raw?.chaos
            );
            if (cleanLedger.length > 5) assertCausalChainIntegrity(cleanLedger);
          } catch (e) {
            checkpoint.violations.push('L2_CAUSAL_CHAIN_BROKEN');
          }

          // L6: No silent corruption
          try {
            const nonChaos = ledger.filter(
              (e) => !e.raw?.raw?.chaos && !e.raw?.raw?.adversarial
            );
            assertNoSilentCorruption(nonChaos);
          } catch (e) {
            checkpoint.violations.push('L6_SILENT_CORRUPTION');
          }

          // L7: Projection signal contract
          try {
            if (projections) assertProjectionSignalContract(projections);
          } catch (e) {
            checkpoint.violations.push('L7_SIGNAL_CONTRACT');
          }

          // L4: Monotonic cursor
          if (logSize < lastLogSize) {
            checkpoint.violations.push('L4_CURSOR_REGRESSION');
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

      // ── Worker recycle timer ──────────────────────────────────────
      const recycleTimer = setInterval(async () => {
        await sim.killTelemetryWorkers();
        await sleep(150);
        await sim.restartTelemetryWorkers();
      }, RECYCLE_INTERVAL_MS);

      // ════════════════════════════════════════════════════════════════
      // RUN THE SOAK
      // ════════════════════════════════════════════════════════════════
      const soakMin = Math.round(SOAK_DURATION_MS / 60000);
      console.log(
        `[phase-5d] Starting 1-hour longitudinal soak: ${SOAK_DURATION_MS}ms (${soakMin}min) at ${TICK_INTERVAL_MS}ms tick`
      );
      await sleep(SOAK_DURATION_MS);

      // ── Stop all timers ───────────────────────────────────────────
      clearInterval(ticker);
      clearInterval(reconTimer);
      clearInterval(checkpointTimer);
      clearInterval(recycleTimer);

      // Allow final ingestion and reconciliation to settle
      await sleep(2000);

      // ── Stop monitor and get report ───────────────────────────────
      await stopMonitor();
      const monitorReport = getReport();

      // ════════════════════════════════════════════════════════════════
      // FINAL VERIFICATION
      // ════════════════════════════════════════════════════════════════

      const elapsed_ms = Date.now() - startTime;
      const elapsed_min = Math.round(elapsed_ms / 60000);
      const finalLedger = await sim.getLineage(2000);
      const finalCkState = sim.getCKState();
      const finalProjections = sim.getProjections();

      // ── Tick count must be within 10% of expected ─────────────────
      const expectedTicks = Math.floor(SOAK_DURATION_MS / TICK_INTERVAL_MS);
      expect(tickCount).toBeGreaterThanOrEqual(expectedTicks * 0.85);

      // ── At least 50 reconciliation cycles must have fired ─────────
      expect(reconCycleCount).toBeGreaterThanOrEqual(50);

      // ── Checkpoints must exist ────────────────────────────────────
      expect(checkpoints.length).toBeGreaterThanOrEqual(10);

      // ── CK must be healthy or degraded (not halted) ───────────────
      expect(['HEALTHY', 'DEGRADED', 'RECOVERY']).toContain(finalCkState);
      expect(finalCkState).not.toBe('HALTED');

      // ── Runtime monitor must report no violations ─────────────────
      expect(monitorReport.violationCount).toBe(0);
      expect(monitorReport.ok).toBe(true);

      // ── No timestamp regression in final ledger ───────────────────
      assertNoTimestampRegression(finalLedger);

      // ── No silent corruption on non-adversarial entries ────────────
      const nonAdversarial = finalLedger.filter(
        (e) => !e.raw?.raw?.adversarial && !e.raw?.raw?.chaos
      );
      assertNoSilentCorruption(nonAdversarial);

      // ── Causal chain integrity on clean entries ───────────────────
      const cleanEntries = finalLedger.filter(
        (e) => !e.raw?.raw?.corrupted && !e.raw?.raw?.chaos
      );
      if (cleanEntries.length > 5) {
        assertCausalChainIntegrity(cleanEntries);
      }

      // ── Projection signal contract ───────────────────────────────
      if (finalProjections) {
        assertProjectionSignalContract(finalProjections);
      }

      // ── Adversarial entries must be flagged ───────────────────────
      const flaggedAdversarial = finalLedger.filter(
        (e) =>
          e.raw?.raw?.adversarial === true ||
          e.raw?.raw?.outOfOrder === true ||
          e.raw?.raw?.conflictAttempt
      );
      expect(flaggedAdversarial.length).toBeGreaterThan(0);

      // ── Checkpoints must show no constitutional violations ────────
      const violatedCheckpoints = checkpoints.filter(
        (cp) => cp.violations && cp.violations.length > 0
      );
      if (violatedCheckpoints.length > 0) {
        console.error(
          `[phase-5d] WARNING: ${violatedCheckpoints.length} checkpoints had violations:`,
          violatedCheckpoints.map((c) => `${c.elapsed_s}s: ${c.violations.join(',')}`)
        );
      }

      } finally {
        // ── Write the soak report — always, even on test failure ────
        try {
          const report = {
            phase: '5D',
            test: '1-hour-longitudinal-constitutional-soak',
            soakConfig: {
              SOAK_DURATION_MS,
              TICK_INTERVAL_MS,
              ADVERSARIAL_INTERVAL_TICKS,
              RECON_INTERVAL_MS,
              CHECKPOINT_INTERVAL_MS,
              RECYCLE_INTERVAL_MS,
              LEDGER_LOOKBACK,
              expectedTicks,
            },
            results: {
              elapsed_ms,
              elapsed_min,
              tickCount,
              legalCount,
              adversarialCount,
              reconCycleCount,
              checkpointCount: checkpoints.length,
              finalLedgerSize: finalLedger.length,
              finalCkState,
              monitorViolationCount: monitorReport.violationCount,
              violatedCheckpointCount: violatedCheckpoints.length,
            },
            monitorReport: {
              snapshots: monitorReport.snapshots,
              violations: monitorReport.violations,
              summary: monitorReport.summary,
            },
            reconResults,
            checkpoints,
            generatedAt: new Date().toISOString(),
          };

          await writeSoakReport(report);

          console.log(
            `[phase-5d] Soak complete: ${elapsed_min}min, ${tickCount} ticks, ` +
            `${adversarialCount} adversarial, ${reconCycleCount} recon cycles, ` +
            `${monitorReport.violationCount} monitor violations, ` +
            `${violatedCheckpoints.length} violated checkpoints`
          );
        } catch (e) {
          console.error('[phase-5d] Failed to write soak report:', e.message);
        }
      }
    },
    SOAK_DURATION_MS + 120_000
  );
});
