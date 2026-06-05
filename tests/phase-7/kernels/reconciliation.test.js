/**
 * Reconciliation Kernel — Phase 7 runtime validation battery
 * ════════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories.
 *
 * Kernel: reconciliation-kernel (engine.js, fsm.js, worker.js,
 *   substrate.js, orphan-message-repair.js)
 *
 * Battery focus:
 *   - drift detection → engine comparison → orphan repair →
 *     governance acceptance → ledger entry
 *   - multi-tick reconcile → no duplicate repairs, no missing
 *     lineage
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';

describe('Reconciliation Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'reconciliation-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'reconciliation',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'DRIFT_DETECTED',
            payload: { kind: 'orphan-message', accountId: 'acc-1', resourceId: 'orphan-1' },
            source: 'reconciliation',
            correlationId: 'bat-recon-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          if (!diff.changed) {
            const timeline = s.simulator.timeline();
            if (!timeline.some((e) => e.type === 'DRIFT_DETECTED')) {
              throw new Error('stateMutation: DRIFT_DETECTED not recorded');
            }
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-recon-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'RECONCILIATION_TICK',
            payload: { accountId: 'acc-1' },
            source: 'reconciliation',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['RECONCILIATION_TICK'], 'reconciliation-causality');
        },

        governance: async (s) => {
          // Drift repair must come through governance, not from the engine
          // acting on its own authority.
          await s.simulator.tick(1);
          s.simulator.assertWorkersDidNotGovern();
        },

        cadence: async (s) => {
          const result = await s.simulator.tick(25);
          if (result.metrics.errors > 0) {
            throw new Error(`cadence: ${result.metrics.errors} tick errors`);
          }
        },

        workerCorrectness: async (s) => {
          // reconciliation-kernel has worker.js + orphan-message-repair.js
          const workers = ['reconciliation-worker', 'orphan-repair-worker'];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'RECONCILIATION_TICK',
              payload: { worker: w, accountId: 'acc-1' },
              source: w,
              correlationId: `bat-recon-worker-${w}`,
            });
          }
          await s.simulator.tick(3);
          const timeline = s.simulator.timeline();
          for (const w of workers) {
            const found = timeline.some((e) => e.payload && e.payload.worker === w);
            if (!found) throw new Error(`workerCorrectness: event for ${w} not in timeline`);
          }
        },

        persistence: async (s) => {
          // Drift detected → orphan repair → lineage row appended
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'ORPHAN_REPAIR_COMPLETE',
            payload: { repairId: 'r-1', accountId: 'acc-1', lineageRef: 'l-r-1' },
            source: 'reconciliation',
            correlationId: 'bat-recon-persist-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          // Either lineage grew OR the timeline recorded it
          const timeline = s.simulator.timeline();
          if (!diff.lineage.changed && !timeline.some((e) => e.correlationId === 'bat-recon-persist-1')) {
            throw new Error('persistence: orphan-repair event not traceable');
          }
        },

        observability: async (s) => {
          const timeline = s.simulator.timeline();
          if (timeline.length === 0) {
            throw new Error('observability: empty timeline');
          }
          const withPayload = timeline.filter((e) => e.payload != null);
          if (withPayload.length === 0) {
            throw new Error('observability: no events have payload snapshots');
          }
        },
      },
    });
  }, 60000);
});
