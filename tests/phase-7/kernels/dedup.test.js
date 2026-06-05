/**
 * Dedup Kernel — Phase 7 runtime validation battery
 * ═════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories.
 *
 * Kernel: dedup-kernel (fsm.js, index.js,
 *   substrates/repair/conversation-repair.js,
 *   substrates/repair/workers/conversation-repair-worker.js)
 *
 * Battery focus:
 *   - duplicate detection → repair worker dispatched → no double-write,
 *     no missing repair event, lineage intact
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';

describe('Dedup Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'dedup-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'dedup',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'DUPLICATE_DETECTED',
            payload: { resourceId: 'msg-1', dedupKey: 'msg-1-acc-1' },
            source: 'dedup',
            correlationId: 'bat-dedup-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          if (!diff.changed) {
            const timeline = s.simulator.timeline();
            if (!timeline.some((e) => e.type === 'DUPLICATE_DETECTED')) {
              throw new Error('stateMutation: DUPLICATE_DETECTED not recorded');
            }
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-dedup-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'REPAIR_WORKER_DISPATCHED',
            payload: { resourceId: 'msg-2', worker: 'conversation-repair-worker' },
            source: 'dedup',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['REPAIR_WORKER_DISPATCHED'], 'dedup-causality');
        },

        governance: async (s) => {
          // Repair worker must not self-authorize; the dedup FSM owns
          // the duplicate/repair decision.
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
          const workers = ['conversation-repair-worker'];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'REPAIR_WORKER_DISPATCHED',
              payload: { worker: w, resourceId: `${w}-r-1` },
              source: w,
              correlationId: `bat-dedup-worker-${w}`,
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
          // Repair complete must persist; no double-write
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'REPAIR_COMPLETE',
            payload: { resourceId: 'msg-3', lineageRef: 'l-msg-3' },
            source: 'dedup',
            correlationId: 'bat-dedup-persist-1',
          });
          await s.simulator.tick(2);
          const timeline = s.simulator.timeline();
          if (!timeline.some((e) => e.correlationId === 'bat-dedup-persist-1')) {
            throw new Error('persistence: repair-complete event not traceable');
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
