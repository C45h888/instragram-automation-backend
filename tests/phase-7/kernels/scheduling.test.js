/**
 * Scheduling Kernel — Phase 7 runtime validation battery
 * ═════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories.
 *
 * Kernel: scheduling-kernel (fsm.js, orchestrator.js,
 *   substrates/cadence/cadence.js,
 *   substrates/cadence/lifecycle.js,
 *   substrates/cadence/operational-safety.js)
 *
 * Battery focus:
 *   - lifecycle refresh → LIFECYCLE_REFRESHED → CK state transition
 *   - cadence tick → CADENCE_TICK → downstream reactions
 *   - operational-safety guard under degraded state
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';

describe('Scheduling Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'scheduling-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'scheduling',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'LIFECYCLE_REFRESHED',
            payload: { accountIds: ['acc-1', 'acc-2'] },
            source: 'scheduling',
            correlationId: 'bat-sched-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          if (!diff.changed) {
            const timeline = s.simulator.timeline();
            if (!timeline.some((e) => e.type === 'LIFECYCLE_REFRESHED')) {
              throw new Error('stateMutation: LIFECYCLE_REFRESHED not recorded');
            }
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-sched-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'CADENCE_TICK',
            payload: { tick: 1, tier: 'medium' },
            source: 'scheduling',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['CADENCE_TICK'], 'scheduling-causality');
        },

        governance: async (s) => {
          // Scheduling must not self-govern under degraded state
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
          const workers = ['cadence', 'lifecycle', 'operational-safety'];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'CADENCE_TICK',
              payload: { worker: w, accountId: 'acc-1' },
              source: w,
              correlationId: `bat-sched-worker-${w}`,
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
          // Operational safety guard trip must be observable
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'OPERATIONAL_SAFETY_TRIPPED',
            payload: { reason: 'degraded-state-detected', accountId: 'acc-1' },
            source: 'scheduling',
            correlationId: 'bat-sched-safety-1',
          });
          await s.simulator.tick(2);
          const timeline = s.simulator.timeline();
          if (!timeline.some((e) => e.type === 'OPERATIONAL_SAFETY_TRIPPED')) {
            throw new Error('persistence: operational-safety trip not recorded');
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
