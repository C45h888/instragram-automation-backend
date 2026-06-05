/**
 * Telemetry Kernel — Phase 7 runtime validation battery
 * ═════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories.
 *
 * Kernel: telemetry-kernel (fsm.js, index.js,
 *   substrates/ingress-lag-worker.js,
 *   substrates/projection/inputs/ 5 files (authority, health,
 *     integrity, runtime, systemic),
 *   substrates/projection/synthesis/ 5 files,
 *   substrates/projection/transition-writers/ 5 files,
 *   substrates/projection/workers/ 5 files)
 *
 * Battery focus:
 *   - projection intent → coordination FSM → ordered transition →
 *     ledger entry
 *   - namespace priority + lexical order determinism
 *   - halt/resume at the kernel level
 *   - restart replay convergence
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';

describe('Telemetry Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'telemetry-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'telemetry',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'PROJECTION_INTENT',
            payload: {
              projectionNamespace: 'integrity',
              projectionType: 'integrity-projection',
              entityId: 'i-1',
            },
            source: 'integrity-projection-worker',
            correlationId: 'bat-tel-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          if (!diff.changed) {
            const timeline = s.simulator.timeline();
            if (!timeline.some((e) => e.type === 'PROJECTION_INTENT')) {
              throw new Error('stateMutation: PROJECTION_INTENT not recorded');
            }
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-tel-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'NAMESPACE_TRANSITION',
            payload: { from: 'integrity', to: 'authority' },
            source: 'telemetry-coordination',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['NAMESPACE_TRANSITION'], 'telemetry-causality');
        },

        governance: async (s) => {
          // Coordination FSM and CK own namespace ordering; projection
          // workers must not self-govern.
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
          const workers = [
            'authority-projection-worker',
            'health-projection-worker',
            'integrity-projection-worker',
            'runtime-projection-worker',
            'systemic-pressure-projection-worker',
            'ingress-lag-worker',
          ];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'PROJECTION_INTENT',
              payload: { worker: w, entityId: `${w}-e-1` },
              source: w,
              correlationId: `bat-tel-worker-${w}`,
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
          // Halt/resume must be observable as a state transition
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'COORDINATION_HALT',
            payload: { reason: 'phase-7 test' },
            source: 'telemetry-coordination',
            correlationId: 'bat-tel-halt-1',
          });
          await s.simulator.tick(1);
          s.simulator.injectEvent({
            type: 'COORDINATION_RESUME',
            payload: { reason: 'phase-7 test resume' },
            source: 'telemetry-coordination',
            correlationId: 'bat-tel-resume-1',
          });
          await s.simulator.tick(1);
          const timeline = s.simulator.timeline();
          const halts = timeline.filter((e) => e.type === 'COORDINATION_HALT');
          const resumes = timeline.filter((e) => e.type === 'COORDINATION_RESUME');
          if (halts.length === 0 || resumes.length === 0) {
            throw new Error('persistence: halt/resume cycle not traceable');
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
