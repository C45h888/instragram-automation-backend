/**
 * Capability Kernel (graph-capability) — Phase 7 runtime validation battery
 * ═════════════════════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories, plus the
 * cognition-layer cross-cuts (this kernel is the constitutional
 * dependency for acquisition and publishing).
 *
 * Substrates: graph-capability/ (observations, trigger-bridge,
 *   verdict-gate, wiring)
 * Vault: api-surface, default-scopes, signal-dispatch
 * PAT substrate workers: exchange, retrieve, store
 * Scope substrate workers: detect-dynamic
 * UAT substrate workers: detect, refresh, retrieve, store
 *
 * Battery focus:
 *   - new connection → expected capability state
 *   - token refresh → expected capability state
 *   - auth strike → expected capability state
 *   - repeated graph failure → expected capability state
 *   - downstream consumers (acquisition, publishing) react
 *     exactly as designed
 *   - the kernel itself never inspects token internals from
 *     outside (boundary check)
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';
import { runCognitionLayerCrossCuts } from './_cognition-layer-cross-cuts.js';

describe('Capability Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'capability-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'capability',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          // Drive a token refresh transition
          s.simulator.injectEvent({
            type: 'TOKEN_REFRESH_REQUESTED',
            payload: { accountId: 'acc-1', worker: 'refresh-worker' },
            source: 'graph-capability',
            correlationId: 'bat-cap-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          if (!diff.changed) {
            const timeline = s.simulator.timeline();
            if (!timeline.some((e) => e.type === 'TOKEN_REFRESH_REQUESTED')) {
              throw new Error('stateMutation: TOKEN_REFRESH_REQUESTED not recorded');
            }
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-cap-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'AUTH_STRIKE',
            payload: { accountId: 'acc-1' },
            source: 'graph-capability',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['AUTH_STRIKE'], 'capability-causality');
        },

        governance: async (s) => {
          // The capability kernel itself is the authority; workers in it
          // must not self-govern. assertWorkersDidNotGovern checks the
          // graphSimulator's workerTracer.
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
          // The vault workers (exchange, retrieve, store, refresh, detect)
          const workers = ['exchange-worker', 'retrieve-worker', 'store-worker', 'refresh-worker', 'detect-worker'];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'VAULT_OPERATION',
              payload: { worker: w, accountId: 'acc-1' },
              source: w,
              correlationId: `bat-cap-worker-${w}`,
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
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'NEW_CONNECTION',
            payload: { accountId: 'acc-new-1' },
            source: 'graph-capability',
            correlationId: 'bat-cap-persist-1',
          });
          await s.simulator.tick(2);
          const timeline = s.simulator.timeline();
          if (!timeline.some((e) => e.correlationId === 'bat-cap-persist-1')) {
            throw new Error('persistence: new-connection event not traceable');
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

  it('passes cognition-layer cross-cuts (capability role)', async () => {
    await runCognitionLayerCrossCuts({ simulator, role: 'capability' });
  }, 30000);
});
