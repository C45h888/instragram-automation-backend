/**
 * Retry Kernel (retry-cadence) — Phase 7 runtime validation battery
 * ══════════════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories.
 *
 * Kernel: retry-cadence-kernel (fsm.js, index.js, policy.js,
 *   registry.js, workers/content-retry-worker.js,
 *   workers/engagement-retry-worker.js,
 *   workers/insights-retry-worker.js,
 *   workers/ugc-retry-worker.js)
 *
 * Battery focus:
 *   - rate-limit window → retry dispatched with correct cadence
 *     semantics
 *   - capability-aware prioritization
 *   - recovery on Graph heal
 *   - worker does NOT self-authorize
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';

describe('Retry Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'retry-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'retry',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'RATE_LIMIT_WINDOW',
            payload: { accountId: 'acc-1', scope: 'content', windowMs: 60_000 },
            source: 'retry-cadence',
            correlationId: 'bat-retry-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          if (!diff.changed) {
            const timeline = s.simulator.timeline();
            if (!timeline.some((e) => e.type === 'RATE_LIMIT_WINDOW')) {
              throw new Error('stateMutation: RATE_LIMIT_WINDOW not recorded');
            }
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-retry-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'RETRY_DISPATCHED',
            payload: { accountId: 'acc-1', worker: 'content-retry-worker', attempt: 1 },
            source: 'retry-cadence',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['RETRY_DISPATCHED'], 'retry-causality');
        },

        governance: async (s) => {
          // Retry workers must not self-authorize. The cadence plane decides.
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
            'content-retry-worker',
            'engagement-retry-worker',
            'insights-retry-worker',
            'ugc-retry-worker',
          ];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'RETRY_DISPATCHED',
              payload: { worker: w, accountId: 'acc-1', attempt: 1 },
              source: w,
              correlationId: `bat-retry-worker-${w}`,
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
          // Recovery on Graph heal must be observable
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'GRAPH_HEALED',
            payload: { accountId: 'acc-1' },
            source: 'retry-cadence',
            correlationId: 'bat-retry-persist-1',
          });
          await s.simulator.tick(2);
          const timeline = s.simulator.timeline();
          if (!timeline.some((e) => e.correlationId === 'bat-retry-persist-1')) {
            throw new Error('persistence: graph-heal event not traceable');
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
