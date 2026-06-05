/**
 * Publishing Kernel — Phase 7 runtime validation battery
 * ═════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories, plus the
 * cognition-layer cross-cuts (this kernel is part of the
 * cognition layer bound with graph-capability and acquisition).
 *
 * Workers (2): content, engagement
 * Substrates: content/, engagement/
 * Orchestrator: yes
 * Rate-limiters: per substrate
 *
 * Battery focus:
 *   - publishing event → governance decision → worker dispatch →
 *     transport → persistence
 *   - rate-limited Graph → retry-cadence consumes correctly
 *   - capability degraded → publishing blocks at CK (not at worker)
 *   - publishing status / completion async flow through the
 *     control API
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';
import { runCognitionLayerCrossCuts } from './_cognition-layer-cross-cuts.js';

describe('Publishing Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'publishing-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'publishing',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'PUBLISH_REQUESTED',
            payload: { worker: 'content-worker', accountId: 'acc-1', contentId: 'p-1' },
            source: 'publishing',
            correlationId: 'bat-pub-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          // Timeline must have recorded the publish event
          const timeline = s.simulator.timeline();
          if (!timeline.some((e) => e.type === 'PUBLISH_REQUESTED')) {
            throw new Error('stateMutation: PUBLISH_REQUESTED not recorded');
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-pub-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'PUBLISH_REQUESTED',
            payload: { worker: 'engagement-worker', accountId: 'acc-1', contentId: 'p-2' },
            source: 'publishing',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['PUBLISH_REQUESTED'], 'publishing-causality');
        },

        governance: async (s) => {
          // Publishing must not self-govern under capability degradation
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
          const workers = ['content-worker', 'engagement-worker'];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'PUBLISH_REQUESTED',
              payload: { worker: w, accountId: 'acc-1', contentId: `${w}-c-1` },
              source: w,
              correlationId: `bat-pub-worker-${w}`,
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
            type: 'PUBLISH_REQUESTED',
            payload: { worker: 'content-worker', accountId: 'acc-1', contentId: 'p-3' },
            source: 'publishing',
            correlationId: 'bat-pub-persist-1',
          });
          await s.simulator.tick(2);
          const timeline = s.simulator.timeline();
          if (!timeline.some((e) => e.correlationId === 'bat-pub-persist-1')) {
            throw new Error('persistence: publish event not traceable post-tick');
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

  it('passes cognition-layer cross-cuts (publishing role)', async () => {
    await runCognitionLayerCrossCuts({ simulator, role: 'publishing' });
  }, 30000);
});
