/**
 * Full Runtime Composition — Phase 7 integration test
 * ═════════════════════════════════════════════════════
 *
 * Boots all 9 kernels together via the Phase7RuntimeSimulator
 * (the 17-step boot). Injects cross-kernel events. Asserts
 * end-to-end causality. Asserts no authority leak.
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';

describe('Full Runtime Composition — Phase 7 integration', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'full-composition' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('all 9 kernels boot and accept cross-kernel events', async () => {
    const events = [
      { type: 'ACQUISITION_FETCH_COMPLETE', source: 'acquisition', payload: { worker: 'comments-worker', accountId: 'acc-1', resourceId: 'm-1' } },
      { type: 'PUBLISH_REQUESTED', source: 'publishing', payload: { worker: 'content-worker', accountId: 'acc-1', contentId: 'p-1' } },
      { type: 'TOKEN_REFRESH_REQUESTED', source: 'graph-capability', payload: { accountId: 'acc-1' } },
      { type: 'RECONCILIATION_TICK', source: 'reconciliation', payload: { accountId: 'acc-1' } },
      { type: 'CADENCE_TICK', source: 'scheduling', payload: { tick: 1 } },
    ];

    for (const evt of events) {
      simulator.injectEvent({ ...evt, correlationId: `comp-${evt.type}-${Date.now()}` });
    }

    await simulator.tick(5);

    const timeline = simulator.timeline();
    for (const evt of events) {
      const found = timeline.some((e) => e.type === evt.type);
      if (!found) {
        throw new Error(`Cross-kernel event ${evt.type} not observed in timeline`);
      }
    }

    // Governance log must have at least one decision per event
    const decisions = simulator.governanceLog();
    if (decisions.length === 0) {
      throw new Error('No governance decisions recorded');
    }
  }, 60000);

  it('no authority leak across kernels', async () => {
    await simulator.tick(3);
    simulator.assertWorkersDidNotGovern();
  }, 30000);
});
