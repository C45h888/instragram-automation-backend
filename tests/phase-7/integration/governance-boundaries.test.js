/**
 * Governance Boundaries — Phase 7 integration test
 * ═════════════════════════════════════════════════
 *
 * Workers are observed via simulator.workerTrace(). Assert NONE
 * of them call validate*, NONE inspect token state, NONE mutate
 * governance state. FSM/CK are the sole authority.
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';

describe('Governance Boundaries — Phase 7 integration', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'governance-boundaries' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('no worker activity produces governance decisions', async () => {
    const workerEvents = [
      { type: 'ACQUISITION_FETCH_COMPLETE', source: 'comments-worker' },
      { type: 'ACQUISITION_FETCH_COMPLETE', source: 'messages-worker' },
      { type: 'PUBLISH_REQUESTED', source: 'content-worker' },
      { type: 'PUBLISH_REQUESTED', source: 'engagement-worker' },
      { type: 'RECONCILIATION_TICK', source: 'reconciliation-worker' },
      { type: 'CADENCE_TICK', source: 'cadence' },
    ];

    for (const evt of workerEvents) {
      simulator.injectEvent({ ...evt, correlationId: `gov-bound-${Date.now()}-${Math.random()}` });
    }
    await simulator.tick(5);

    // The shared contract assertion
    simulator.assertWorkersDidNotGovern();

    // Direct check: every governance decision must come from CK or a FSM
    const decisions = simulator.governanceLog();
    const forbidden = decisions.filter(
      (d) => d.source && !['CK', 'FSM'].includes(d.source) && !d.source.endsWith('-fsm')
    );
    if (forbidden.length > 0) {
      const err = new Error(
        `Governance decisions made by non-authority sources: ${forbidden.length}`
      );
      err.violations = forbidden;
      throw err;
    }
  }, 60000);
});
