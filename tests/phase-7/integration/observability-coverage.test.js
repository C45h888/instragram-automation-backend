/**
 * Observability Coverage — Phase 7 integration test
 * ═══════════════════════════════════════════════════
 *
 * Every emit, every decision, every deviation must be
 * reconstructable from the timeline. Assert timeline completeness
 * for a representative set of operational flows.
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';

const OPERATIONAL_FLOWS = [
  { type: 'ACQUISITION_FETCH_COMPLETE', source: 'acquisition' },
  { type: 'PUBLISH_REQUESTED', source: 'publishing' },
  { type: 'CAPABILITY_TRANSITION', source: 'graph-capability' },
  { type: 'RECONCILIATION_TICK', source: 'reconciliation' },
  { type: 'CADENCE_TICK', source: 'scheduling' },
];

describe('Observability Coverage — Phase 7 integration', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'observability-coverage' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('all operational flows are reconstructable from the timeline', async () => {
    const correlationIds = [];
    for (const flow of OPERATIONAL_FLOWS) {
      const corr = `obs-cov-${flow.type}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      correlationIds.push(corr);
      simulator.injectEvent({ ...flow, correlationId: corr });
    }
    await simulator.tick(3);

    const timeline = simulator.timeline();

    // 1. Timeline non-empty
    if (timeline.length < OPERATIONAL_FLOWS.length) {
      throw new Error(
        `Timeline incomplete: injected ${OPERATIONAL_FLOWS.length}, observed ${timeline.length}`
      );
    }

    // 2. Each event type appears in the timeline
    for (const flow of OPERATIONAL_FLOWS) {
      const found = timeline.some((e) => e.type === flow.type);
      if (!found) {
        throw new Error(`Operational flow ${flow.type} not observed in timeline`);
      }
    }

    // 3. Each correlationId appears exactly once (uniqueness / no duplicates)
    for (const corr of correlationIds) {
      const matches = timeline.filter((e) => e.correlationId === corr);
      if (matches.length === 0) {
        throw new Error(`CorrelationId ${corr} not in timeline`);
      }
      if (matches.length > 1) {
        throw new Error(`CorrelationId ${corr} duplicated (${matches.length} matches)`);
      }
    }

    // 4. All events have required fields
    for (const e of timeline) {
      if (e.timestamp == null) {
        throw new Error('Timeline event missing timestamp');
      }
      if (!e.type) {
        throw new Error('Timeline event missing type');
      }
      if (e.payload === undefined) {
        throw new Error(`Timeline event ${e.type} missing payload snapshot`);
      }
    }

    // 5. Governance log also reconstructable
    const decisions = simulator.governanceLog();
    if (decisions.length === 0) {
      throw new Error('Governance log empty — decisions not reconstructable');
    }
    for (const d of decisions) {
      if (!d.type) {
        throw new Error('Governance decision missing type');
      }
      if (d.timestamp == null) {
        throw new Error('Governance decision missing timestamp');
      }
    }
  }, 60000);
});
