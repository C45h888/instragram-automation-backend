// Phase 9 — Long-running integrity invariant.
// Runs the long or epic tier. At every N-tick window, asserts
// three non-increasing invariants:
//   - authority violations (drift findings)
//   - drift findings
//   - orphan lineage events (event_ids with no terminating mutation)
//
// Catches slow degradation that a one-shot composition test would miss.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

const TIER = (process.env.PHASE9_CADENCE_TIER || 'long').toLowerCase();
const TICKS = TIER === 'epic' ? 1000 : 500;
const WINDOW = 50;
const FIXTURES = ['message-created', 'comment-created', 'mention-created'];
const WORKERS = ['insights', 'publishing', 'capability'];

describe('integration/phase-9-long-running-integrity', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-integrity' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-long-running-integrity');
    if (harness) await harness.shutdown();
  }, 30000);

  it(`non-increasing violations over ${TICKS} ticks (tier=${TIER})`, async () => {
    let prevViolations = 0;
    let prevOrphans = 0;
    for (let t = 0; t < TICKS; t++) {
      const f = FIXTURES[t % FIXTURES.length];
      const w = WORKERS[t % WORKERS.length];
      harness.injectEvent({
        type: 'WEBHOOK_DELIVERED',
        source: 'runtime/ingress',
        payload: { fixture: f, tick: t },
        correlationId: `p9-int-wh-${t}`,
      });
      harness.injectEvent({
        type: `${w.toUpperCase()}_REQUESTED`,
        source: `graph/${w}`,
        payload: { tick: t },
        correlationId: `p9-int-gr-${t}`,
      });
      await harness.tick(1);

      if ((t + 1) % WINDOW === 0) {
        const snap = harness.snapshotDeriver.derive();
        const events = Object.values(snap.events);
        const orphans = events.filter((e) => e.worker_count > 0 && e.mutation_count === 0).length;
        const violations = harness.driftDetector.snapshot().length;
        // Non-increasing invariant.
        if (violations > prevViolations) {
          throw new Error(`Authority violations increased at tick ${t}: ${prevViolations} → ${violations}`);
        }
        if (orphans > prevOrphans) {
          throw new Error(`Orphan events increased at tick ${t}: ${prevOrphans} → ${orphans}`);
        }
        prevViolations = violations;
        prevOrphans = orphans;
      }
    }
    writer.bumpAssertions(1);
  }, TICKS * 200);
});
