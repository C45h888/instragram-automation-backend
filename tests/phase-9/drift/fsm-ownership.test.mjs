// Phase 9 — Drift: fsm
// fsm transitions emit lineage

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('drift/fsm-ownership', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-drift-fsm' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('drift', 'fsm-ownership');
    if (harness) await harness.shutdown();
  }, 30000);

  it('no fsm drift detected in a clean run', async () => {
    // Run a small chain through the runtime.
    const correlationId = `p9-drift-fsm-${Date.now()}`;
    harness.injectEvent({
      type: 'WEBHOOK_DELIVERED',
      source: 'runtime/ingress',
      payload: { fixture: 'message-created' },
      correlationId,
    });
    await harness.tick(3);

    const findings = harness.driftDetector.snapshot();
    const relevant = findings.filter((d) => d.event_id === correlationId);
    expect(relevant, JSON.stringify(relevant)).toEqual([]);
    writer.bumpAssertions(1);
  });
});
