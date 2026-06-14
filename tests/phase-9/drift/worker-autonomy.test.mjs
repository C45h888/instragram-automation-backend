// Phase 9 — Drift: autonomy
// workers never import scheduler/governance/fsm

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('drift/worker-autonomy', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-drift-autonomy' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('drift', 'worker-autonomy');
    if (harness) await harness.shutdown();
  }, 30000);

  it('no autonomy drift detected in a clean run', async () => {
    // Run a small chain through the runtime.
    const correlationId = `p9-drift-autonomy-${Date.now()}`;
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
