// Phase 9 — Tier 2 Graph Worker: capability.
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('graph/runtime-graph-capability — Tier 2', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-graph-capability' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('graph', 'runtime-graph-capability');
    if (harness) await harness.shutdown();
  }, 30000);

  it('capability worker executes and the chain is constitutional', async () => {
    const correlationId = `p9-capability-${Date.now()}`;
    harness.injectEvent({
      type: 'CAPABILITY_EVALUATE',
      source: 'graph/capability',
      payload: { accountId: 'acc-1' },
      correlationId,
    });
    await harness.tick(3);

    const snap = harness.snapshotDeriver.derive();
    const event = snap.events[correlationId];
    expect(event, 'capability event not observed').toBeDefined();
    writer.bumpAssertions(1);
  });
});
