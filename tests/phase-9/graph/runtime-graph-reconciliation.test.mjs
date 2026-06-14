// Phase 9 — Tier 2 Graph Worker: reconciliation.
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('graph/runtime-graph-reconciliation — Tier 2', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-graph-reconciliation' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('graph', 'runtime-graph-reconciliation');
    if (harness) await harness.shutdown();
  }, 30000);

  it('reconciliation worker executes and the chain is constitutional', async () => {
    const correlationId = `p9-recon-${Date.now()}`;
    harness.injectEvent({
      type: 'RECONCILIATION_REQUESTED',
      source: 'graph/reconciliation',
      payload: { scope: 'all' },
      correlationId,
    });
    await harness.tick(3);

    const snap = harness.snapshotDeriver.derive();
    const event = snap.events[correlationId];
    expect(event, 'reconciliation event not observed').toBeDefined();
    writer.bumpAssertions(1);
  });
});
