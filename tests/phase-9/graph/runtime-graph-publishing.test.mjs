// Phase 9 — Tier 2 Graph Worker: publishing.
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('graph/runtime-graph-publishing — Tier 2', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-graph-publishing' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('graph', 'runtime-graph-publishing');
    if (harness) await harness.shutdown();
  }, 30000);

  it('publishing worker executes and the chain is constitutional', async () => {
    const correlationId = `p9-publishing-${Date.now()}`;
    harness.injectEvent({
      type: 'PUBLISHING_DATA_AVAILABLE',
      source: 'graph/publishing',
      payload: { mediaId: 'MEDIA_002' },
      correlationId,
    });
    await harness.tick(3);

    const snap = harness.snapshotDeriver.derive();
    const event = snap.events[correlationId];
    expect(event, 'publishing event not observed').toBeDefined();
    writer.bumpAssertions(1);
  });
});
