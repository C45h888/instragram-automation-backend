// Phase 9 — Kernel Sovereignty
// Kernel: publishing
//
// Validates the publishing kernel can:
//   1. Boot with CK in HEALTHY state (no degraded-mode errors)
//   2. Accept publishing domain events without rejections
//   3. Show no unexpected governance rejections in the log

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('sovereignty/kernel-sovereignty-publishing', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-sov-publishing' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('sovereignty', 'kernel-sovereignty-publishing');
    if (harness) await harness.shutdown();
  }, 30000);

  it('CK is in HEALTHY state after boot', async () => {
    const snap = harness.simulator.stateInspector.snapshot();
    expect(snap, 'state inspector returned null').not.toBeNull();
    expect(snap.governance, 'governance state missing').toBeDefined();
    expect(snap.governance.state, 'CK is not HEALTHY — kernel boot produced degraded-mode errors').toBe('HEALTHY');
    writer.bumpAssertions(1);
  });

  it('publishing domain event routes through CK without unexpected rejections', async () => {
    const decisionsBefore = harness.simulator.governanceLog().length;

    harness.simulator.injectEvent({
      type: 'PUBLISHING_DATA_AVAILABLE',
      source: 'sovereignty-test',
      payload: { accountId: 'test-account' },
      correlationId: `p9-sov-publishing-${Date.now()}`,
    });

    const newDecisions = harness.simulator.governanceLog().slice(decisionsBefore);
    expect(newDecisions.length, 'no governance decision fired for PUBLISHING_DATA_AVAILABLE').toBeGreaterThan(0);

    const rejections = newDecisions.filter((d) => d.result === false || d.result?.allowed === false);
    expect(rejections, `CK rejected PUBLISHING_DATA_AVAILABLE: ${JSON.stringify(rejections)}`).toHaveLength(0);
    writer.bumpAssertions(2);
  });

  it('lineage ledger has entries after publishing event injection', async () => {
    const snap = await harness.simulator.snapshot();
    expect(snap.lineage, 'lineage missing from snapshot').toBeDefined();
    expect(snap.lineage.size, 'lineage is empty after publishing kernel boot — events not written to ledger').toBeGreaterThan(0);
    writer.bumpAssertions(1);
  });
});
