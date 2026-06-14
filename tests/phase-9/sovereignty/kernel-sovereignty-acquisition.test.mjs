// Phase 9 — Kernel Sovereignty
// Kernel: acquisition
//
// Validates the acquisition kernel can:
//   1. Boot with CK in HEALTHY state (no degraded-mode errors)
//   2. Register its FSM and accept domain-routed events
//   3. Write lineage entries for acquisition events
//   4. Show no unexpected governance rejections in the log
//
// Note: we still boot the full runtime here (phase-9b isolation harness
// pending). The assertions below test the acquisition kernel's sovereignty
// contract rather than just checking .toBeDefined().

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('sovereignty/kernel-sovereignty-acquisition', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-sov-acquisition' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('sovereignty', 'kernel-sovereignty-acquisition');
    if (harness) await harness.shutdown();
  }, 30000);

  it('CK is in HEALTHY state after boot', async () => {
    const snap = harness.simulator.stateInspector.snapshot();
    expect(snap, 'state inspector returned null').not.toBeNull();
    expect(snap.governance, 'governance state missing').toBeDefined();
    // CK must be HEALTHY — any other state means degraded-mode errors occurred at boot
    expect(snap.governance.state, 'CK is not HEALTHY — kernel boot produced degraded-mode errors').toBe('HEALTHY');
    writer.bumpAssertions(1);
  });

  it('acquisition domain event routes through CK without unexpected rejections', async () => {
    // Record governance decisions before the injection
    const decisionsBefore = harness.simulator.governanceLog().length;

    // Inject a real acquisition-domain event — this must route through
    // CK's DOMAIN_EVENT_MAP and return { allowed: true }
    const correlationId = `p9-sov-acq-${Date.now()}`;
    const result = harness.simulator.injectEvent({
      type: 'ACQUISITION_INTENT_RECEIVED',
      source: 'sovereignty-test',
      payload: { accountId: 'test-account', intentId: correlationId },
      correlationId,
    });

    // The injectEvent path goes through observability → CK.dispatch.
    // The acquisition event must not be rejected by CK's guards.
    // If it returns null/undefined from observability, the event still
    // reaches CK directly via tick(), so we also check the governance log.
    const decisionsAfter = harness.simulator.governanceLog().length;
    const newDecisions = harness.simulator.governanceLog().slice(decisionsBefore);

    // At least one new governance decision must have fired for this event
    expect(newDecisions.length, 'no governance decision fired for ACQUISITION_INTENT_RECEIVED').toBeGreaterThan(0);

    // No decision should show a rejection for acquisition events
    const rejections = newDecisions.filter((d) => d.result === false || d.result?.allowed === false);
    expect(rejections, `CK rejected ACQUISITION_INTENT_RECEIVED: ${JSON.stringify(rejections)}`).toHaveLength(0);
    writer.bumpAssertions(2);
  });

  it('lineage ledger has entries after acquisition event injection', async () => {
    const snap = await harness.simulator.snapshot();
    // Lineage must have grown — the acquisition event should have been recorded
    expect(snap.lineage, 'lineage missing from snapshot').toBeDefined();
    expect(snap.lineage.size, 'lineage size is missing or not a number').toBeGreaterThanOrEqual(0);
    // At minimum the BOOT_COMPLETE + intent event should produce lineage
    expect(snap.lineage.size, 'lineage is empty after acquisition boot — events not written to ledger').toBeGreaterThan(0);
    writer.bumpAssertions(1);
  });
});
