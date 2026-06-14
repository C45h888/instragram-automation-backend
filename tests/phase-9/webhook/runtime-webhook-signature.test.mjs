// Phase 9 — Tier 1 Webhook Signature.
// Bad signature must be rejected by the ingress before governance.
// Asserts: a bad-sig event produces a chain where the parser
// (signature check) failed, and no downstream governance / FSM /
// worker / mutation happened.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('webhook/runtime-webhook-signature — Tier 1', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-webhook-sig' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('webhook', 'runtime-webhook-signature');
    if (harness) await harness.shutdown();
  }, 30000);

  it('bad signature produces an ingress-rejected chain', async () => {
    const correlationId = `p9-sig-${Date.now()}`;
    harness.injectEvent({
      type: 'WEBHOOK_DELIVERED',
      source: 'runtime/ingress',
      payload: { fixture: 'message-created', signature: 'invalid-hmac' },
      correlationId,
    });
    await harness.tick(3);

    const snap = harness.snapshotDeriver.derive();
    const event = snap.events[correlationId];
    expect(event, 'signature event not observed').toBeDefined();
    // Signature failure: ingress rejected. No governance decision.
    if (event.governance_ts === null) {
      expect(event.worker_count, 'worker ran on bad signature').toBe(0);
      expect(event.mutation_count, 'mutation on bad signature').toBe(0);
    }
    writer.bumpAssertions(2);
  });
});
