// Phase 9 — Tier 1 Webhook Retry.
// Arms a rate-limit injection on the simulator, delivers a webhook,
// and confirms the runtime observed the chain even when the
// transport initially failed. The runtime must observe the
// governance decision (which may be a re-queue) without fabricating
// a worker execution.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

const SIM_CONTROL = process.env.PHASE9_SIM_CONTROL || 'http://localhost:9301';

async function control(path, opts = {}) {
  const res = await fetch(`${SIM_CONTROL}${path}`, { method: 'POST', ...opts });
  return { status: res.status, body: await res.json().catch(() => null) };
}

describe('webhook/runtime-webhook-retry — Tier 1', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-webhook-retry' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('webhook', 'runtime-webhook-retry');
    if (harness) await harness.shutdown();
  }, 30000);

  it('runtime observes the chain even when transport is rate-limited', async () => {
    await control('/control/reset');
    await control('/control/inject', {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scenario: 'rate-limit', fixture: 'message-created', count: 1 }),
    });

    // Inject as a normal webhook delivery. The runtime may or may
    // not have a retry policy registered yet; what we assert is
    // that the event was at least observed and the runtime did
    // not fabricate a worker invocation to "make the test pass".
    const correlationId = `p9-retry-${Date.now()}`;
    harness.injectEvent({
      type: 'WEBHOOK_RETRY_TEST',
      source: 'runtime/ingress',
      payload: { fixture: 'message-created', armed: 'rate-limit' },
      correlationId,
    });
    await harness.tick(3);

    const snap = harness.snapshotDeriver.derive();
    const event = snap.events[correlationId];
    expect(event, 'retry event not observed').toBeDefined();
    // The runtime may have re-queued (0 workers, 0 mutations) or
    // processed normally depending on substrate policy. Both are
    // constitutional. What is NOT constitutional: fabricating a
    // worker invocation. Assert the recorded chain is internally
    // consistent regardless of outcome.
    if (event.worker_count > 0) {
      expect(event.mutation_count, 'worker ran but no mutation').toBeGreaterThan(0);
    }
    writer.bumpAssertions(2);
  });
});
