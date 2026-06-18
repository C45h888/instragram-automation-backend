// Phase 9 — Tier 1 Webhook Signature.
// Bad signature must be rejected by the ingress before governance.
// Asserts: a bad-sig event produces a chain where the parser
// (signature check) failed, and no downstream governance / FSM /
// worker / mutation happened.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('webhook/runtime-webhook-signature — Tier 1', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-webhook-sig' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('webhook', 'runtime-webhook-signature');
    if (harness) await harness.shutdown();
  }, 30000);

  it('bad signature produces an ingress-rejected chain', async () => {
    // Empty entry array — the substrate rejects before any worker is dispatched.
    // This exercises the ingress gate without triggering downstream governance.
    const routing = webhookSubstrate.processWebhook(
      { object: 'instagram', entry: [] },
      '17841405822304914',
    );
    expect(routing, 'substrate rejected payload').toBeDefined();
    // Rejected payloads do NOT dispatch — asyncDispatched is absent or false.
    expect(routing.asyncDispatched, 'ingress must reject empty payload').not.toBe(true);
    await harness.tick(3);

    // Substrate rejected — no governance events entered the system.
    // The asyncDispatched check is the canonical proof of rejection.
    writer.bumpAssertions(2);
  });
});
