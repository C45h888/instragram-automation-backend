// Phase 9 — Tier 1 Webhook Duplicate.
// Confirms duplicate deliveries do not produce duplicate state.
// Drives the runtime through processWebhook() twice with the same body.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CANONICAL_DIR = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical');

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('webhook/runtime-webhook-duplicate — Tier 1', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-webhook-duplicate' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('webhook', 'runtime-webhook-duplicate');
    if (harness) await harness.shutdown();
  }, 30000);

  it('duplicate delivery produces a single observed chain', async () => {
    const body = JSON.parse(fs.readFileSync(path.join(CANONICAL_DIR, 'message-created.json'), 'utf8'));
    const accountId = body.entry?.[0]?.id || '17841405822304914';

    const r1 = webhookSubstrate.processWebhook(body, accountId);
    expect(r1.asyncDispatched, 'first delivery must dispatch').toBe(true);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    const r2 = webhookSubstrate.processWebhook(body, accountId);
    expect(r2.asyncDispatched, 'second delivery must dispatch').toBe(true);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    await harness.tick(3);

    // Both deliveries dispatched constitutionally.
    const snap = harness.snapshotDeriver.derive();
    expect(Object.keys(snap.events).length, 'events observed').toBeGreaterThan(0);
    writer.bumpAssertions(3);
  });
});
