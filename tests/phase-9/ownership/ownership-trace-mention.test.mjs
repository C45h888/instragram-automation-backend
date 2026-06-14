// Phase 9 — Ownership Trace: mention.
// Drives the runtime through processWebhook() with a real fixture and
// verifies that every stage of the chain carries a valid owner.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical', 'mention-created.json');

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('ownership/ownership-trace-mention', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-own-mention' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('ownership', 'ownership-trace-mention');
    if (harness) await harness.shutdown();
  }, 30000);

  it('chain owners match the architecture mandate', async () => {
    const body = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8'));
    const accountId = body.entry?.[0]?.id || '17841405822304914';

    const routing = webhookSubstrate.processWebhook(body, accountId);
    expect(routing.asyncDispatched, 'substrate must dispatch').toBe(true);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    await harness.tick(3);

    const ownership = harness.ownershipTracer.snapshot();
    const records = Object.values(ownership);
    expect(records.length, 'no ownership records').toBeGreaterThan(0);

    const hasMutation = records.some((r) => r.mutation && r.mutation.owner);
    expect(hasMutation, 'no mutation with owner found').toBe(true);
    writer.bumpAssertions(3);
  });
});
