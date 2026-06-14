// Phase 9 — Drift: semantic-drift
// Drives processWebhook() with a real fixture and asserts zero drift findings.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical', 'message-created.json');

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('drift/semantic-drift', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-drift-semantic-drift' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('drift', 'semantic-drift');
    if (harness) await harness.shutdown();
  }, 30000);

  it('no drift detected in a clean run', async () => {
    const body = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8'));
    const accountId = body.entry?.[0]?.id || '17841405822304914';

    const routing = webhookSubstrate.processWebhook(body, accountId);
    expect(routing.asyncDispatched, 'substrate must dispatch').toBe(true);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    await harness.tick(3);

    const findings = harness.driftDetector.snapshot();
    expect(findings, JSON.stringify(findings)).toEqual([]);
    writer.bumpAssertions(2);
  });
});
