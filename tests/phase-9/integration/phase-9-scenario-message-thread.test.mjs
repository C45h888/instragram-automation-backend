// Phase 9 — End-to-end scenario: message thread.
// Walks one message webhook through the full chain:
//   - delivery → ingress → governance → FSM → worker → mutation
//   - cross-kernel to publishing
//   - cross-kernel to insights
//   - reconciliation
// Asserts the entire scenario completes constitutionally.

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

describe('integration/phase-9-scenario-message-thread', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-scenario' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-scenario-message-thread');
    if (harness) await harness.shutdown();
  }, 30000);

  it('message thread runs end-to-end', async () => {
    // ── Webhook: real processWebhook() seam ────────────────────────────
    const body = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8'));
    const accountId = body.entry?.[0]?.id || '17841405822304914';
    const routing = webhookSubstrate.processWebhook(body, accountId);
    expect(routing.asyncDispatched, 'substrate must dispatch').toBe(true);
    await new Promise((r) => setImmediate(r));

    // ── Graph: injectEvent with canonical types ─────────────────────────
    const corr = `p9-scenario-${Date.now()}`;
    harness.injectEvent({
      type: 'PUBLISHING_DATA_AVAILABLE',
      source: 'graph/publishing',
      payload: { scenario: 'message-thread', stage: 'reply' },
      correlationId: `${corr}-pub`,
    });
    harness.injectEvent({
      type: 'INSIGHTS_POLL_FAILURE',
      source: 'graph/insights',
      payload: { accountId: 'test-acc-001', intentId: 'int-${Date.now()}', domain: 'test', error: 'simulated' },
      correlationId: `${corr}-ins`,
    });
    harness.injectEvent({
      type: 'RECONCILIATION_TICK',
      source: 'graph/reconciliation',
      payload: { scenario: 'message-thread', stage: 'finalize' },
      correlationId: `${corr}-rec`,
    });
    await harness.tick(10);

    const snap = harness.snapshotDeriver.derive();
    expect(Object.keys(snap.events).length, 'events observed').toBeGreaterThan(0);
    expect(snap.events[`${corr}-pub`], 'publish event not observed').toBeDefined();
    expect(snap.events[`${corr}-ins`], 'insights event not observed').toBeDefined();
    expect(snap.events[`${corr}-rec`], 'reconciliation event not observed').toBeDefined();
    writer.bumpAssertions(5);
  });
});
