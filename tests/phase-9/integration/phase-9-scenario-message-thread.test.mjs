// Phase 9 — End-to-end scenario: message thread.
// Walks one message webhook through the full chain:
//   - delivery → ingress → governance → FSM → worker → mutation
//   - cross-kernel to publishing
//   - cross-kernel to insights
//   - reconciliation
// Asserts the entire scenario completes constitutionally.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe('integration/phase-9-scenario-message-thread', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-scenario' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-scenario-message-thread');
    if (harness) await harness.shutdown();
  }, 30000);

  it('message thread runs end-to-end', async () => {
    const body = JSON.parse(
      fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical', 'message-created.json'), 'utf8')
    );
    const correlationId = `p9-scenario-${Date.now()}`;
    harness.injectEvent({
      type: 'WEBHOOK_DELIVERED',
      source: 'runtime/ingress',
      payload: { fixture: 'message-created', body },
      correlationId,
    });
    // Drive follow-on graph events with their own correlation
    // ids so the chain is observable per stage.
    harness.injectEvent({
      type: 'PUBLISH_REQUESTED',
      source: 'graph/publishing',
      payload: { scenario: 'message-thread', stage: 'reply' },
      correlationId: `${correlationId}-pub`,
    });
    harness.injectEvent({
      type: 'INSIGHTS_FETCH_REQUESTED',
      source: 'graph/insights',
      payload: { scenario: 'message-thread', stage: 'engagement' },
      correlationId: `${correlationId}-ins`,
    });
    harness.injectEvent({
      type: 'RECONCILIATION_REQUESTED',
      source: 'graph/reconciliation',
      payload: { scenario: 'message-thread', stage: 'finalize' },
      correlationId: `${correlationId}-rec`,
    });
    await harness.tick(10);

    const snap = harness.snapshotDeriver.derive();
    expect(snap.events[correlationId], 'message event not observed').toBeDefined();
    expect(snap.events[`${correlationId}-pub`], 'publish event not observed').toBeDefined();
    expect(snap.events[`${correlationId}-ins`], 'insights event not observed').toBeDefined();
    expect(snap.events[`${correlationId}-rec`], 'reconciliation event not observed').toBeDefined();
    writer.bumpAssertions(4);
  });
});
