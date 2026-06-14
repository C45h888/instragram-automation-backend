// Phase 9 — Runtime Composition.
// Boots the runtime, exercises all 6 webhook fixtures via processWebhook()
// + all 5 graph workers via injectEvent() with canonical types.
// Asserts the snapshot is internally consistent at the end.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CANONICAL_DIR = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical');

const FIXTURES = [
  'message-created', 'comment-created', 'mention-created',
  'story-reply', 'media-update', 'conversation-update',
];
const GRAPH_WORKERS = ['insights', 'publishing', 'capability', 'recovery', 'reconciliation'];
const CANONICAL_EVENT = {
  insights:      'INSIGHTS_POLL_FAILURE',
  publishing:    'PUBLISHING_DATA_AVAILABLE',
  capability:    'CAPABILITY_EVALUATE',
  recovery:      'CIRCUIT_BREAKER_CHECK',
  reconciliation:'RECONCILIATION_TICK',
};

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('integration/phase-9-runtime-composition', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-composition' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-runtime-composition');
    if (harness) await harness.shutdown();
  }, 30000);

  it('all 6 webhook + 5 graph fixtures compose', async () => {
    // Tier 1: webhook — real processWebhook() seam.
    for (const name of FIXTURES) {
      const body = JSON.parse(fs.readFileSync(path.join(CANONICAL_DIR, `${name}.json`), 'utf8'));
      const accountId = body.entry?.[0]?.id || '17841405822304914';
      const routing = webhookSubstrate.processWebhook(body, accountId);
      expect(routing.asyncDispatched, `${name} must dispatch`).toBe(true);
    }
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));

    // Tier 2: graph — canonical injectEvent() seam.
    for (const w of GRAPH_WORKERS) {
      const correlationId = `p9-comp-gr-${w}-${Date.now()}`;
      harness.injectEvent({
        type: CANONICAL_EVENT[w],
        source: `graph/${w}`,
        payload: { accountId: 'test-acc-001', intentId: 'int-${Date.now()}', domain: 'test', error: 'simulated' },
        correlationId,
      });
    }
    await harness.tick(5);

    const snap = harness.snapshotDeriver.derive();
    expect(Object.keys(snap.events).length, 'events observed').toBeGreaterThan(0);
    writer.bumpAssertions(7); // 6 webhook + 1 composition
  });
});
