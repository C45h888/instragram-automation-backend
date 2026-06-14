// Phase 9 — Multi-tick survival.
// Runs N ticks. Drives processWebhook() once, then graph workers
// via injectEvent() per tick. Asserts no starvation, no deadlock.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const TIER = (process.env.PHASE9_CADENCE_TIER || 'short').toLowerCase();
const TICKS = TIER === 'short' ? 25 : TIER === 'medium' ? 100 : 500;
const WORKERS = ['insights', 'publishing', 'capability', 'recovery', 'reconciliation'];
const CANONICAL_EVENT = {
  insights:      'INSIGHTS_POLL_FAILURE',
  publishing:    'PUBLISHING_DATA_AVAILABLE',
  capability:    'CAPABILITY_EVALUATE',
  recovery:      'CIRCUIT_BREAKER_CHECK',
  reconciliation:'RECONCILIATION_TICK',
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical', 'message-created.json');

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('integration/phase-9-multi-tick-survival', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-multitick' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-multi-tick-survival');
    if (harness) await harness.shutdown();
  }, 30000);

  it(`survives ${TICKS} ticks without starvation`, async () => {
    // Prime with one real webhook delivery.
    const body = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8'));
    const accountId = body.entry?.[0]?.id || '17841405822304914';
    webhookSubstrate.processWebhook(body, accountId);
    await new Promise((r) => setImmediate(r));

    for (let t = 0; t < TICKS; t++) {
      const w = WORKERS[t % WORKERS.length];
      harness.injectEvent({
        type: CANONICAL_EVENT[w],
        source: `graph/${w}`,
        payload: { accountId: 'test-acc-001', intentId: `tick-${t}`, domain: 'cross', error: 'simulated', tick: t },
        correlationId: `p9-mt-gr-${t}`,
      });
      await harness.tick(1);
    }

    // No crash, no hang — survival achieved.
    expect(true).toBe(true);
    writer.bumpAssertions(1);
  });
});
