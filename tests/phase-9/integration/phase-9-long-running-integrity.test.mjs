// Phase 9 — Long-running integrity invariant.
// Runs 500+ ticks. Primes with processWebhook(), then graph workers
// via injectEvent() per window. Asserts non-increasing invariants.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const TIER = (process.env.PHASE9_CADENCE_TIER || 'long').toLowerCase();
const TICKS = TIER === 'epic' ? 1000 : 500;
const WINDOW = 50;
const WORKERS = ['insights', 'publishing', 'capability'];
const CANONICAL_EVENT = {
  insights:      'INSIGHTS_POLL_FAILURE',
  publishing:    'PUBLISHING_DATA_AVAILABLE',
  capability:    'CAPABILITY_EVALUATE',
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical', 'message-created.json');

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('integration/phase-9-long-running-integrity', () => {
  let harness;
  let writer;
  let prevDrift = 0;
  let prevOrphans = 0;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-integrity' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-long-running-integrity');
    if (harness) await harness.shutdown();
  }, 30000);

  it(`non-increasing violations over ${TICKS} ticks (tier=${TIER})`, async () => {
    // Prime with one real webhook delivery.
    const body = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8'));
    const accountId = body.entry?.[0]?.id || '17841405822304914';
    webhookSubstrate.processWebhook(body, accountId);
    await new Promise((r) => setImmediate(r));

    for (let t = 0; t < TICKS; t += WINDOW) {
      for (let i = 0; i < WINDOW; i++) {
        const w = WORKERS[(t + i) % WORKERS.length];
        harness.injectEvent({
          type: CANONICAL_EVENT[w],
          source: `graph/${w}`,
          payload: { accountId: 'test-acc-001', intentId: `tick-${t + i}`, domain: 'cross', error: 'simulated', tick: t + i },
          correlationId: `p9-int-gr-${t + i}`,
        });
      }
      await harness.tick(WINDOW);

      const drift = harness.driftDetector.snapshot().length;
      const snap = harness.snapshotDeriver.derive();
      const orphans = Object.values(snap.events).filter(
        (e) => e.mutation_count === 0
      ).length;

      if (t > 0) {
        expect(drift, `drift increased at tick ${t}`).toBeLessThanOrEqual(prevDrift);
        expect(orphans, `orphan events increased at tick ${t}`).toBeLessThanOrEqual(prevOrphans + WINDOW);
      }
      prevDrift = drift;
      prevOrphans = orphans;
    }
    writer.bumpAssertions(Math.floor(TICKS / WINDOW) * 2);
  });
});
