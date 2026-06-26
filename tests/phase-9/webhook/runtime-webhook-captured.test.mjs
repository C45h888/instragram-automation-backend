// Phase 9 — Tier 1 Webhook (captured fixtures).
// Slot reserved for phase 10 (VPS / RunPod). When captured/ is
// non-empty, this test discovers every .json in captured/, delivers
// each through the real runtime, and asserts the 4-point constitutional
// contract (observed event_id, worker > 0, governance observed, no drift).
//
// Canonical fixtures are sha256-named copies of the same shapes in
// ../fixtures/webhooks/canonical/. The captured/ slot validates the
// discovery-and-contract path — same pipeline, same contract, different
// fixture source.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createRequire } from 'node:module';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CAPTURED_DIR = path.join(__dirname, '..', 'fixtures', 'webhooks', 'captured');

const require = createRequire(import.meta.url);
const webhookSubstrate = require('../../../acquisition-kernel/substrates/webhook-acquisition-substrate');
const constitutionalKernel = require('../../../control-plane/governance/constitutional-kernel');

describe('webhook/runtime-webhook-captured — Tier 1', () => {
  let harness;
  const writerRef = {};

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-webhook-captured' });
    await harness.boot();
    webhookSubstrate.setGovernance(constitutionalKernel);
    writerRef.writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writerRef.writer.writeSummary('webhook', 'runtime-webhook-captured');
    if (harness) await harness.shutdown();
  }, 30000);

  it('captured/ directory exists and is ready to receive payloads', () => {
    const exists = fs.existsSync(CAPTURED_DIR);
    const readme = path.join(CAPTURED_DIR, 'README.md');
    const hasReadme = fs.existsSync(readme);
    if (!exists) throw new Error('captured/ directory missing');
    if (!hasReadme) throw new Error('captured/README.md missing');
  });

  // Dynamically discover all .json fixtures in captured/
  const capturedFixtures = fs.readdirSync(CAPTURED_DIR)
    .filter((f) => f.endsWith('.json'))
    .map((f) => ({
      filename: f,
      body: JSON.parse(fs.readFileSync(path.join(CAPTURED_DIR, f), 'utf8')),
    }));

  for (const { filename, body } of capturedFixtures) {
    it(`captured: ${filename} — 4-point constitutional contract`, async () => {
      const accountId = body.entry?.[0]?.id || `acc-${filename}`;

      // Timeline baseline: record length before delivery so we can
      // isolate this fixture's events from boot-time noise and from
      // other fixture runs in the same test file.
      const timelineBefore = harness.simulator.timeline().length;

      // Drive the runtime through the same production seam as
      // runtime-webhook-ingress: processWebhook().
      const routing = webhookSubstrate.processWebhook(body, accountId);
      expect(routing, `${filename}: substrate rejected payload`).toBeDefined();
      expect(routing.asyncDispatched, `${filename}: substrate did not dispatch`).toBe(true);

      // Drain the setImmediate queue and give the runtime 3 ticks
      // to process any queued workers.
      await new Promise((r) => setImmediate(r));
      await new Promise((r) => setImmediate(r));
      await harness.tick(3);

      // Isolate current-run events using the timeline boundary.
      const allObs = harness.recorder.snapshot();
      const currentObs = allObs.slice(timelineBefore);

      // Bucket by event_id so we can answer "did any event_id
      // see a worker? did any event_id see governance?"
      const byId = {};
      for (const o of currentObs) {
        if (!byId[o.event_id]) {
          byId[o.event_id] = {
            event_id: o.event_id,
            kinds: [],
            sources: new Set(),
            has_worker: false,
            has_mutation: false,
            has_governance: false,
          };
        }
        const b = byId[o.event_id];
        b.kinds.push(o.kind);
        b.sources.add(o.source);
        if (o.kind === 'worker') b.has_worker = true;
        if (o.kind === 'mutation') b.has_mutation = true;
        if (o.kind === 'governance') b.has_governance = true;
      }

      const eventIds = Object.keys(byId);
      expect(eventIds.length, `${filename}: no events observed for this delivery`).toBeGreaterThan(0);

      // Point 1: at least one current-run event has worker kind.
      // The substrate stages WEBHOOK_EVENT_STAGED for each item.
      const hasWorker = Object.values(byId).some((b) => b.has_worker);
      expect(hasWorker, `${filename}: no worker execution observed`).toBe(true);

      // Point 2: at least one current-run event has governance kind.
      // The constitutional kernel records a divergence when the
      // PERSIST_STAGED_EVENT guard rejects (inferred_state_not_ready).
      // This is constitutional — the runtime handled the event.
      const hasGovernance = Object.values(byId).some((b) => b.has_governance);
      expect(hasGovernance, `${filename}: no governance observation`).toBe(true);

      // Point 3: no drift findings from this run.
      const drift = harness.driftDetector.snapshot();
      expect(drift, `${filename}: drift detected: ${JSON.stringify(drift)}`).toEqual([]);

      writerRef.writer.bumpAssertions(3);
    });
  }
});
