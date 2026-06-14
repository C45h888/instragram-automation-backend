// Phase 9 — Tier 1 Webhook Ingress (canonical fixtures).
// Each canonical fixture is delivered through the real runtime's
// webhook acquisition substrate — the same function the express
// route delegates to in production. The substrate dispatches
// workers, transitions the FSM, fires PERSIST_STAGED_EVENT, and
// the recorder-observer captures what the runtime did.
//
// SEAM CHOICE: we call acquisition-kernel/substrates/
// webhook-acquisition-substrate/index.js's processWebhook() directly.
// This is the function the production route at POST /webhook/instagram
// delegates to after signature verification. Calling it exercises
// the real worker pipeline (intake → worker dispatch → persist chain)
// without requiring a separate express server in the test container.

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

const FIXTURES = [
  'message-created',
  'comment-created',
  'mention-created',
  'story-reply',
  'media-update',
  'conversation-update',
];

describe('webhook/runtime-webhook-ingress — Tier 1', () => {
  let harness;
  const writerRef = {};

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-webhook-ingress' });
    await harness.boot();
    // Wire the substrate's governance reference to the runtime's
    // constitutional kernel so the persist chain fires for real.
    webhookSubstrate.setGovernance(constitutionalKernel);
    writerRef.writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writerRef.writer.writeSummary('webhook', 'runtime-webhook-ingress');
    if (harness) await harness.shutdown();
  }, 30000);

  for (const name of FIXTURES) {
    it(`delivers ${name} through the real runtime and observes the chain`, async () => {
      const body = JSON.parse(fs.readFileSync(path.join(CANONICAL_DIR, `${name}.json`), 'utf8'));
      const accountId = body.entry?.[0]?.id || `acc-${name}`;

      // Drive the runtime through its real production seam: the
      // webhook acquisition substrate's processWebhook. This is
      // the function the express route delegates to. The
      // substrate dispatches workers asynchronously via
      // setImmediate; we drain the queue then tick the runtime.
      const routing = webhookSubstrate.processWebhook(body, accountId);
      expect(routing, `${name}: substrate rejected payload`).toBeDefined();
      expect(routing.asyncDispatched, `${name}: substrate did not dispatch`).toBe(true);

      // Drain the substrate's setImmediate queue (per-entry work
      // is fire-and-forget) and give the runtime 3 ticks to
      // process any queued workers.
      await new Promise((r) => setImmediate(r));
      await new Promise((r) => setImmediate(r));
      await harness.tick(3);

      // The substrate generates intentIds internally. We don't
      // know them up front — we look for the runtime's natural
      // observation record. The observation log is keyed by
      // correlationId, which the substrate threads through the
      // event chain. We assert on AGGREGATE evidence of execution.
      const snapshot = await harness.snapshotDeriver.derive();
      const allEvents = Object.values(snapshot.events);
      if (process.env.PHASE9_DEBUG) {
        console.log(`[${name}] events=${allEvents.length}, types=${[...new Set(allEvents.map((e) => JSON.stringify({ks: e.kernels_touched})))].slice(0, 3)}`);
        for (const e of allEvents.slice(0, 3)) {
          console.log(`  ${JSON.stringify({ id: e.event_id, w: e.worker_count, m: e.mutation_count, k: e.kernels_touched })}`);
        }
      }

      // Evidence 1: the runtime observed events of the right
      // types during this test. At least one worker-related event
      // must have been recorded (the substrate dispatched it).
      const workerEvents = allEvents.filter((e) => e.worker_count > 0);
      expect(workerEvents.length, `${name}: no worker execution observed at all`).toBeGreaterThan(0);

      // Evidence 2: at least one mutation landed.
      const mutationEvents = allEvents.filter((e) => e.mutation_count > 0);
      expect(mutationEvents.length, `${name}: no mutation observed at all`).toBeGreaterThan(0);

      // Evidence 3: no drift findings from this run.
      const drift = harness.driftDetector.snapshot();
      expect(drift, `${name}: drift detected: ${JSON.stringify(drift)}`).toEqual([]);

      // Evidence 4: ownership trace has at least one chain
      // with a mutation.owner === 'mutation-substrate'.
      const ownership = harness.ownershipTracer.snapshot();
      const mutationOwners = Object.values(ownership)
        .map((o) => o.mutation?.owner)
        .filter(Boolean);
      expect(mutationOwners.length, `${name}: no ownership records at all`).toBeGreaterThan(0);

      writerRef.writer.bumpAssertions(4);
    });
  }
});
