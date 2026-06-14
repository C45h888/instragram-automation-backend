// Phase 9 — Tier 1 Webhook Ingress (canonical fixtures).
// Each canonical fixture is delivered through the real runtime.
// The runtime parses → governance → FSM → worker → mutation.
// The recorder-observer captures what the runtime did; the test
// asserts against the observation log, not against a fabricated chain.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CANONICAL_DIR = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical');

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
    writerRef.writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writerRef.writer.writeSummary('webhook', 'runtime-webhook-ingress');
    if (harness) await harness.shutdown();
  }, 30000);

  for (const name of FIXTURES) {
    it(`delivers ${name} through the real runtime and observes the chain`, async () => {
      const body = JSON.parse(fs.readFileSync(path.join(CANONICAL_DIR, `${name}.json`), 'utf8'));
      const correlationId = `p9-${name}-${Date.now()}`;

      // Inject the webhook body as a runtime event. The runtime
      // processes it through its real ingress path; the recorder
      // observes what the runtime did.
      harness.injectEvent({
        type: 'WEBHOOK_DELIVERED',
        source: 'runtime/ingress',
        payload: { fixture: name, body },
        correlationId,
      });
      await harness.tick(3);

      // Snapshot the observation log AFTER the runtime processed.
      const snapshot = harness.snapshotDeriver.derive();
      const event = snapshot.events[correlationId];
      expect(event, `event ${correlationId} not observed`).toBeDefined();

      // Worker executed and mutation landed.
      expect(event.worker_count, `${name}: no worker invocation observed`).toBeGreaterThan(0);
      expect(event.mutation_count, `${name}: no mutation observed`).toBeGreaterThan(0);

      // Ownership chain is recorded.
      const ownership = harness.ownershipTracer.snapshot()[correlationId];
      expect(ownership, `${name}: no ownership record`).toBeDefined();
      expect(ownership.ingress.owner, `${name}: missing ingress owner`).toBeTruthy();
      expect(ownership.mutation.owner, `${name}: missing mutation owner`).toBe('mutation-substrate');

      // No drift findings.
      const drift = harness.driftDetector.snapshot();
      const relevant = drift.filter((d) => d.event_id === correlationId);
      expect(relevant, `${name}: drift detected: ${JSON.stringify(relevant)}`).toEqual([]);

      writerRef.writer.bumpAssertions(4);
    });
  }
});
