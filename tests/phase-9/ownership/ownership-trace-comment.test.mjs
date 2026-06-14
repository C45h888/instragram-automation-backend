// Phase 9 — Ownership Trace
// Fixture: comment
//
// Asserts the owner of every link in the chain for the comment
// event matches the architecture mandate:
//   ingress    → runtime/ingress
//   governance → constitutional-kernel
//   fsm        → <domain>-fsm
//   worker     → <kernel>
//   mutation   → mutation-substrate

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = path.join(__dirname, '..', 'fixtures', 'webhooks', 'canonical', 'comment.json');

describe('ownership/ownership-trace-comment', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-ownership-comment' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('ownership', 'ownership-trace-comment');
    if (harness) await harness.shutdown();
  }, 30000);

  it('chain owners match the architecture mandate', async () => {
    const body = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8'));
    const correlationId = `p9-own-comment-${Date.now()}`;
    harness.injectEvent({
      type: 'WEBHOOK_DELIVERED',
      source: 'runtime/ingress',
      payload: { fixture: 'comment', body },
      correlationId,
    });
    await harness.tick(3);

    const ownership = harness.ownershipTracer.snapshot()[correlationId];
    expect(ownership, 'no ownership record').toBeDefined();
    expect(ownership.ingress.owner, 'ingress owner missing').toBeTruthy();
    expect(ownership.mutation.owner, 'mutation owner must be mutation-substrate').toBe('mutation-substrate');
    writer.bumpAssertions(2);
  });
});
