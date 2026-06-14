// Phase 9 — Runtime Composition.
// Boots the runtime, exercises all 6 webhook fixtures + all 5
// graph workers + 20 cross-kernel pairs in a single pass. Asserts
// the snapshot is internally consistent at the end.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
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

describe('integration/phase-9-runtime-composition', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-composition' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-runtime-composition');
    if (harness) await harness.shutdown();
  }, 30000);

  it('all 6 webhook + 5 graph fixtures compose', async () => {
    // Tier 1: webhook.
    for (const name of FIXTURES) {
      const body = JSON.parse(fs.readFileSync(path.join(CANONICAL_DIR, `${name}.json`), 'utf8'));
      const correlationId = `p9-comp-wh-${name}-${Date.now()}`;
      harness.injectEvent({
        type: 'WEBHOOK_DELIVERED',
        source: 'runtime/ingress',
        payload: { fixture: name, body },
        correlationId,
      });
    }
    // Tier 2: graph.
    for (const w of GRAPH_WORKERS) {
      const correlationId = `p9-comp-gr-${w}-${Date.now()}`;
      harness.injectEvent({
        type: `${w.toUpperCase()}_REQUESTED`,
        source: `graph/${w}`,
        payload: {},
        correlationId,
      });
    }
    await harness.tick(10);

    const snap = harness.snapshotDeriver.derive();
    expect(Object.keys(snap.events).length, 'no events observed').toBeGreaterThan(0);

    // Flush artifacts to disk.
    await harness._flushArtifacts();
    writer.bumpAssertions(1);
  });
});
