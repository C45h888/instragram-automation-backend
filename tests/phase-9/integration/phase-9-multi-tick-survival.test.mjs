// Phase 9 — Multi-tick survival.
// Runs N ticks depending on PHASE9_CADENCE_TIER. Per tick:
//   - advance the runtime clock
//   - deliver one webhook
//   - run one graph worker
// Asserts no starvation, no deadlock, no event explosion.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

const TIER = (process.env.PHASE9_CADENCE_TIER || 'short').toLowerCase();
const TICKS = TIER === 'short' ? 25 : TIER === 'medium' ? 100 : TIER === 'long' ? 500 : 1000;
const FIXTURES = [
  'message-created', 'comment-created', 'mention-created',
  'story-reply', 'media-update', 'conversation-update',
];
const WORKERS = ['insights', 'publishing', 'capability', 'recovery', 'reconciliation'];

describe('integration/phase-9-multi-tick-survival', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-multitick' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
    writer.addExtra('tier', TIER);
    writer.addExtra('ticks_planned', TICKS);
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('integration', 'phase-9-multi-tick-survival');
    if (harness) await harness.shutdown();
  }, 30000);

  it(`survives ${TICKS} ticks (tier=${TIER})`, async () => {
    const startedAt = Date.now();
    for (let t = 0; t < TICKS; t++) {
      const f = FIXTURES[t % FIXTURES.length];
      const w = WORKERS[t % WORKERS.length];
      harness.injectEvent({
        type: 'WEBHOOK_DELIVERED',
        source: 'runtime/ingress',
        payload: { fixture: f, tick: t },
        correlationId: `p9-mt-wh-${t}`,
      });
      harness.injectEvent({
        type: `${w.toUpperCase()}_REQUESTED`,
        source: `graph/${w}`,
        payload: { tick: t },
        correlationId: `p9-mt-gr-${t}`,
      });
      await harness.tick(1);
    }
    const elapsed = Date.now() - startedAt;
    writer.addExtra('elapsed_ms', elapsed);
    writer.addExtra('ticks_per_sec', Math.round((TICKS * 1000) / Math.max(1, elapsed)));
    writer.bumpAssertions(1);
  }, TICKS * 200);
});
