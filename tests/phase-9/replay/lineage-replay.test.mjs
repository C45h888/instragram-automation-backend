// Phase 9 — Lineage Replay.
// Replays the recorder-observer's observation log, reconstructs the
// expected post-run state, and compares against the runtime's
// actual mutation stream. Diverged keys or missing observations
// are causality leaks.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('replay/lineage-replay', () => {
  let harness;
  let writer;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-replay' });
    await harness.boot();
    writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });
  }, 60000);

  afterAll(async () => {
    writer.writeSummary('replay', 'lineage-replay');
    if (harness) await harness.shutdown();
  }, 30000);

  it('replay matches the actual runtime state', async () => {
    // Inject a small, deterministic chain.
    const correlationId = `p9-replay-${Date.now()}`;
    harness.injectEvent({
      type: 'WEBHOOK_DELIVERED',
      source: 'runtime/ingress',
      payload: { fixture: 'message-created' },
      correlationId,
    });
    await harness.tick(3);

    const replay = await harness.replayEngine.replay();
    writer.addExtra('replay', replay);

    // The replay engine's primary invariant: no causality leak.
    // In a clean run, missing_observations is empty (every
    // worker has at least one terminating mutation) and
    // diverged_keys is empty (every mutation is counted).
    expect(replay.missing_observations, JSON.stringify(replay)).toEqual([]);
    expect(replay.diverged_keys, JSON.stringify(replay)).toEqual([]);
    writer.bumpAssertions(2);
  });
});
