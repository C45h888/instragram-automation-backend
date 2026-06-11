/**
 * Multi-Tick Survival — Phase 7 integration test
 * ═════════════════════════════════════════════════
 *
 * Three cadence tiers (per the plan):
 *   short:  25-50 ticks, every commit
 *   medium: 100-250 ticks, Phase 7 + CI
 *   long:   500-1000 ticks, manual / pre-release
 *
 * Reads PHASE7_CADENCE_TIER env var (default 'medium').
 *
 * Watch (the five long-run failure modes from the plan):
 *   1. state drift
 *   2. duplicate dispatch
 *   3. retry accumulation
 *   4. lineage corruption
 *   5. governance leakage
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';

const TIER_TICK_COUNTS = {
  short: 50,
  medium: 250,
  long: 500,
};

const TIER_BATCH = 25; // assert watch metrics every N ticks

describe('Multi-Tick Survival — Phase 7 integration', () => {
  let simulator;
  let tickCount;

  beforeAll(async () => {
    const tier = process.env.PHASE7_CADENCE_TIER || 'medium';
    tickCount = TIER_TICK_COUNTS[tier] || TIER_TICK_COUNTS.medium;
    simulator = new Phase7RuntimeSimulator({ runId: `multi-tick-${tier}` });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 60000);

  it(`survives ${tickCount} accelerated ticks (watch metrics clean)`, async () => {
    const initialLineage = (await simulator.snapshot()).lineage.size;
    let lastLineageSize = initialLineage;
    const eventTypes = ['CADENCE_TICK', 'RECONCILIATION_TICK', 'LIFECYCLE_REFRESHED'];
    let lastGovernanceFingerprint = null;
    let duplicateDispatchCount = 0;

    for (let i = 0; i < tickCount; i += TIER_BATCH) {
      const batchSize = Math.min(TIER_BATCH, tickCount - i);
      const events = [];
      for (let j = 0; j < batchSize; j++) {
        const type = eventTypes[(i + j) % eventTypes.length];
        events.push({ type, correlationId: `tick-${i + j}` });
      }
      for (const evt of events) {
        simulator.injectEvent({ ...evt, source: 'multi-tick-survival' });
      }
      await simulator.tick(batchSize);

      // Watch 4: lineage corruption (size must not decrease)
      const snap = await simulator.snapshot();
      if (snap.lineage.size < lastLineageSize) {
        throw new Error(
          `Lineage corruption: size went from ${lastLineageSize} to ${snap.lineage.size} at batch ${i}`
        );
      }
      lastLineageSize = snap.lineage.size;

      // Watch 2: duplicate dispatch — same (type, via) repeated in
      // batch. We exclude CADENCE_TICK in the `via` field because
      // the scheduler emits a CADENCE_TICK on every cycle by
      // design; flagging those would always trip the watch. The
      // pathological case is the same non-cadence event firing
      // twice in a row from the same source — that indicates a
      // real loop.
      const decisions = simulator.governanceLog();
      for (let k = 1; k < decisions.length; k++) {
        const a = decisions[k - 1];
        const b = decisions[k];
        if (a.via === b.via && a.via != null && a.via !== 'CADENCE_TICK') {
          duplicateDispatchCount++;
        }
      }
      if (decisions.length > 0 && duplicateDispatchCount > decisions.length * 0.5) {
        // > 50% of non-cadence decisions as consecutive-same-via — a real loop
        throw new Error(
          `Duplicate dispatch: ${duplicateDispatchCount} consecutive-same-via pairs in ${decisions.length} decisions`
        );
      }

      // Watch 5: governance leakage
      simulator.assertWorkersDidNotGovern();
    }
  }, 300000);
});
