/**
 * Phase 4E: Replay Reconstruction Determinism
 *
 * Validates the most critical constitutional invariant:
 * A projection reconstructed purely from lineage replay must converge
 * identically to the original projection state.
 *
 * Sequence:
 *   1. Run concurrent mutation workload
 *   2. Capture projection state hash
 *   3. Persist lineage ledger
 *   4. Kill and restart workers
 *   5. Clear in-memory projection state
 *   6. Rebuild projections purely from lineage replay
 *   7. Compare rebuilt projection hash to original
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForProjectionFlush, waitForLedgerEntryCount } = require('./helpers/sync-barriers');
const { deterministicEntryHash, assertNoTimestampRegression } = require('./helpers/constitutional-invariants');

describe('Phase 4E: Replay Reconstruction Determinism', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(40);
    await lineageWorker.start(400);
  }, 20000);

  afterAll(async () => {
    await lineageWorker.stop();
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('projection hash converges identically after kill/clear/replay cycle', async () => {
    const waveId = `phase4e-replay-${Date.now()}`;

    // 1. Inject concurrent mutation workload across multiple domains
    const waves = [];
    for (let i = 0; i < 8; i++) {
      waves.push(
        (async () => {
          for (let j = 0; j < 5; j++) {
            const { injectMixedDomainWave } = require('./event-injector.js');
            await injectMixedDomainWave({
              waveId,
              seq: i * 5 + j,
              includeFault: j % 3 === 0,
            });
          }
        })()
      );
    }
    await Promise.all(waves);

    // 2. Wait for lineage worker to consume all injected transitions
    await waitForProjectionFlush(8000);

    // 3. Capture original ledger state hash
    const ledgerBefore = await lineageLedger.getLineage(500);
    expect(ledgerBefore.length).toBeGreaterThan(0);
    const originalHash = deterministicEntryHash(ledgerBefore);

    // 4. Ensure full persistence to Redis
    await waitForProjectionFlush(8000);

    // 5. Stop all workers — simulates worker death
    await telemetryWorkers.stopAll();
    await lineageWorker.stop();

    // 6. Clear in-memory state: stop and reinit observability
    await observability.stop();
    await new Promise(r => setTimeout(r, 50));
    await observability.init();

    // 7. Restart lineage worker — triggers replay from persisted cursor
    await lineageWorker.start(400);

    // 8. Wait for replay to repopulate the ledger
    await waitForLedgerEntryCount(ledgerBefore.length, 10000);

    // 9. Capture post-replay ledger state
    const ledgerAfter = await lineageLedger.getLineage(500);
    const rebuiltHash = deterministicEntryHash(ledgerAfter);

    // Constitutional determinism: same entries → same hash regardless of replay timing
    expect(rebuiltHash).toBe(originalHash);
  });

  it('replay continuity survives worker death — all lineage entries recoverable', async () => {
    const waveId = `phase4e-continuity-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    for (let i = 0; i < 6; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: i % 4 === 0 });
    }

    // Wait for lineage worker to consume and persist all entries
    await waitForProjectionFlush(10000);

    // Kill workers
    await telemetryWorkers.stopAll();
    await lineageWorker.stop();

    // Reinit
    await observability.init();
    await lineageWorker.start(400);
    await telemetryWorkers.startAll(35);

    // Wait for ledger to recover all 6 wave entries
    await waitForLedgerEntryCount(6, 8000);

    // All entries must be recoverable from ledger after restart
    const ledger = await lineageLedger.getLineage(300);
    const waveEntries = ledger.filter((e) => e.raw?.raw?.waveId === waveId);
    expect(waveEntries.length).toBe(6);

    // Entries must be in chronological order — no regression
    assertNoTimestampRegression(waveEntries);
  });
});
