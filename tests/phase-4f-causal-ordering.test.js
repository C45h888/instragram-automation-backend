/**
 * Phase 4F: Causal Ordering Guarantees
 *
 * Validates explicit ordering invariants that the Node.js event loop
 * does NOT guarantee under sustained concurrency:
 *   - Lineage timestamps never regress
 *   - Cursor positions remain monotonic
 *   - Replay windows never overlap illegally
 *   - No duplicate causal chain insertion (idempotent handling)
 *   - Stale authority chains are flagged
 *   - Conflicting FSM transitions are flagged as violations
 *
 * Constitutional law:
 *   A replay event may never mutate prior lineage history.
 *   Duplicate replay injection must remain idempotent.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntryCount, waitForLedgerEntry } = require('./helpers/sync-barriers');
const { assertNoTimestampRegression, assertMonotonicCursors, assertIdempotentReplay, assertStaleEntriesFlagged } = require('./helpers/constitutional-invariants');

describe('Phase 4F: Causal Ordering Guarantees', () => {
  beforeAll(async () => {
    await observability.init();
    await lineageWorker.start(400);
  }, 15000);

  afterAll(async () => {
    await lineageWorker.stop();
    await observability.stop();
  });

  it('lineage timestamps never regress across concurrent wave injection', async () => {
    const waveId = `phase4f-order-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    for (let i = 0; i < 8; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }

    await waitForLedgerEntryCount(8, 8000);
    const ledger = await lineageLedger.getLineage(500);
    const waveEntries = ledger.filter((e) => e.raw?.raw?.waveId === waveId);

    assertNoTimestampRegression(waveEntries);
    expect(waveEntries.length).toBe(8);
  });

  it('cursor positions advance monotonically — never retreat', async () => {
    const waveId = `phase4f-cursor-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    const cursors = [];
    for (let i = 0; i < 5; i++) {
      cursors.push(observability.query.getLogSize());
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }

    assertMonotonicCursors(cursors);
  });

  it('duplicate causal chain insertion is idempotent — no corruption', async () => {
    const { injectDuplicateCausalChain } = require('./event-injector.js');
    const dupId = `phase4f-dup-${Date.now()}`;

    injectDuplicateCausalChain({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId: dupId,
      previousState: 'RECEIVED',
      nextState: 'PROCESSED',
    });

    // Wait for lineage worker to consume duplicate entries
    await waitForLedgerEntryCount(2, 8000);
    const ledger = await lineageLedger.getLineage(50);
    const dupEntries = ledger.filter(
      (e) =>
        e.entityId === dupId &&
        e.raw?.raw?.duplicateEmission
    );

    // Both emissions must be recorded — idempotent means no crash, no data loss
    assertIdempotentReplay(dupEntries, 2);
    const emissions = dupEntries.map((e) => e.raw.raw.duplicateEmission).sort();
    expect(emissions).toEqual([1, 2]);
  });

  it('stale authority chain is flagged as out-of-order', async () => {
    const { injectOutOfOrderEntry } = require('./event-injector.js');
    const entityId = `phase4f-stale-${Date.now()}`;

    injectOutOfOrderEntry({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'IDLE',
      nextState: 'STALE_ACTIVE',
      backDateMs: 5000,
    });

    const staleEntry = await waitForLedgerEntry(
      (e) => e.entityId === entityId && e.raw?.raw?.outOfOrder === true,
      50,
      8000
    );

    // Entry must be present but flagged — not silently accepted as fresh
    expect(staleEntry).toBeDefined();
    expect(staleEntry.raw.raw.outOfOrder).toBe(true);
  });

  it('conflicting FSM transitions from same previous state are flagged', async () => {
    const { injectConflictingTransition } = require('./event-injector.js');
    const conflictId = `phase4f-conflict-${Date.now()}`;

    injectConflictingTransition({
      domain: 'acquisition',
      entity: 'acquisition_intent',
      entityId: conflictId,
      previousState: 'RECEIVED',
      nextStateA: 'PROCESSED',
      nextStateB: 'REJECTED',
    });

    await waitForLedgerEntryCount(2, 8000);
    const ledger = await lineageLedger.getLineage(50);
    const conflictEntries = ledger.filter((e) => e.entityId === conflictId);

    expect(conflictEntries.length).toBe(2);
    const allFlagged = conflictEntries.every((e) => e.raw?.raw?.conflictAttempt);
    expect(allFlagged).toBe(true);
  });
});
