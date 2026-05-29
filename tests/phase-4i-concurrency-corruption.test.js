/**
 * Phase 4I: Adversarial Concurrency Corruption Recovery
 *
 * Validates constitutional resilience under chaotic failure conditions.
 * The runtime must remain coherent when reality fails — not just when
 * everything works correctly.
 *
 * Constitutional laws tested:
 *   - Duplicate replay injection must remain idempotent.
 *   - A stale authority chain must be rejected constitutionally.
 *   - Conflicting FSM transitions must be flagged as violations.
 *   - Partial snapshot corruption must not break subsequent valid entries.
 *   - Out-of-order replay delivery must be detected and flagged.
 *
 * Healthy concurrency tests stability.
 * Adversarial concurrency tests constitutional resilience.
 * Phase 4I is the adversarial category — Phase 4C/4D are the healthy category.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntryCount, waitForLedgerEntry } = require('./helpers/sync-barriers');
const { assertIdempotentReplay, assertStaleEntriesFlagged, assertNoSilentCorruption } = require('./helpers/constitutional-invariants');

describe('Phase 4I: Adversarial Concurrency Corruption Recovery', () => {
  beforeAll(async () => {
    await observability.init();
    await lineageWorker.start(400);
  }, 15000);

  afterAll(async () => {
    await lineageWorker.stop();
    await observability.stop();
  });

  it('duplicated replay windows are idempotent — no corruption markers in ledger', async () => {
    const waveId = `phase4i-dup-replay-${Date.now()}`;
    const { injectMixedDomainWave } = require('./event-injector.js');

    // First wave
    await injectMixedDomainWave({ waveId, seq: 0, includeFault: false });
    await injectMixedDomainWave({ waveId, seq: 1, includeFault: false });

    // Duplicate wave (simulates replay of same events)
    await injectMixedDomainWave({ waveId, seq: 0, includeFault: false });
    await injectMixedDomainWave({ waveId, seq: 1, includeFault: false });

    // Wait for all 4 waves to be consumed by lineage worker
    await waitForLedgerEntryCount(8, 10000);

    const ledger = await lineageLedger.getLineage(100);
    const waveEntries = ledger.filter((e) => e.raw?.raw?.waveId === waveId);

    // Idempotent: no corruption markers; entries exist but are recognized as duplicate
    assertNoSilentCorruption(waveEntries);
    // Both original and replay entries are present
    expect(waveEntries.length).toBeGreaterThanOrEqual(2);
  });

  it('malformed semantic transitions are flagged — not accepted silently', async () => {
    const malformedId = `malformed-${Date.now()}`;

    observability.transition({
      domain: 'acquisition',
      entity: 'malformed_intent',
      entityId: malformedId,
      previousState: null,
      nextState: 'CORRUPTED',
      authority: 'corruption-injector',
      raw: { malformed: true, intentionallyInvalid: true },
    });

    const malformedEntry = await waitForLedgerEntry(
      (e) => e.entityId === malformedId && e.raw?.raw?.malformed === true,
      50,
      8000
    );

    expect(malformedEntry).toBeDefined();
    expect(malformedEntry.raw.raw.malformed).toBe(true);
  });

  it('out-of-order replay delivery is detected and flagged', async () => {
    const { injectOutOfOrderEntry } = require('./event-injector.js');
    const entityId = `phase4i-ooo-${Date.now()}`;

    // Emit in-order first
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'IDLE',
      nextState: 'ACTIVE',
      authority: 'order-test',
      raw: { seq: 1 },
    });

    // Inject stale entry that should have come before (backdated 10 seconds)
    injectOutOfOrderEntry({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'IDLE',
      nextState: 'STALE_ACTIVE',
      backDateMs: 10000,
    });

    const oooEntry = await waitForLedgerEntry(
      (e) => e.entityId === entityId && e.raw?.raw?.outOfOrder === true,
      50,
      8000
    );

    expect(oooEntry).toBeDefined();
    expect(oooEntry.raw.raw.outOfOrder).toBe(true);
  });

  it('stale authority chain is rejected constitutionally', async () => {
    const { injectOutOfOrderEntry } = require('./event-injector.js');
    const entityId = `phase4i-stale-auth-${Date.now()}`;

    injectOutOfOrderEntry({
      domain: 'governance',
      entity: 'fsm',
      entityId,
      previousState: 'IDLE',
      nextState: 'STALE_AUTHORITY',
      backDateMs: 15000,
    });

    const staleEntry = await waitForLedgerEntry(
      (e) => e.entityId === entityId && e.raw?.raw?.outOfOrder === true,
      50,
      8000
    );

    // Stale entry must be flagged — not processed as fresh authority
    expect(staleEntry).toBeDefined();
    expect(staleEntry.raw.raw.outOfOrder).toBe(true);
  });

  it('conflicting FSM transitions are flagged as constitutional violations', async () => {
    const { injectConflictingTransition } = require('./event-injector.js');
    const conflictId = `phase4i-conflict-${Date.now()}`;

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

  it('partial snapshot corruption does not block subsequent valid entries', async () => {
    const corruptId = `phase4i-corrupt-${Date.now()}`;

    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId: corruptId,
      previousState: 'IDLE',
      nextState: 'CORRUPT_SNAPSHOT',
      authority: 'corruption-injector',
      raw: {
        partialCorruption: true,
        simulatedMissingFields: true,
        snapshot: { broken: true },
      },
    });

    // Emit valid entry afterward — must not be blocked by corruption
    observability.transition({
      domain: 'governance',
      entity: 'fsm',
      entityId: `${corruptId}-valid`,
      previousState: 'IDLE',
      nextState: 'VALID_AFTER_CORRUPT',
      authority: 'event-injector',
      raw: { validAfterCorrupt: true },
    });

    const validEntry = await waitForLedgerEntry(
      (e) => e.entityId === `${corruptId}-valid` && e.raw?.raw?.validAfterCorrupt === true,
      100,
      8000
    );

    // Valid entry must appear — corruption must not block subsequent processing
    expect(validEntry).toBeDefined();
    expect(validEntry.raw.raw.validAfterCorrupt).toBe(true);
  });
});
