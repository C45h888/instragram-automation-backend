import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLogSize, waitForLedgerEntry } = require('./helpers/sync-barriers');

describe('Phase 4B: Relay-to-Lineage Immutability', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(40);
    await lineageWorker.start(500);
  }, 20000);

  afterAll(async () => {
    await lineageWorker.stop();
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('persists semantic projection entries without mutation to canonical fields', async () => {
    const start = observability.query.getLogSize();

    // Wait for telemetry workers to emit at least one SEMANTIC_PROJECTION_TRANSITION
    await waitForLogSize(start + 1, 4000);

    const { entries } = observability.query.getEntriesSince(start);
    const projections = entries.filter((e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION');
    expect(projections.length).toBeGreaterThan(0);

    // Poll ledger until lineage worker has consumed and persisted the projection entry
    const ledgerProjection = await waitForLedgerEntry(
      (e) => e.raw?.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION',
      200,
      8000
    );
    expect(ledgerProjection).toBeDefined();

    const source = projections.find(
      (p) => p.entityId === ledgerProjection.entityId
    );
    expect(source).toBeDefined();
    expect(ledgerProjection.domain).toBe(source.domain);
    expect(ledgerProjection.entity).toBe(source.entity);
    expect(ledgerProjection.nextState).toBe(source.nextState);
    expect(ledgerProjection.authority).toBe(source.authority);
  });
});
