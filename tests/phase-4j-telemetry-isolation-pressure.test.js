/**
 * Phase 4J: Telemetry Isolation Under Pressure
 *
 * Validates that telemetry projection workers cannot recursively influence
 * constitutional state under sustained high-frequency polling. This closes
 * the gap identified in the architecture audit: telemetry isolation was
 * only verified statically (Phase 4A file-content checks) but never tested
 * at runtime under pressure.
 *
 * Constitutional law:
 *   Telemetry projections may not recursively influence constitutional state.
 *   No feedback amplification loop may form between projection output and
 *   governance domain state transitions.
 *
 * Tests:
 *   1. High-frequency telemetry polling does not create governance transitions
 *   2. Projection output remains bounded — no amplification over sustained window
 *   3. No cross-contamination from projection workers into constitutional domains
 *   4. Projection worker authority never appears on governance/execution/acquisition entries
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntryCount, waitForCursorAdvance } = require('./helpers/sync-barriers');
const { assertNoCrossDomainContamination } = require('./helpers/constitutional-invariants');

const PROJECTION_AUTHORITIES = [
  'runtime-projection-worker',
  'integrity-projection-worker',
  'authority-projection-worker',
  'health-projection-worker',
  'systemic-pressure-projection-worker',
];

const CONSTITUTIONAL_DOMAINS = ['governance', 'execution', 'acquisition', 'publishing', 'scheduling'];

describe('Phase 4J: Telemetry Isolation Under Pressure', () => {
  beforeAll(async () => {
    await observability.init();
    // Start telemetry workers at high frequency to stress isolation
    await telemetryWorkers.startAll(20);
    await lineageWorker.start(300);
  }, 20000);

  afterAll(async () => {
    await lineageWorker.stop();
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('projection worker authority never appears on constitutional domain entries', async () => {
    // Inject governance transitions to create legitimate activity
    const { injectMixedDomainWave, injectReconciliationTick } = require('./event-injector.js');
    const waveId = `phase4j-isolation-${Date.now()}`;

    // Run sustained workload to give telemetry workers many polling cycles
    for (let i = 0; i < 10; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }
    await injectReconciliationTick();

    // Wait for lineage worker to consume all entries
    await waitForLedgerEntryCount(10, 10000);

    const ledger = await lineageLedger.getLineage(300);

    // Assert no entry exists where a projection worker's authority
    // appears on a constitutional domain transition
    const contamination = ledger.filter(
      (e) =>
        CONSTITUTIONAL_DOMAINS.includes(e.domain) &&
        PROJECTION_AUTHORITIES.includes(e.authority)
    );

    expect(contamination.length).toBe(0);
  });

  it('telemetry polling does not create feedback amplification — entry rate stays bounded', async () => {
    const { injectMixedDomainWave } = require('./event-injector.js');
    const waveId = `phase4j-amplify-${Date.now()}`;

    // Capture baseline ledger size after injecting a fixed workload
    const baselineBefore = await lineageLedger.getSize();

    for (let i = 0; i < 5; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }

    // Wait for consumption
    await waitForLedgerEntryCount(baselineBefore + 1, 8000);

    const entriesAfterFirstWave = await lineageLedger.getSize();

    // Inject another identical workload
    for (let i = 5; i < 10; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }

    await waitForLedgerEntryCount(entriesAfterFirstWave + 1, 8000);

    const entriesAfterSecondWave = await lineageLedger.getSize();

    // Growth should be roughly proportional to injected workload.
    // If telemetry was amplifying, the second wave would produce
    // disproportionately more entries than the first.
    const firstGrowth = entriesAfterFirstWave - baselineBefore;
    const secondGrowth = entriesAfterSecondWave - entriesAfterFirstWave;

    // Allow some variance but second wave should not be dramatically larger
    expect(secondGrowth).toBeLessThanOrEqual(firstGrowth * 3);
  });

  it('projection entries themselves never appear as governance state changes', async () => {
    const start = observability.query.getLogSize();

    // Inject a single governance transition to trigger projection workers
    observability.transition({
      domain: 'governance',
      entity: 'runtime',
      entityId: 'governance-runtime',
      previousState: 'HEALTHY',
      nextState: 'DEGRADED',
      authority: 'phase4j-test',
      raw: { test: true },
    });

    // Wait for both the transition AND any projection responses
    await waitForCursorAdvance(start, 8000);

    const { entries } = observability.query.getEntriesSince(start);

    // Projection entries should exist (telemetry workers emit them)
    const projectionEntries = entries.filter(
      (e) => e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );
    expect(projectionEntries.length).toBeGreaterThan(0);

    // But NO projection entry should have domain='governance' and entity='runtime'
    // (which would indicate telemetry feeding back into governance)
    const governanceFeedback = projectionEntries.filter(
      (e) => e.domain === 'governance' && e.entity === 'runtime'
    );
    expect(governanceFeedback.length).toBe(0);
  });

  it('sustained polling over 200+ ticks produces no cross-domain contamination', async () => {
    const { injectMixedDomainWave } = require('./event-injector.js');
    const waveId = `phase4j-sustained-${Date.now()}`;

    // Run enough waves to generate 200+ observability entries
    for (let i = 0; i < 25; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: i % 7 === 0 });
    }

    // Wait for lineage worker to consume all entries
    await waitForLedgerEntryCount(20, 15000);

    const ledger = await lineageLedger.getLineage(500);

    // Use the formal invariant to check for forbidden cross-domain pairs
    assertNoCrossDomainContamination(ledger, [
      { sourceDomain: 'projection', targetDomain: 'governance' },
      { sourceDomain: 'projection', targetDomain: 'execution' },
      { sourceDomain: 'telemetry', targetDomain: 'governance' },
    ]);

    // Additionally verify no SEMANTIC_PROJECTION_TRANSITION entries
    // carry a constitutional domain authority chain
    const projectionEntries = ledger.filter(
      (e) => e.raw?.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );
    const suspicious = projectionEntries.filter(
      (e) => CONSTITUTIONAL_DOMAINS.includes(e.domain) && e.domain !== 'projection'
    );
    expect(suspicious.length).toBe(0);
  });
});
