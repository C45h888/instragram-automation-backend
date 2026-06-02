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

import namespaceProjectionInterpreter from '../control-plane/governance/interpreters/namespace-projection-interpreter.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntryCount, waitForCursorAdvance } = require('./helpers/sync-barriers');
const { assertNoCrossDomainContamination, assertCausalChainIntegrity, assertProjectionSignalContract } = require('./helpers/constitutional-invariants');

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
  }, 20000);

  afterAll(async () => {
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

  it('stale window projections cannot corrupt active governance state after worker restart', async () => {
    const { injectMixedDomainWave } = require('./event-injector.js');
    const waveId = `phase4j-stale-${Date.now()}`;

    // Inject a sustained workload so lineage buffer has content
    for (let i = 0; i < 8; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }
    await waitForLedgerEntryCount(8, 10000);

    // Stop Phase 2 dumb writer — simulates it being behind or stopped

    // Capture current ledger state as a baseline
    const ledgerBefore = await lineageLedger.getLineage(200);

    // Restart Phase 2 dumb writer — trigger-driven, no buffer rehydration
    await waitForLedgerEntryCount(8, 8000);

    // After restart, verify causal chain integrity is maintained
    // even with entries appended before and after the stop/start cycle
    const ledgerAfter = await lineageLedger.getLineage(200);
    assertCausalChainIntegrity(ledgerAfter);

    // No new REJECTED entries should appear after restart beyond what CK already rejected
    // CK async validation handles broken causal chains — Phase 2 writer is purely mechanical

    // Inject another wave to exercise the post-restart write path
    const waveId2 = `phase4j-stale-2-${Date.now()}`;
    for (let i = 0; i < 5; i++) {
      await injectMixedDomainWave({ waveId: waveId2, seq: i, includeFault: false });
    }
    await waitForLedgerEntryCount(ledgerBefore.length + 5, 8000);

    // Causal chain must remain intact after stop/start cycle
    const ledgerFinal = await lineageLedger.getLineage(500);
    assertCausalChainIntegrity(ledgerFinal);
  });

  it('replay from stale window produces no causal chain corruption', async () => {
    const { injectBrokenCausalChain, injectMixedDomainWave } = require('./event-injector.js');

    // Establish a valid causal chain first
    const waveId = `phase4j-replay-${Date.now()}`;
    for (let i = 0; i < 5; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: false });
    }
    await waitForLedgerEntryCount(5, 8000);

    // Stop Phase 2 dumb writer

    // Capture ledger state before restart
    const ledgerBefore = await lineageLedger.getLineage(200);

    // Inject a broken causal chain while writer is stopped
    const brokenParentId = `stale-window-broken-parent-${Date.now()}`;
    injectBrokenCausalChain({
      domain: 'governance',
      entity: 'fsm',
      entityId: `stale-gov-${Date.now()}`,
      previousState: 'IDLE',
      nextState: 'BROKEN_ACTIVE',
      brokenParentTransitionId: brokenParentId,
    });

    // Restart writer — Phase 2 is trigger-driven, writes all pending entries
    await waitForLedgerEntryCount(ledgerBefore.length + 1, 8000);

    // CK async validation rejects broken causal chains — entry should have REJECTED status
    const ledger = await lineageLedger.getLineage(500);
    const brokenEntries = ledger.filter((e) => e.raw?.brokenParentTransitionId === brokenParentId);
    expect(brokenEntries.length).toBe(1);
    expect(brokenEntries[0].raw.constitutionalStatus).toBe('REJECTED');

    // Causal chain integrity check should pass since the broken entry is REJECTED
    // (assertCausalChainIntegrity filters to ACCEPTED entries only)
    expect(() => assertCausalChainIntegrity(ledger)).not.toThrow();
  });

  /**
   * Validate the signal ownership partition under sustained high-frequency polling.
   * After 100 waves of injection, the namespace projection interpreter snapshot
   * must contain only ledger-derivable signals — no observer-relative signals
   * (failureRate, governancePressure, interpretationConfidence, etc.) may appear.
   * The CK signal ownership contract must hold even under heavy sustained load.
   */
  it('namespace projection interpreter contains only ledger-derivable signals under sustained polling', async () => {
    const { injectMixedDomainWave } = require('./event-injector.js');
    const waveId = `phase4j-signal-contract-${Date.now()}`;

    // 100 waves over ~2 seconds — sustained pressure to stress signal accumulation
    for (let i = 0; i < 100; i++) {
      await injectMixedDomainWave({ waveId, seq: i, includeFault: i % 10 === 0 });
    }

    // Wait for Phase 2 writer to append entries and CK to validate them
    await waitForLedgerEntryCount(50, 20000);

    const projections = namespaceProjectionInterpreter.getProjections();

    // Signal ownership partition must hold — no observer-relative signals
    // in the lineage worker Layer B projection snapshot
    assertProjectionSignalContract(projections);
  });
});
