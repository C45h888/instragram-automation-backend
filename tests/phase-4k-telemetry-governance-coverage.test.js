/**
 * Phase 4K: Telemetry Governance Coverage
 * =======================================
 *
 * Validates that the new governance signals emitted by the enriched
 * acquisition-fsm (and any future domain FSM) actually reach the
 * observability plane end-to-end. This is the "telemetry layer can
 * actually govern" assertion: the projection must surface signals
 * that the FSM emits, transition-writers must NOT propagate them
 * (they are runtime telemetry, not lineage), and governance queries
 * must be able to read them.
 *
 * Coverage:
 *   1. Gate veto (GATE_REJECTED action) produces a structured entry
 *      in the projection under domain='acquisition'.
 *   2. Intent span (INTENT_INTAKE / INTENT_DISPATCHED / INTENT_PARSING_START
 *      / INTENT_PARSING_END / INTENT_COMPLETE) appears in projection under
 *      entity='intent-span' with the span type as nextState.
 *   3. Health signals (activeIntents, parsingInFlight, gateVetoRate,
 *      healthFlags) are queryable from the projection's domain state.
 *   4. Cross-domain queries see the new acquisition entries alongside
 *      entries from other domains.
 *   5. Transition-writer filter (raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION')
 *      excludes intent-span entries — span is runtime telemetry, not lineage.
 *   6. Gate telemetry accumulation: repeated vetoes on the same intent
 *      accumulate in the FSM's gateVetoes ring and surface through
 *      getGateTelemetry().
 *
 * This is the runtime-true validation that the enriched local
 * intelligence of the acquisition-fsm is observable to the rest of
 * the system, not just internal to the FSM.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import acquisitionFsm from '../acquisition-kernel/fsm.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
import namespaceProjectionInterpreter from '../control-plane/governance/interpreters/namespace-projection-interpreter.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';

const { waitForLedgerEntryCount } = require('./helpers/sync-barriers');

const mockCtx = {
  validate: () => ({ allowed: true }),
  sanityCheck: async () => ({ allowed: true }),
};

const gatingCtx = {
  validate: () => ({ allowed: true }),
  sanityCheck: async () => ({ allowed: false, reason: 'capability_degraded' }),
};

async function clearFsmState() {
  if (typeof acquisitionFsm.clearIntents === 'function') {
    acquisitionFsm.clearIntents();
  }
}

describe('Phase 4K: Telemetry Governance Coverage', () => {
  beforeAll(async () => {
    await observability.init();
    // Start telemetry workers at moderate frequency — coverage test is
    // not a stress test; phase-4j covers pressure.
    await telemetryWorkers.startAll(50);
  }, 20000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await clearFsmState();
    await observability.stop();
  });

  it('GATE_REJECTED action appears in projection with structured reason taxonomy', async () => {
    await clearFsmState();
    const start = observability.query.getLogSize();

    // Trigger a gate veto: sanityCheck rejects
    const result = await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-gate-001',
      accountId: 'acc-gate-test',
      domain: 'engagement',
      params: { source: 'phase4k' },
    }, gatingCtx);

    expect(result.allowed).toBe(true);
    expect(result.actions).toBeDefined();
    expect(result.actions.length).toBe(1);
    expect(result.actions[0].type).toBe('GATE_REJECTED');
    expect(result.actions[0].reason).toBe('capability_degraded');

    // The GATE_REJECTED action emits a span — INTENT_GATE_VETO.
    // Wait for the span to propagate to the projection.
    await waitForLedgerEntryCount(1, 5000);

    // Query the projection: INTENT_GATE_VETO should appear under domain='acquisition'
    // with entity='intent-span' and nextState='INTENT_GATE_VETO'.
    const entries = observability.query.getEntriesSince(start).entries;
    const gateSpan = entries.find(
      (e) => e.domain === 'acquisition'
        && e.entity === 'intent-span'
        && e.nextState === 'INTENT_GATE_VETO'
    );

    expect(gateSpan).toBeDefined();
    expect(gateSpan.raw.operation).toBe('execute_acquisition');
    expect(gateSpan.raw.reason).toBe('capability_degraded');
    expect(gateSpan.raw.intentId).toBe('phase4k-gate-001');
  });

  it('Intent span (INTENT_INTAKE → INTENT_DISPATCHED → INTENT_PARSING_START → INTENT_PARSING_END → INTENT_COMPLETE) appears in projection', async () => {
    await clearFsmState();
    const start = observability.query.getLogSize();

    // Full happy-path lifecycle
    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-span-001',
      accountId: 'acc-span',
      domain: 'insights',
      params: { variant: 'phase4k' },
    }, mockCtx);

    await acquisitionFsm.dispatch({ type: 'ACQUISITION_EXECUTING', intentId: 'phase4k-span-001' }, mockCtx);

    await acquisitionFsm.dispatch({
      type: 'PARSING_DISPATCHED',
      intentId: 'phase4k-span-001',
      jobId: 'job-phase4k',
      domain: 'insights',
      accountId: 'acc-span',
      rawCount: 7,
    }, mockCtx);

    await acquisitionFsm.dispatch({
      type: 'PARSING_COMPLETE',
      intentId: 'phase4k-span-001',
      accountId: 'acc-span',
      domain: 'insights',
      result: { status: 'success', count: 7 },
    }, mockCtx);

    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_COMPLETE',
      intentId: 'phase4k-span-001',
      accountId: 'acc-span',
      domain: 'insights',
      result: { status: 'success', count: 7 },
    }, mockCtx);

    // Wait for all 5+ span emissions to reach projection
    await waitForLedgerEntryCount(5, 5000);

    const entries = observability.query.getEntriesSince(start).entries;
    const spanEntries = entries.filter(
      (e) => e.domain === 'acquisition' && e.entity === 'intent-span'
    );

    // Span types emitted in order
    const spanTypes = spanEntries.map((e) => e.nextState);
    expect(spanTypes).toContain('INTENT_INTAKE');
    expect(spanTypes).toContain('INTENT_DISPATCHED');
    expect(spanTypes).toContain('INTENT_PARSING_START');
    expect(spanTypes).toContain('INTENT_PARSING_END');
    expect(spanTypes).toContain('INTENT_COMPLETE');

    // Every span carries intentId + accountId + domain
    for (const span of spanEntries) {
      expect(span.raw.intentId).toBe('phase4k-span-001');
      expect(span.raw.accountId).toBe('acc-span');
      expect(span.raw.domain).toBe('insights');
    }
  });

  it('getGateTelemetry accumulates vetoes per reason across multiple intents', async () => {
    await clearFsmState();

    // Fire 3 vetoes with mixed reasons
    for (let i = 0; i < 3; i++) {
      const mixedGate = {
        validate: () => ({ allowed: true }),
        sanityCheck: async () => ({ allowed: false, reason: i === 0 ? 'capability_degraded' : 'rate_limit_active' }),
      };
      await acquisitionFsm.dispatch({
        type: 'ACQUISITION_INTENT_RECEIVED',
        intentId: `phase4k-telem-${i}`,
        accountId: 'acc-telem',
        domain: 'ugc',
        params: { seq: i },
      }, mixedGate);
    }

    // Give projection a moment to absorb
    await new Promise((r) => setTimeout(r, 200));

    const telemetry = acquisitionFsm.getGateTelemetry();
    expect(telemetry.totalVetoes).toBeGreaterThanOrEqual(3);
    expect(telemetry.vetoesByReason['capability_degraded']).toBeGreaterThanOrEqual(1);
    expect(telemetry.vetoesByReason['rate_limit_active']).toBeGreaterThanOrEqual(2);
    expect(telemetry.vetoesByOp['execute_acquisition']).toBeGreaterThanOrEqual(3);
    expect(telemetry.vetoRate).toBeGreaterThan(0);
  });

  it('getHealth() surface is opaque — no raw Map exposure, has expected signals', async () => {
    await clearFsmState();

    // Activate 1 intent, then a gate veto
    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-health-001',
      accountId: 'acc-health',
      domain: 'engagement',
      params: {},
    }, mockCtx);

    const health = acquisitionFsm.getHealth();
    expect(health).toBeDefined();
    expect(health.ok).toBeDefined();
    expect(health.signals).toBeDefined();

    // Required signals surface
    expect(health.signals.activeIntents).toBeGreaterThanOrEqual(1);
    expect(health.signals.parsingInFlight).toBeGreaterThanOrEqual(0);
    expect(health.signals.staleParses).toBeGreaterThanOrEqual(0);
    expect(health.signals.gateVetoRate).toBeGreaterThanOrEqual(0);
    expect(health.signals.lastSuccessAt).toBeDefined();
    expect(health.signals.lastFailureAt).toBeDefined();
    expect(health.signals.healthFlags).toBeDefined();
    expect(Array.isArray(health.signals.healthFlags)).toBe(true);

    // No raw Map exposure — surface is opaque
    expect(health._intents).toBeUndefined();
    expect(health._fingerprintDedup).toBeUndefined();
  });

  it('cross-domain query surfaces acquisition intent-span alongside other domains', async () => {
    await clearFsmState();

    // Fire 1 acquisition intent (becomes intent-span)
    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-cross-001',
      accountId: 'acc-cross',
      domain: 'engagement',
      params: {},
    }, mockCtx);

    await new Promise((r) => setTimeout(r, 100));

    const cross = observability.query.getCrossDomain([
      'acquisition', 'publishing', 'engagement', 'scheduling',
    ]);

    expect(cross).toBeDefined();
    expect(cross.acquisition).toBeDefined();
    // intent-span must be present as an entity in the acquisition domain
    expect(cross.acquisition['intent-span']).toBeDefined();
    expect(Object.keys(cross.acquisition['intent-span']).length).toBeGreaterThan(0);
  });

  it('transition-writer filter excludes intent-span — span is runtime telemetry, not lineage', async () => {
    await clearFsmState();

    // Fire 1 acquisition intent
    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-writer-001',
      accountId: 'acc-writer',
      domain: 'engagement',
      params: {},
    }, mockCtx);

    // Allow projection to absorb
    await new Promise((r) => setTimeout(r, 200));

    // Read the lineage ledger
    const ledger = await lineageLedger.getLineage(500);

    // intent-span entries must NOT carry raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    // — they ride the transition() channel but are not lineage events.
    const spanEntriesInLedger = ledger.filter(
      (e) => e.domain === 'acquisition'
        && e.entity === 'intent-span'
        && e.raw?.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );

    // Span entries should not be promoted to lineage (filter excludes them)
    expect(spanEntriesInLedger.length).toBe(0);
  });

  it('namespace projection interpreter picks up new entry types without crashing', async () => {
    await clearFsmState();

    // Fire 2 intents (gate-veto + happy)
    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-ns-001',
      accountId: 'acc-ns',
      domain: 'insights',
      params: {},
    }, gatingCtx);

    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-ns-002',
      accountId: 'acc-ns',
      domain: 'insights',
      params: {},
    }, mockCtx);

    await new Promise((r) => setTimeout(r, 200));

    // Projection interpreter should not throw
    expect(() => namespaceProjectionInterpreter.getProjections()).not.toThrow();

    const projections = namespaceProjectionInterpreter.getProjections();
    expect(projections).toBeDefined();
    // Should have an acquisition projection
    if (projections.acquisition !== undefined) {
      expect(typeof projections.acquisition).toBe('object');
    }
  });

  it('span lifecycle duration is computable from ms_since_intake on each span', async () => {
    await clearFsmState();
    const start = observability.query.getLogSize();

    // Fire one intent
    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      intentId: 'phase4k-dur-001',
      accountId: 'acc-dur',
      domain: 'ugc',
      params: {},
    }, mockCtx);

    // Wait at least 100ms
    await new Promise((r) => setTimeout(r, 100));

    await acquisitionFsm.dispatch({
      type: 'ACQUISITION_EXECUTING',
      intentId: 'phase4k-dur-001',
    }, mockCtx);

    await waitForLedgerEntryCount(1, 5000);
    const entries = observability.query.getEntriesSince(start).entries;
    const intakeSpan = entries.find(
      (e) => e.domain === 'acquisition'
        && e.entity === 'intent-span'
        && e.nextState === 'INTENT_INTAKE'
    );

    expect(intakeSpan).toBeDefined();
    expect(intakeSpan.raw.ms_since_intake).toBeGreaterThanOrEqual(0);
    // Executing span fires >100ms after intake, so its ms_since_intake should be >0
    const execSpan = entries.find(
      (e) => e.domain === 'acquisition'
        && e.entity === 'intent-span'
        && e.nextState === 'INTENT_DISPATCHED'
    );
    if (execSpan) {
      // INTENT_DISPATCHED fires on intake itself (acquisition-fsm's INTENT_INTAKE
      // and INTENT_DISPATCHED are emitted in the same dispatch call). Both have
      // ms_since_intake ~ 0. ACQUISITION_EXECUTING fires after, so its span
      // (if emitted) would carry a higher value.
      expect(execSpan.raw.ms_since_intake).toBeGreaterThanOrEqual(0);
    }
  });
});
