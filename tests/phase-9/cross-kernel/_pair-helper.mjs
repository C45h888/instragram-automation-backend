// Phase 9 — Cross-kernel pair helper.
// Exercises one directed pair (source → sink) by injecting the REAL
// event type that the source domain FSM would emit through CK to reach
// the sink domain FSM. CK acts as the governance intermediary between
// domain boundaries — this tests that the constitutional routing is
// intact for each canonical cross-kernel flow.
//
// CROSS_KERNEL_DISPATCH does not exist in the runtime. Cross-kernel
// communication uses specific event types in DOMAIN_EVENT_MAP or
// GLOBAL_TRANSITION_MAP that one FSM emits through ctx.dispatchGlobal()
// and CK routes to the target domain FSM.

import p9 from '../runtime/index.mjs';

// Canonical cross-kernel event map.
// Each source → sink pair maps to the REAL event type that the source
// FSM would emit through ctx.dispatchGlobal() to reach the sink.
// The payload provides the fields each FSM guard requires.
const CROSS_KERNEL_EVENT = {
  // insights → *: CK_INSIGHTS_FAILURE_OBSERVED is the global handler
  // (GLOBAL_TRANSITION_MAP line 1142) that acquisition FSM emits when
  // a worker poll fails. CK dispatches CAPABILITY_CHECK or ACQUISITION_DEFER.
  'insights→acquisition': { type: 'CK_INSIGHTS_FAILURE_OBSERVED',
    payload: { accountId: 'test-acc-001', intentId: 'ins-to-acq', domain: 'acquisition', error: 'test' }},
  'insights→capability':  { type: 'CK_INSIGHTS_FAILURE_OBSERVED',
    payload: { accountId: 'test-acc-001', intentId: 'ins-to-cap', domain: 'capability', error: 'test' }},
  'insights→recovery':    { type: 'CK_INSIGHTS_FAILURE_OBSERVED',
    payload: { accountId: 'test-acc-001', intentId: 'ins-to-rec', domain: 'recovery', error: 'test' }},
  'insights→publishing':  { type: 'DB_WRITE_REQUESTED',
    payload: { domain: 'publishing', accountId: 'test-acc-001', table: 'test', operation: 'insert', rows: [] }},

  // acquisition → *: CK handles these through GLOBAL_TRANSITION_MAP.
  // CAPABILITY_CHECK routes to graph-capability FSM.
  // ACQUISITION_DEFER routes to acquisition FSM (stays in domain).
  'acquisition→capability': { type: 'CAPABILITY_CHECK',
    payload: { businessAccountId: 'test-acc-001' }},
  'acquisition→publishing': { type: 'DB_WRITE_REQUESTED',
    payload: { domain: 'publishing', accountId: 'test-acc-001', table: 'test', operation: 'insert', rows: [] }},
  'acquisition→recovery':   { type: 'ACQUISITION_DEFER',
    payload: { accountId: 'test-acc-001', domain: 'acquisition', reason: 'test_defer' }},
  'acquisition→insights':   { type: 'ACQUISITION_INTENT_RECEIVED',
    payload: { accountId: 'test-acc-001', intentId: 'acq-to-ins', domain: 'acquisition', eventType: 'test' }},

  // publishing → *: publishing FSM events routed through CK.
  'publishing→acquisition': { type: 'PUBLISHING_DATA_AVAILABLE',
    payload: { accountId: 'test-acc-001' }},
  'publishing→capability':  { type: 'DB_WRITE_REQUESTED',
    payload: { domain: 'capability', accountId: 'test-acc-001', table: 'test', operation: 'insert', rows: [] }},
  'publishing→recovery':    { type: 'PUBLISH_FAILURE',
    payload: { accountId: 'test-acc-001', intentId: 'pub-to-rec', error: 'test' }},
  'publishing→insights':    { type: 'PUBLISHING_DATA_AVAILABLE',
    payload: { accountId: 'test-acc-001' }},

  // capability → *: graph-capability FSM events routed through CK.
  'capability→acquisition': { type: 'CAPABILITY_FAILED',
    payload: { businessAccountId: 'test-acc-001' }},
  'capability→recovery':    { type: 'CAPABILITY_FAILED',
    payload: { businessAccountId: 'test-acc-001' }},
  'capability→insights':    { type: 'CAPABILITY_EVALUATE',
    payload: { businessAccountId: 'test-acc-001' }},

  // recovery → *: engagement FSM events routed through CK.
  'recovery→acquisition':   { type: 'CIRCUIT_BREAKER_CLEARED',
    payload: { accountId: 'test-acc-001' }},
  'recovery→publishing':    { type: 'CIRCUIT_BREAKER_CLEARED',
    payload: { accountId: 'test-acc-001' }},
  'recovery→capability':    { type: 'RETRY_REQUESTED',
    payload: { accountId: 'test-acc-001', domain: 'test' }},
  'recovery→insights':      { type: 'CIRCUIT_BREAKER_CLEARED',
    payload: { accountId: 'test-acc-001' }},
};

export async function runPair({ source, sink, suiteName = 'cross-kernel' }) {
  const harness = new p9.RuntimeHarness({ runId: `p9-pair-${source}-to-${sink}` });
  await harness.boot();
  const writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });

  const key = `${source}→${sink}`;
  const mapping = CROSS_KERNEL_EVENT[key];
  if (!mapping) {
    const result = { source, sink, event: null, ownership: null,
      findings: [{ kind: 'cross-kernel-unrouted', reason: `no canonical event mapping for ${key}` }] };
    writer.addExtra('pair', result);
    writer.writeSummary(suiteName, `${source}-to-${sink}`);
    await harness.shutdown();
    return result;
  }

  const correlationId = `p9-${source}-to-${sink}-${Date.now()}`;
  harness.injectEvent({
    type: mapping.type,
    source,
    payload: mapping.payload,
    correlationId,
  });
  await harness.tick(3);

  const snap = harness.snapshotDeriver.derive();
  const event = snap.events[correlationId];

  const findings = [];
  if (!event) {
    findings.push({ kind: 'cross-kernel-contamination', reason: 'event not observed' });
  }
  const ownership = harness.ownershipTracer.snapshot()[correlationId];
  if (!ownership) {
    findings.push({ kind: 'cross-kernel-contamination', reason: 'no ownership record' });
  }

  const result = { source, sink, event, ownership, findings };
  writer.addExtra('pair', result);
  writer.writeSummary(suiteName, `${source}-to-${sink}`);
  await harness.shutdown();
  return result;
}
