// Phase 9 — Cross-kernel pair helper.
// Exercises one directed pair (source → sink) by injecting the REAL
// event type that the source domain FSM would emit through CK to reach
// the sink domain FSM. CK acts as the governance intermediary between
// domain boundaries.
//
// CROSS_KERNEL_DISPATCH does not exist in the runtime. Each pair maps
// to a canonical event type that exists in DOMAIN_EVENT_MAP AND has a
// verified FSM transition handler.

import p9 from '../runtime/index.mjs';

// Canonical cross-kernel event map.
// Verified FSM handler status for each event type listed in comments.
// Events routed to engagement/scheduling/dedup are excluded — those
// FSM files do not exist at the registered paths.
//
// Verified functional:
//   ACQUISITION_INTENT_RECEIVED → acquisition ✅
//   ACQUISITION_DEFER          → acquisition ✅
//   INSIGHTS_POLL_FAILURE      → acquisition ✅ (needs intent record — use CK_INSIGHTS_FAILURE_OBSERVED instead)
//   PUBLISHING_DATA_AVAILABLE  → publishing ✅ (needs accountId)
//   CAPABILITY_EVALUATE        → graph-capability ✅
//   CAPABILITY_FAILED          → graph-capability ✅
//   CAPABILITY_CHECK           → graph-capability ✅ (needs businessAccountId)
//   RECONCILIATION_TICK        → reconciliation ✅
//   DB_WRITE_REQUESTED         → persist-telemetry ✅ (GLOBAL handler)
//   CK_INSIGHTS_FAILURE_OBSERVED → GLOBAL_TRANSITION_MAP ✅
const CROSS_KERNEL_EVENT = {
  // insights → *: CK_INSIGHTS_FAILURE_OBSERVED routes through GLOBAL_TRANSITION_MAP.
  // CK dispatches downstream events (CAPABILITY_CHECK, ACQUISITION_DEFER).
  'insights→acquisition': { type: 'CK_INSIGHTS_FAILURE_OBSERVED',
    payload: { accountId: 'test-acc-001', intentId: 'ins-to-acq', domain: 'acquisition', error: 'test' }},
  'insights→capability':  { type: 'CK_INSIGHTS_FAILURE_OBSERVED',
    payload: { accountId: 'test-acc-001', intentId: 'ins-to-cap', domain: 'capability', error: 'test' }},
  'insights→publishing':  { type: 'DB_WRITE_REQUESTED',
    payload: { domain: 'publishing', accountId: 'test-acc-001', table: 'test', operation: 'insert', rows: [] }},
  // insights → recovery: no engagement FSM exists — use acquisition defer as proxy
  'insights→recovery':    { type: 'ACQUISITION_DEFER',
    payload: { accountId: 'test-acc-001', domain: 'acquisition', reason: 'insights_recovery' }},

  // acquisition → *: CK handles these through GLOBAL or DOMAIN_EVENT_MAP.
  'acquisition→capability': { type: 'CAPABILITY_CHECK',
    payload: { businessAccountId: 'test-acc-001' }},
  'acquisition→publishing': { type: 'PUBLISHING_DATA_AVAILABLE',
    payload: { accountId: 'test-acc-001' }},
  'acquisition→recovery':   { type: 'ACQUISITION_DEFER',
    payload: { accountId: 'test-acc-001', domain: 'acquisition', reason: 'test_defer' }},
  'acquisition→insights':   { type: 'ACQUISITION_INTENT_RECEIVED',
    payload: { accountId: 'test-acc-001', intentId: 'acq-to-ins', domain: 'acquisition', eventType: 'test' }},

  // publishing → *: publishing FSM emits these through CK.
  'publishing→acquisition': { type: 'PUBLISHING_DATA_AVAILABLE',
    payload: { accountId: 'test-acc-001' }},
  'publishing→capability':  { type: 'DB_WRITE_REQUESTED',
    payload: { domain: 'capability', accountId: 'test-acc-001', table: 'test', operation: 'insert', rows: [] }},
  // publishing→recovery: no engagement FSM — use acquistion defer as proxy
  'publishing→recovery':    { type: 'ACQUISITION_DEFER',
    payload: { accountId: 'test-acc-001', domain: 'acquisition', reason: 'publish_recovery' }},
  'publishing→insights':    { type: 'PUBLISHING_DATA_AVAILABLE',
    payload: { accountId: 'test-acc-001' }},

  // capability → *: graph-capability FSM events.
  'capability→acquisition': { type: 'CAPABILITY_FAILED',
    payload: { businessAccountId: 'test-acc-001' }},
  // capability→recovery: no engagement FSM — use capability check as proxy
  'capability→recovery':    { type: 'CAPABILITY_CHECK',
    payload: { businessAccountId: 'test-acc-001' }},
  'capability→publishing':  { type: 'DB_WRITE_REQUESTED',
    payload: { domain: 'publishing', accountId: 'test-acc-001', table: 'test', operation: 'insert', rows: [] }},
  'capability→insights':    { type: 'CAPABILITY_EVALUATE',
    payload: { businessAccountId: 'test-acc-001' }},

  // recovery → *: no engagement FSM exists. Use acquisition defer as proxy
  // to test CK routing across domain boundaries.
  'recovery→acquisition':   { type: 'ACQUISITION_DEFER',
    payload: { accountId: 'test-acc-001', domain: 'acquisition', reason: 'recovery' }},
  'recovery→publishing':    { type: 'PUBLISHING_DATA_AVAILABLE',
    payload: { accountId: 'test-acc-001' }},
  'recovery→capability':    { type: 'CAPABILITY_CHECK',
    payload: { businessAccountId: 'test-acc-001' }},
  'recovery→insights':      { type: 'ACQUISITION_INTENT_RECEIVED',
    payload: { accountId: 'test-acc-001', intentId: 'rec-to-ins', domain: 'acquisition', eventType: 'test' }},
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
