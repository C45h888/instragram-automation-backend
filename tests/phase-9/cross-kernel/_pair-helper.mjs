// Phase 9 — Cross-kernel pair helper.
// Runs one directed pair (source → sink). Each pair is exercised
// by injecting a real CK-routable domain event (not CROSS_KERNEL_DISPATCH,
// which is not in CK's DOMAIN_EVENT_MAP). The runtime's CK routes it
// through the constitutional domain map; the sink's observation list is
// inspected.
//
// FIX (2026-06-14): CROSS_KERNEL_DISPATCH was not in CK's DOMAIN_EVENT_MAP,
// so CK's dispatch() routed it to the global handler path instead of the
// domain path. All pairs now inject the actual domain event type that CK
// recognizes, so the routing is tested for real.

import p9 from '../runtime/index.mjs';
import { writeFileSync } from 'node:fs';
import path from 'node:path';

/**
 * Map of directed pair → { eventType, payload }.
 * The event type must be in CK's DOMAIN_EVENT_MAP so CK routes it
 * constitutionally rather than falling through to the global handler.
 * See control-plane/governance/constitutional-kernel.js §DOMAIN_EVENT_MAP.
 */
const PAIR_EVENT_MAP = {
  'acquisition:publishing':   { eventType: 'PUBLISHING_DATA_AVAILABLE',    sinkDomain: 'publishing' },
  'acquisition:recovery':     { eventType: 'RECONCILIATION_TICK',           sinkDomain: 'reconciliation' },
  'acquisition:insights':     { eventType: 'INSIGHTS_POLL_FAILURE',        sinkDomain: 'acquisition' },
  'acquisition:capability':   { eventType: 'CAPABILITY_CHECK',              sinkDomain: 'graph-capability' },
  'publishing:recovery':      { eventType: 'RECONCILIATION_TICK',           sinkDomain: 'reconciliation' },
  'publishing:insights':      { eventType: 'READ_RESULT_AVAILABLE',         sinkDomain: 'graph-capability' },
  'publishing:capability':    { eventType: 'READ_RESULT_AVAILABLE',         sinkDomain: 'graph-capability' },
  'publishing:acquisition':   { eventType: 'ACQUISITION_DEFER',            sinkDomain: 'acquisition' },
  'recovery:acquisition':      { eventType: 'ACQUISITION_INTENT_RECEIVED',  sinkDomain: 'acquisition' },
  'recovery:insights':         { eventType: 'INSIGHTS_POLL_FAILURE',         sinkDomain: 'acquisition' },
  'recovery:capability':       { eventType: 'CAPABILITY_CHECK',             sinkDomain: 'graph-capability' },
  'recovery:publishing':       { eventType: 'PUBLISHING_DATA_AVAILABLE',     sinkDomain: 'publishing' },
  'insights:publishing':      { eventType: 'PUBLISHING_DATA_AVAILABLE',     sinkDomain: 'publishing' },
  'insights:recovery':         { eventType: 'RECONCILIATION_TICK',          sinkDomain: 'reconciliation' },
  'insights:capability':      { eventType: 'CAPABILITY_CHECK',             sinkDomain: 'graph-capability' },
  'insights:acquisition':      { eventType: 'ACQUISITION_DEFER',            sinkDomain: 'acquisition' },
  'capability:insights':       { eventType: 'INSIGHTS_POLL_FAILURE',        sinkDomain: 'acquisition' },
  'capability:recovery':       { eventType: 'RECONCILIATION_TICK',          sinkDomain: 'reconciliation' },
  'capability:acquisition':    { eventType: 'ACQUISITION_INTENT_RECEIVED',  sinkDomain: 'acquisition' },
  'capability:publishing':     { eventType: 'PUBLISHING_DATA_AVAILABLE',    sinkDomain: 'publishing' },
};

export async function runPair({ source, sink, suiteName = 'cross-kernel' }) {
  const harness = new p9.RuntimeHarness({ runId: `p9-pair-${source}-to-${sink}` });
  await harness.boot();
  const writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });

  // Reset drift detector so findings from prior tests don't pollute this run
  harness.driftDetector.reset();

  const correlationId = `p9-${source}-to-${sink}-${Date.now()}`;

  // Look up the real CK-routable event for this pair
  const pairKey = `${source}:${sink}`;
  const mapping = PAIR_EVENT_MAP[pairKey];
  if (!mapping) {
    const err = new Error(`_pair-helper: no event mapping for pair '${pairKey}' — add it to PAIR_EVENT_MAP`);
    await harness.shutdown();
    throw err;
  }

  // Reset governance log cursor so we can assert on decisions made during this test
  const decisionsBefore = harness.simulator.governanceLog().length;

  // Inject the real domain-routed event through the observability plane
  harness.injectEvent({
    type: mapping.eventType,
    source: `${source}-kernel`,
    payload: {
      kind: 'cross-kernel',
      from: source,
      to: sink,
      sinkDomain: mapping.sinkDomain,
    },
    correlationId,
  });
  await harness.tick(3);

  // ── Assertions ─────────────────────────────────────────────────────────────

  const findings = [];

  // 1. Event was observed in the constitutional path
  const snap = harness.snapshotDeriver.derive();
  const event = snap.events[correlationId];
  if (!event) {
    findings.push({ kind: 'cross-kernel-contamination', reason: 'event not observed in constitutional path' });
  } else {
    // 2. No drift detected for this event
    const drift = harness.driftDetector.snapshot();
    if (drift.length > 0) {
      findings.push({ kind: 'cross-kernel-contamination', reason: `drift detected: ${JSON.stringify(drift)}` });
    }

    // 3. Event touched the sink domain (constitutional routing verified)
    const touchedKernels = event.kernels_touched || [];
    if (!touchedKernels.includes(mapping.sinkDomain)) {
      findings.push({
        kind: 'cross-kernel-contamination',
        reason: `sink domain '${mapping.sinkDomain}' not touched by event — kernels_touched: ${JSON.stringify(touchedKernels)}`,
      });
    }
  }

  // 4. Ownership record exists
  const ownership = harness.ownershipTracer.snapshot()[correlationId];
  if (!ownership) {
    findings.push({ kind: 'cross-kernel-contamination', reason: 'no ownership record for correlationId' });
  }

  // 5. No unexpected governance rejections for this event
  const decisionsAfter = harness.simulator.governanceLog().length;
  const newDecisions = harness.simulator.governanceLog().slice(decisionsBefore);
  const rejections = newDecisions.filter(
    (d) => d.result === false || d.result?.allowed === false
  );
  if (rejections.length > 0) {
    findings.push({ kind: 'cross-kernel-contamination', reason: `governance rejected: ${JSON.stringify(rejections)}` });
  }

  const result = { source, sink, event, ownership, findings, correlationId, eventType: mapping.eventType };
  writer.addExtra('pair', result);
  writer.writeSummary(suiteName, `${source}-to-${sink}`);
  await harness.shutdown();
  return result;
}
