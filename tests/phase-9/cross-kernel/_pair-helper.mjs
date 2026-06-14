// Phase 9 — Cross-kernel pair helper.
// Runs one directed pair (source → sink). Each pair is exercised
// by injecting a cross-kernel event; the runtime's CK routes it
// through the cross-kernel bridge; the sink's observation list is
// inspected. The recorder-observer captures what the runtime did.

import p9 from '../runtime/index.mjs';
import { writeFileSync } from 'node:fs';
import path from 'node:path';

export async function runPair({ source, sink, suiteName = 'cross-kernel' }) {
  const harness = new p9.RuntimeHarness({ runId: `p9-pair-${source}-to-${sink}` });
  await harness.boot();
  const writer = new p9.ReportWriter({ reportDir: harness.reportDir, runId: harness.runId });

  const correlationId = `p9-${source}-to-${sink}-${Date.now()}`;
  const publicSignal = { kind: 'cross-kernel', from: source, to: sink };
  const internalState = { tokens: 'should-not-leave-source' };

  harness.injectEvent({
    type: 'CROSS_KERNEL_DISPATCH',
    source,
    payload: { public_signal: publicSignal, internal_state: internalState, sink },
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
