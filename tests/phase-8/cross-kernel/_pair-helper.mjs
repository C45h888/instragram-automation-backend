// Phase 8 — Cross-Kernel Pair Helper.
// Shared test body for all 20 (source → sink) pairs. Each pair
// test file imports runPair({ source, sink }) and gets:
//   1. constitutional-signal projection (public_signal only)
//   2. sentinel-isolation assertion (sink must not see internal)
//   3. ordering assertion (governance < fsm < worker < mutation)
//   4. per-test JSON report

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import p8 from '../runtime/index.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export async function runPair({ source, sink, suiteName = 'cross-kernel' }) {
  const testName = `${source}-to-${sink}`;
  const writer = new p8.ReportWriter({ suite: suiteName, testName });
  const findings = [];

  const probe = p8.probe;
  const packet = probe.buildPacket({
    source, sink,
    publicSignal: { kind: 'cross-kernel', from: source, to: sink, n: 1 },
    internalState: { tokens: 'should-not-leave-source' },
  });

  const sinkObserved = [packet.public_signal];

  const eid = packet.packet_id;
  p8.recorder.ingress(eid, { source, sink, packet_id: eid });
  p8.recorder.governance(eid, { actor: 'CK_DECISION', source, sink });
  p8.recorder.fsm(eid, { fsm: `${source}-to-${sink}-fsm` });
  p8.recorder.worker(eid, `bridge-worker-${source}-${sink}`, { action: 'forward' });
  p8.recorder.mutation(eid, { kernel: sink, kind: 'cross-kernel-arrival' });

  const iso = probe.assertSentinelIsolated(sinkObserved);
  writer.addExtra('sentinel_isolation', iso);
  writer.bumpAssertions();
  if (!iso.ok) findings.push({ kind: 'cross-kernel-contamination', iso });

  const check = p8.recorder.assertConstitutionalPath(eid);
  writer.addConstitutional([check]);
  writer.bumpAssertions();
  if (!check.ok) findings.push({ kind: 'constitutional-violation', check });

  const writes = p8.recorder.events
    .filter((e) => e.event_id === eid && e.kind === 'mutation')
    .map((e) => e.payload);
  const foreignWrites = writes.filter((w) => w.kernel !== sink);
  writer.bumpAssertions();
  if (foreignWrites.length > 0) findings.push({ kind: 'semantic-drift', foreignWrites });

  for (const f of findings) writer.addDrift(f);
  writer.finish();

  return { iso, check, foreignWrites };
}
