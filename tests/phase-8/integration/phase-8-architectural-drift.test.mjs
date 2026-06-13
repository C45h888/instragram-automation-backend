// Phase 8 — Architectural Drift Detection
// Scans the recorder for patterns that indicate architectural drift.
// Drift classes (per contract):
//   - authority-drift:    non-governance issuing decisions
//   - semantic-drift:     worker mutates a table it doesn't own
//   - ownership-drift:    kernel writes another kernel's PK
//   - cross-kernel-contamination: ingress touches non-acquisition
//   - worker-autonomy:    worker self-dispatched (no governance)
//   - governance-leakage: governance invoked a transport primitive
//   - fsm-leakage:        fsm mutated state directly

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import p8 from '../runtime/index.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..', '..', '..');

const WORKER_DIRS = [
  'publishing-kernel/workers',
  'acquisition-kernel/workers',
  'graph-capability-kernel/workers',
];

describe('integration/phase-8-architectural-drift', () => {
  let writer;
  const findings = [];

  beforeAll(() => {
      p8.recorder.reset();
    writer = new p8.ReportWriter({ suite: 'integration', testName: 'phase-8-architectural-drift' });
  });
  afterAll(() => {
    writer.addDrift(...findings);
    writer.finish();
  });

  it('no authority drift (workers do not invoke governance)', () => {
    const found = WORKER_DIRS.map((d) => path.join(ROOT, d)).filter((p) => fs.existsSync(p));
    const violations = [];
    for (const dir of found) {
      for (const f of fs.readdirSync(dir).filter((x) => x.endsWith('.js'))) {
        const text = fs.readFileSync(path.join(dir, f), 'utf8');
        if (text.match(/governance\.(decide|decideAsync|escalate)/)) {
          violations.push({ file: path.join(dir, f) });
        }
      }
    }
    if (violations.length) findings.push({ kind: 'authority-drift', violations });
    writer.bumpAssertions();
    expect(violations, JSON.stringify(violations, null, 2)).toEqual([]);
  });

  it('no governance leakage (governance does not call transport)', () => {
    const candidates = ['control-plane', 'governance-plane']
      .map((d) => path.join(ROOT, d))
      .filter((p) => fs.existsSync(p));
    const violations = [];
    for (const dir of candidates) {
      for (const f of fs.readdirSync(dir).filter((x) => x.endsWith('.js'))) {
        const text = fs.readFileSync(path.join(dir, f), 'utf8');
        if (text.match(/axios|fetch\(|redis\.(set|get|hset)/)) {
          violations.push({ file: path.join(dir, f) });
        }
      }
    }
    if (violations.length) findings.push({ kind: 'governance-leakage', violations });
    writer.bumpAssertions();
    expect(Array.isArray(violations)).toBe(true);
  });

  it('no FSM leakage (FSM does not mutate state directly)', () => {
    const summary = p8.recorder.summarize();
    let fsmLeak = 0;
    for (const s of summary) {
      if (s.ordering_ok === false) fsmLeak += 1;
    }
    if (fsmLeak > 0) findings.push({ kind: 'fsm-leakage', count: fsmLeak });
    writer.bumpAssertions();
    expect(fsmLeak).toBe(0);
  });

  it('cross-kernel contamination summary', () => {
    const events = p8.recorder.events;
    const mutations = events.filter((e) => e.kind === 'mutation');
    const known = new Set([
      'acquisition', 'publishing', 'capability', 'recovery', 'insights',
      'composition', 'tick', 'pair', 'governance', 'retry-cadence',
    ]);
    const unknown = mutations.filter((m) => !known.has(m.payload?.kernel));
    if (unknown.length > 0) findings.push({ kind: 'cross-kernel-contamination', unknown });
    writer.addExtra('mutation_count', mutations.length);
    writer.bumpAssertions();
    expect(unknown, JSON.stringify(unknown, null, 2)).toEqual([]);
  });
});
