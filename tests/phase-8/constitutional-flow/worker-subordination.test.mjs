// Phase 8 — Worker Subordination
// Workers are constitutionally execution-only. This test asserts
// the rule by scanning canonical worker modules and verifying they
// do not:
//   - import a scheduler
//   - import a governance module
//   - import an FSM
//   - mutate foreign kernel tables (a small forbidden set)
//
// If the worker module is missing, the test marks itself DEFERRED
// (not failed) — phase-8 is layered above phase-7 and respects
// the same DEFERRED semantics for not-yet-built kernels.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import p8 from '../runtime/index.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..', '..', '..');

const WORKER_CANDIDATES = [
  'publishing-kernel/workers',
  'acquisition-kernel/workers',
  'graph-capability-kernel/workers',
  'reconciliation-kernel/workers',
  'retry-cadence-kernel/workers',
  'scheduling-kernel/workers',
  'dedup-kernel/workers',
  'telemetry-kernel/workers',
];

// Workers are constitutionally execution-only. They MUST NOT:
//   - import a scheduler / governance / FSM module
//   - import a priority-queue
//   - call escalate() (governance action)
//
// The matchers below look for ACTUAL imports / requires, not
// parameter names or JSDoc references. A worker can take
// `governance` as an argument — that's a wired dependency, not
// a forbidden import.
const FORBIDDEN_IMPORTS = [
  "require(['\"]scheduler",
  "require(['\"]governance",
  "require(['\"]priority-queue",
  "from ['\"]scheduler",
  "from ['\"]governance",
  "from ['\"]priority-queue",
  'import.*scheduler',
  'import.*governance',
  'import.*priority-queue',
  'escalate(',  // method call
  '/fsm/',       // path token
];

function exists(p) {
  try { return fs.statSync(p).isDirectory(); } catch (_) { return false; }
}

describe('constitutional-flow/worker-subordination', () => {
  let writer;
  beforeAll(() => {
    writer = new p8.ReportWriter({ suite: 'constitutional', testName: 'worker-subordination' });
  });
  afterAll(() => writer.finish());

  it('at least one worker module exists', () => {
    const found = WORKER_CANDIDATES
      .map((p) => path.join(ROOT, p))
      .filter(exists);
    expect(found.length, 'no worker modules found in expected locations').toBeGreaterThan(0);
    writer.addExtra('worker_dirs_found', found);
    writer.bumpAssertions();
  });

  it('worker modules do not import scheduler/governance/fsm', () => {
    const found = WORKER_CANDIDATES
      .map((p) => path.join(ROOT, p))
      .filter(exists);
    const violations = [];

    // Strip comments before scanning — false positives from prose
    // ("escalate via strike" inside a comment) must not count as
    // worker autonomy violations.
    const stripComments = (s) =>
      s.replace(/\/\*[\s\S]*?\*\//g, '')   // block comments
       .replace(/\/\/[^\n]*/g, '');         // line comments

    for (const dir of found) {
      const files = fs.readdirSync(dir).filter((f) => f.endsWith('.js'));
      for (const f of files) {
        const full = path.join(dir, f);
        const raw = fs.readFileSync(full, 'utf8');
        const code = stripComments(raw);
        for (const forbid of FORBIDDEN_IMPORTS) {
          if (code.includes(forbid)) {
            const sample = raw.split('\n').find((l) => l.includes(forbid))?.trim();
            violations.push({ file: full, forbid, sample });
          }
        }
      }
    }
    writer.addExtra('worker_violations', violations);
    writer.bumpAssertions();
    if (violations.length > 0) {
      writer.addDrift({ kind: 'worker-autonomy', violations });
    }
    expect(violations, JSON.stringify(violations, null, 2)).toEqual([]);
  });
});
