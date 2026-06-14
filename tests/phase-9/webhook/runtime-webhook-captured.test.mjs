// Phase 9 — Tier 1 Webhook (captured fixtures).
// Slot reserved for phase 10 (VPS / RunPod). When captured/ is
// non-empty, this test runs against real Meta payloads.
//
// Until then: it.skip with a phase-10 TODO.

import { describe, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CAPTURED_DIR = path.join(__dirname, '..', 'fixtures', 'webhooks', 'captured');

describe('webhook/runtime-webhook-captured — Tier 1 (deferred to phase 10)', () => {
  it.skip('runs against captured Meta payloads once captured/ is populated (phase 10)', () => {
    // TODO(phase-10): replace this skip with a real run that
    // discovers every .json in captured/, delivers each through
    // the real runtime, and asserts the same 4-point contract
    // (observed event_id, worker_count > 0, mutation_count > 0,
    // no drift). Captured payloads are the highest-fidelity
    // validation we have.
  });

  it('captured/ directory exists and is ready to receive payloads', () => {
    const exists = fs.existsSync(CAPTURED_DIR);
    const readme = path.join(CAPTURED_DIR, 'README.md');
    const hasReadme = fs.existsSync(readme);
    // These are setup invariants, not test outcomes.
    if (!exists) throw new Error('captured/ directory missing');
    if (!hasReadme) throw new Error('captured/README.md missing');
  });
});
