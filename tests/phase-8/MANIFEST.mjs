// Phase 8 — Test Manifest
// Single source of truth for which test files the runner executes.
// ESM module to match the vitest 4.x ESM-first surface.

import path from 'node:url';

const __dirname = path.fileURLToPath(new URL('.', import.meta.url));

const SUITES = {
  constitutional: [
    'constitutional-flow/webhook-to-state.test.js',
    'constitutional-flow/graph-to-state.test.js',
    'constitutional-flow/worker-subordination.test.js',
    'constitutional-flow/retry-cadence-survival.test.js',
  ],
  webhook: [
    'webhook/ingress-parser.test.js',
    'webhook/governance-decision.test.js',
    'webhook/no-unrelated-mutation.test.js',
    'webhook/webhook-chaos.test.js',
  ],
  'cross-kernel': [
    'cross-kernel/capability-to-acquisition.test.js',
    'cross-kernel/capability-to-publishing.test.js',
    'cross-kernel/capability-to-recovery.test.js',
    'cross-kernel/capability-to-insights.test.js',
    'cross-kernel/acquisition-to-capability.test.js',
    'cross-kernel/acquisition-to-publishing.test.js',
    'cross-kernel/acquisition-to-recovery.test.js',
    'cross-kernel/acquisition-to-insights.test.js',
    'cross-kernel/publishing-to-capability.test.js',
    'cross-kernel/publishing-to-acquisition.test.js',
    'cross-kernel/publishing-to-recovery.test.js',
    'cross-kernel/publishing-to-insights.test.js',
    'cross-kernel/recovery-to-capability.test.js',
    'cross-kernel/recovery-to-acquisition.test.js',
    'cross-kernel/recovery-to-publishing.test.js',
    'cross-kernel/recovery-to-insights.test.js',
    'cross-kernel/insights-to-capability.test.js',
    'cross-kernel/insights-to-acquisition.test.js',
    'cross-kernel/insights-to-publishing.test.js',
    'cross-kernel/insights-to-recovery.test.js',
  ],
  integration: [
    'integration/phase-8-full-composition.test.js',
    'integration/phase-8-multi-tick-survival.test.js',
    'integration/phase-8-architectural-drift.test.js',
  ],
};

function joinHere(p) {
  // Resolve relative to this file's directory using import.meta.url.
  return new URL(p, import.meta.url).pathname;
}

export function allSuites() {
  return Object.entries(SUITES).flatMap(([name, files]) =>
    files.map((f) => ({ suite: name, file: joinHere(f) })));
}

export function suite(suiteName) {
  return (SUITES[suiteName] || []).map((f) => ({ suite: suiteName, file: joinHere(f) }));
}
