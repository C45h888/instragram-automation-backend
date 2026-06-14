// Phase 9 — Sovereignty negative test.
// Asserts the sovereignty detector catches a deliberate coupling.
// If this test PASSES on a system with no coupling, the detector
// works. If it FAILS, the coupling detector is broken.

import { describe, it, expect } from 'vitest';
import { DriftDetector } from '../runtime/drift-detector.mjs';

describe('sovereignty/kernel-sovereignty-acquisition-coupling', () => {
  it('drift detector finds a deliberate cross-kernel violation', () => {
    const detector = new DriftDetector();
    // Simulate a worker emitting a GOVERN event (governance leakage).
    const fakeSim = {
      timeline: () => [
        {
          id: 1,
          type: 'WORKER_GOVERN_DECISION',
          source: 'acquisition-worker',
          correlationId: 'test-1',
          timestamp: Date.now(),
        },
      ],
      mutations: () => [],
    };
    detector.attach(fakeSim);
    const findings = detector.snapshot();
    expect(findings.length, 'governance-leakage not detected').toBeGreaterThan(0);
    expect(findings[0].kind).toBe('governance-leakage');
  });
});
