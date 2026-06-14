// Phase 9 — Sovereignty negative test.
//
// Tests two things:
//   (a) The drift detector catches governance leakage in the real runtime
//       when an event with a worker source contains GOVERN semantics.
//   (b) A synthetic-fakeSim test validates the detector's pattern-matching
//       logic independently (the fake simulator path proves the detection
//       algorithm works; the real harness proves the runtime would surface it).
//
// The test passes only if the detector fires on a real event injection.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p9 from '../runtime/index.mjs';

describe('sovereignty/kernel-sovereignty-acquisition-coupling', () => {
  let harness;

  beforeAll(async () => {
    harness = new p9.RuntimeHarness({ runId: 'p9-sov-coupling' });
    await harness.boot();
  }, 60000);

  afterAll(async () => {
    if (harness) await harness.shutdown();
  }, 30000);

  /**
   * Test (a): real runtime injection.
   *
   * We inject an event that mimics a worker emitting a governance decision.
   * The event type CONTAINS 'WORKER' and 'GOVERN' so the drift detector's
   * pattern fires. In production, this would correspond to a worker that
   * bypasses CK and makes its own validation call.
   *
   * Note: WORKER_GOVERN_DECISION is not in CK's INTERNAL_DOMAIN_EVENTS,
   * so CK will route it as a global event. The drift detector's governance-
   * leakage check fires on the event pattern regardless of whether CK
   * accepted or rejected it — the pattern alone is the violation.
   */
  it('drift detector catches governance leakage from a real event injection', async () => {
    // Reset findings from boot so we only count events from this test
    harness.driftDetector.reset();

    const correlationId = `p9-coupling-${Date.now()}`;

    // Inject an event with worker source + GOVERN semantics
    // (this is the constitutional violation: a worker acting as CK)
    harness.injectEvent({
      type: 'WORKER_GOVERN_DECISION',
      source: 'acquisition-worker',  // worker source = violation
      payload: { accountId: 'test-account', decision: 'ACCEPT' },
      correlationId,
    });
    await harness.tick(2);

    // Drain the drift detector's continuous scan
    const findings = harness.driftDetector.snapshot();
    expect(findings.length, `drift detector did not fire — expected governance-leakage finding, got: ${JSON.stringify(findings)}`).toBeGreaterThan(0);
    expect(findings[0].kind, `expected governance-leakage, got: ${findings[0].kind}`).toBe('governance-leakage');
    expect(findings[0].source, `expected source 'acquisition-worker', got: ${findings[0].source}`).toBe('acquisition-worker');
  });

  /**
   * Test (b): synthetic fake-simulator path — validates the detection
   * algorithm itself, independent of the runtime. The fake simulator
   * proves the pattern-matching logic is correct even without a live system.
   */
  it('drift detector catches governance leakage in synthetic fake-sim mode', () => {
    const { DriftDetector } = require('../runtime/drift-detector.mjs');
    const detector = new DriftDetector();
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
    expect(findings.length, 'governance-leakage not detected in fake-sim path').toBeGreaterThan(0);
    expect(findings[0].kind).toBe('governance-leakage');
  });
});
