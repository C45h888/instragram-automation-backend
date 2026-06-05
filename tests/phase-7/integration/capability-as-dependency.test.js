/**
 * Capability as Dependency — Phase 7 integration test
 * ═════════════════════════════════════════════════════
 *
 * Re-runs the cog-layer cross-cuts at scale. Acquisition and
 * publishing consume graph-capability state only. Token internals
 * never reach them. For each of 10 iterations, inject a capability
 * transition and downstream events with the same correlationId.
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { FORBIDDEN_ENDPOINT_PATTERNS } from '../kernels/_cognition-layer-cross-cuts.js';

const ITERATIONS = 10;

describe('Capability as Dependency — Phase 7 integration', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'capability-as-dependency' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('10 iterations of capability transitions with no token-internals leakage', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const correlationId = `cap-dep-${i}-${Date.now()}`;

      simulator.injectEvent({
        type: 'CAPABILITY_TRANSITION',
        source: 'graph-capability',
        payload: { from: 'VALID', to: i % 2 === 0 ? 'DEGRADED' : 'AUTH_STRIKE', accountId: 'acc-1' },
        correlationId,
      });

      simulator.injectEvent({
        type: 'ACQUISITION_FETCH_COMPLETE',
        source: 'acquisition',
        payload: { worker: 'comments-worker', accountId: 'acc-1' },
        correlationId,
      });

      simulator.injectEvent({
        type: 'PUBLISH_REQUESTED',
        source: 'publishing',
        payload: { worker: 'content-worker', accountId: 'acc-1' },
        correlationId,
      });

      await simulator.tick(3);
    }

    // All capability transitions must be observed
    const timeline = simulator.timeline();
    const capabilityEvents = timeline.filter((e) => e.type === 'CAPABILITY_TRANSITION');
    if (capabilityEvents.length < ITERATIONS) {
      throw new Error(
        `Capability transitions: injected ${ITERATIONS}, observed ${capabilityEvents.length}`
      );
    }

    // No worker trace may contain forbidden token-internals patterns
    const traces = simulator.workerTrace();
    const violations = [];
    for (const record of traces) {
      const context = record.context || {};
      const calls = context.callsMade || context.endpoints || [];
      const flat = JSON.stringify(record);
      for (const pattern of FORBIDDEN_ENDPOINT_PATTERNS) {
        if (flat.match(pattern)) {
          violations.push({ worker: record.worker, pattern: String(pattern) });
        }
      }
      for (const ep of calls) {
        if (typeof ep !== 'string') continue;
        for (const pattern of FORBIDDEN_ENDPOINT_PATTERNS) {
          if (ep.match(pattern)) {
            violations.push({ worker: record.worker, endpoint: ep, pattern: String(pattern) });
          }
        }
      }
    }
    if (violations.length > 0) {
      const err = new Error(
        `Token-internals leakage detected: ${violations.length} violation${violations.length === 1 ? '' : 's'}`
      );
      err.violations = violations;
      throw err;
    }
  }, 120000);
});
