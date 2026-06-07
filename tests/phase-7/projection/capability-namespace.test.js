// ============================================
// Capability namespace — projection plane integration tests
// ============================================
// Verifies the 6th projection namespace (capability) is wired end-to-end:
//   - capability-projection-worker emits PROJECTION_INTENT with
//     projectionNamespace='capability'
//   - capability-transition-writer filters domain='capability' +
//     raw.entryType='SEMANTIC_PROJECTION_TRANSITION'
//   - namespace-projection-interpreter.interpret() updates
//     _projections.domain.capability
//   - getDomainProjection('capability') returns the new slot
//   - retry-cadence policy.getPolicy('telemetry:capability') returns the
//     capability-specific policy (not the engagement default)

import { describe, it, expect, beforeEach } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';

const projectionWorkers = require('../../../telemetry-kernel/substrates/projection/workers');
const transitionWriters = require('../../../telemetry-kernel/substrates/projection/transition-writers');
const namespaceProjectionInterpreter = require('../../../control-plane/governance/interpreters/namespace-projection-interpreter');
const capabilitySynthesis = require('../../../telemetry-kernel/substrates/projection/synthesis/capability-projection');
const capabilityInput = require('../../../telemetry-kernel/substrates/projection/inputs/capability-input');
const retryPolicy = require('../../../retry-cadence-kernel/policy');
const substrateRegistry = require('../../../acquisition-kernel/substrate-registry');

describe('Capability namespace — projection plane', () => {
  // ── 1. Worker registration ──────────────────────────────────────────────
  describe('Worker registration', () => {
    it('capability worker is exported from the 6-worker registry', () => {
      const keys = Object.keys(projectionWorkers.workers);
      expect(keys).toContain('capability');
      expect(keys).toHaveLength(6);
    });

    it('capability worker is an instance of CapabilityProjectionWorker (has the right contract)', () => {
      const w = projectionWorkers.workers.capability;
      expect(w).toBeDefined();
      expect(w._projectType).toBe('CAPABILITY_PROJECTION');
      expect(w._domain).toBe('capability');
      expect(typeof w._getNormalizedInputWindow).toBe('function');
      expect(typeof w._runSynthesis).toBe('function');
      expect(typeof w._computeConfidence).toBe('function');
      expect(typeof w._computeIntegrityScore).toBe('function');
    });

    it('capability worker file does NOT import governance/lineage-worker directly', () => {
      const src = readFileSync(
        path.resolve(process.cwd(), 'telemetry-kernel/substrates/projection/workers/capability-projection-worker.js'),
        'utf8',
      );
      expect(src.includes('governance/lineage-worker')).toBe(false);
    });
  });

  // ── 2. Transition writer ───────────────────────────────────────────────
  describe('Transition writer', () => {
    it('capability writer is in the 6-writer registry', () => {
      const keys = Object.keys(transitionWriters.writers);
      expect(keys).toContain('capability');
      expect(keys).toHaveLength(6);
    });

    it('capability is in the NAMESPACES export', () => {
      expect(transitionWriters.NAMESPACES).toContain('capability');
      expect(transitionWriters.NAMESPACES).toHaveLength(6);
    });
  });

  // ── 3. Namespace projection interpreter ───────────────────────────────
  describe('Namespace projection interpreter', () => {
    it('_projections.domain.capability slot is initialized', () => {
      const slot = namespaceProjectionInterpreter.getDomainProjection('capability');
      expect(slot).toBeDefined();
      expect(slot.state).toBe('UNKNOWN');
      expect(slot.authorityStability).toBe(1.0);
      expect(slot.transitionCount).toBe(0);
    });

    it('interpret() updates _projections.domain.capability on capability projection entries', () => {
      // Reset interpreter's projection state by getting current and re-interpreting fresh
      const entry = {
        raw: {
          projectionNamespace: 'capability',
          projectionPayload: {
            currentCapabilityState: 'AUTHORIZED',
            capabilityAuthorityStability: 0.95,
            timestamp: Date.now(),
          },
        },
        timestamp: Date.now(),
        domain: 'capability',
        authority: 'capability-projection-worker',
      };
      // The interpreter's persist call may throw in test env (no lineageLedger module
      // or no Redis). _computeDomainProjection runs BEFORE the persist call, so the
      // in-memory state is already updated. We catch the persist error to assert state.
      try { namespaceProjectionInterpreter.interpret({ ledgerId: 'test-ledger-1', entry }); } catch (_) {}
      const slot = namespaceProjectionInterpreter.getDomainProjection('capability');
      expect(slot.state).toBe('AUTHORIZED');
      expect(slot.authorityStability).toBeCloseTo(0.95, 5);
      expect(slot.transitionCount).toBeGreaterThanOrEqual(1);
    });

    it('interpret() handles UNAUTHORIZED state', () => {
      const entry = {
        raw: {
          projectionNamespace: 'capability',
          projectionPayload: {
            currentCapabilityState: 'UNAUTHORIZED',
            capabilityAuthorityStability: 0.0,
            timestamp: Date.now(),
          },
        },
        timestamp: Date.now(),
        domain: 'capability',
        authority: 'capability-projection-worker',
      };
      try { namespaceProjectionInterpreter.interpret({ ledgerId: 'test-ledger-2', entry }); } catch (_) {}
      const slot = namespaceProjectionInterpreter.getDomainProjection('capability');
      expect(slot.state).toBe('UNAUTHORIZED');
      expect(slot.authorityStability).toBe(0.0);
    });

    it('interpret() clamps capabilityAuthorityStability to [0, 1]', () => {
      const entry = {
        raw: {
          projectionNamespace: 'capability',
          projectionPayload: {
            currentCapabilityState: 'AUTHORIZED',
            capabilityAuthorityStability: 5.0, // out of range
            timestamp: Date.now(),
          },
        },
        timestamp: Date.now(),
        domain: 'capability',
        authority: 'capability-projection-worker',
      };
      try { namespaceProjectionInterpreter.interpret({ ledgerId: 'test-ledger-3', entry }); } catch (_) {}
      const slot = namespaceProjectionInterpreter.getDomainProjection('capability');
      expect(slot.authorityStability).toBeLessThanOrEqual(1);
      expect(slot.authorityStability).toBeGreaterThanOrEqual(0);
    });
  });

  // ── 4. Synthesis purity ────────────────────────────────────────────────
  describe('Capability synthesis', () => {
    it('synthesize is deterministic given same inputs', () => {
      const signals = {
        transitions: [
          { nextState: 'UNKNOWN', timestamp: 1000 },
          { nextState: 'AUTHORIZED', timestamp: 2000 },
        ],
        scopeCoverage: 1.0,
        now: 3000,
      };
      const a = capabilitySynthesis.synthesize({}, signals);
      const b = capabilitySynthesis.synthesize({}, signals);
      expect(a).toEqual(b);
    });

    it('synthesize returns AUTHORIZED when last transition is AUTHORIZED', () => {
      const signals = {
        transitions: [
          { nextState: 'AUTHORIZED', timestamp: 1000 },
          { nextState: 'AUTHORIZED', timestamp: 2000 },
        ],
        now: 3000,
      };
      const result = capabilitySynthesis.synthesize({}, signals);
      expect(result.currentCapabilityState).toBe('AUTHORIZED');
      expect(result.capabilityAuthorityStability).toBeGreaterThan(0.9);
    });

    it('synthesize returns UNKNOWN for empty input', () => {
      const result = capabilitySynthesis.synthesize({}, { transitions: [], now: 1000 });
      expect(result.currentCapabilityState).toBe('UNKNOWN');
    });

    it('computeConfidence: noise gate when transitions.length < 3', () => {
      expect(capabilitySynthesis.computeConfidence({ noiseGate: true })).toBe(0.0);
      expect(capabilitySynthesis.computeConfidence({ noiseGate: false, transitions: [{}, {}] })).toBe(0.3);
      expect(capabilitySynthesis.computeConfidence({ noiseGate: false, transitions: [{}, {}, {}] })).toBe(0.6);
    });
  });

  // ── 5. Input layer ─────────────────────────────────────────────────────
  describe('Capability input', () => {
    it('getNormalizedInputWindow returns a window with noiseGate for empty state', async () => {
      const w = await capabilityInput.getNormalizedInputWindow({ tickCount: 0 });
      expect(w).toBeDefined();
      expect(Array.isArray(w.transitions)).toBe(true);
      expect(typeof w.now).toBe('number');
      expect(typeof w.windowOpenedAt).toBe('number');
      expect(typeof w.noiseGate).toBe('boolean');
    });
  });

  // ── 6. Retry cadence ───────────────────────────────────────────────────
  describe('Retry cadence policy', () => {
    it('telemetry:capability has a distinct policy (not the engagement default)', () => {
      const policy = retryPolicy.getPolicy('telemetry:capability');
      expect(policy).toBeDefined();
      expect(policy.maxRetries).toBe(3);
      expect(policy.baseDelayMs).toBe(15000);
      expect(policy.maxDelayMs).toBe(60000);
      expect(policy.backoffMultiplier).toBe(1.5);
    });

    it('computeDelay applies the policy correctly', () => {
      const policy = retryPolicy.getPolicy('telemetry:capability');
      expect(retryPolicy.computeDelay(policy, 1)).toBe(15000);
      expect(retryPolicy.computeDelay(policy, 2)).toBe(22500);
      expect(retryPolicy.computeDelay(policy, 3)).toBe(33750);
    });
  });

  // ── 7. Substrate registry ──────────────────────────────────────────────
  describe('Substrate registry wiring', () => {
    it('telemetry:capability resolves to the capability retry worker', () => {
      const w = substrateRegistry.getRetryWorker('telemetry:capability');
      expect(w).toBeDefined();
      expect(w.NAMESPACE).toBe('capability');
      expect(w.STAGING_KEY).toBe('lineage:projection-staging:capability');
      expect(typeof w.execute).toBe('function');
    });
  });
});
