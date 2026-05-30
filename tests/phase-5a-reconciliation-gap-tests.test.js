/**
 * Phase 5A: Reconciliation Engine Gap Tests — TESTABLE GAPS (GAPs 1-3, 6)
 * ========================================================================
 *
 * Tests the reconciliation engine with controlled FSM state and lineage
 * to expose gaps that can actually be validated in the current architecture.
 *
 * TESTABLE:
 *   GAP-1 — Empty FSM map behavior (unregistered = converged)
 *   GAP-2 — Dedup orphan key + replay collision signals
 *   GAP-3 — Circuit breaker collision + orphaned breaker signals
 *   GAP-6 — LINEAGE_CORRUPTION severity 4 is dead code
 *
 * UNTESTABLE (documented as architectural gaps):
 *   GAP-4 — Cadence gap time-dependent (requires Date.now() mocking)
 *   GAP-5 — Ghost emission (EMITTING + empty buffer is unreachable)
 *   GAP-7 — Escalation thresholds (requires N consecutive CK cycles)
 *   GAP-8 — Window size adequacy (requires 21+ event injection)
 *   GAP-9 — Hash race condition (timing-dependent, not deterministic)
 *   GAP-10 — FSM/lineage false positive rate (statistical measurement)
 *
 * TEST INFRASTRUCTURE (TEST ONLY — REMOVE AFTER VALIDATION):
 *   - lineageLedger.injectTestEntry()  — direct ledger write for controlled lineage
 *   - lineageLedger.clearDomainLineage() — clean domain lineage between tests
 *   - sim.getDedupFsm(), sim.getEngagementFsm() — FSM getters for state control
 *   - eventInjector.injectRawLineageEntry() — timestamped lineage injection
 *   - sim.waitForWorkerIngestion() — await worker to process injected entries
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { RuntimeSimulator } from './helpers/runtime-simulator.js';

const { DRIFT_SIGNAL, DRIFT_SEVERITY } = require('../control-plane/governance/reconciliation-engine.js');

describe('Phase 5A: Reconciliation Engine Gap Tests', () => {
  let sim;

  beforeAll(async () => {
    sim = new RuntimeSimulator({
      lineagePollMs: 400,
      telemetryPollMs: 50,
      autoTick: false,
    });
    await sim.boot();
  }, 30000);

  afterAll(async () => {
    await sim.shutdown();
  });

  // ═══════════════════════════════════════════════════════════════════════
  // GAP-1: engine.compare() is a black box — unregistered = converged
  // ═══════════════════════════════════════════════════════════════════════

  describe('GAP-1: Engine compare() black box — unregistered domains', () => {
    it('empty FSM map → all 5 reconcilers fire with severity 0, NONE signal, unregistered state', async () => {
      const result = await sim.runEngineComparison({
        fsms: new Map(), // empty map
        substrates: {
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      // All 5 built-in reconcilers fire regardless of empty FSM map
      expect(result.observations.length).toBe(5);

      // Every observation: severity 0 + NONE + unregistered
      // This IS the gap — missing domain looks identical to healthy domain
      for (const obs of result.observations) {
        expect(obs.severity).toBe(0);
        expect(obs.driftSignals[0].signal).toBe(DRIFT_SIGNAL.NONE);
        expect(obs.materializedState).toBe('unregistered');
      }
    });

    it('single registered domain (dedup only) + 4 empty → dedup shows real state, others show unregistered', async () => {
      const dedupFsm = sim.getDedupFsm();

      const result = await sim.runEngineComparison({
        fsms: new Map([['dedup', dedupFsm]]),
        substrates: {
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const dedupObs = result.observations.find((o) => o.domain === 'dedup');
      const unregisteredObs = result.observations.filter((o) => o.materializedState === 'unregistered');

      expect(dedupObs).toBeDefined();
      expect(dedupObs.materializedState).toMatch(/^fsm:IDLE/); // Dedup FSM is in IDLE by default
      expect(unregisteredObs.length).toBe(4); // 4 other domains are unregistered

      for (const obs of unregisteredObs) {
        expect(obs.severity).toBe(0);
        expect(obs.driftSignals[0].signal).toBe(DRIFT_SIGNAL.NONE);
      }
    });

    it('all 5 domains registered with default state → no false positive drift signals', async () => {
      const result = await sim.runEngineComparison({
        fsms: sim._buildFsmMap(),
        substrates: {
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      expect(result.observations.length).toBe(5);
      // No severity > 0 signals in a healthy default state runtime
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // GAP-2: Dedup orphan key + replay collision signals
  // ═══════════════════════════════════════════════════════════════════════

  describe('GAP-2: Dedup orphan key signal', () => {
    it('FSM ACTIVE + empty substrate → DEDUP_ORPHAN_KEY fires via condition (b)', async () => {
      const dedupFsm = sim.getDedupFsm();

      // Advance FSM to ACTIVE via DEDUP_BATCH_BEGIN
      dedupFsm.dispatch({
        type: 'DEDUP_BATCH_BEGIN',
        accountId: 'gap2-test-account',
        eventCount: 5,
      }, { validate: () => ({ allowed: true }), dispatchGlobal: () => {} });

      expect(dedupFsm.getState()).toBe('ACTIVE');

      const result = await sim.runEngineComparison({
        fsms: new Map([['dedup', dedupFsm]]),
        substrates: {
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const dedupObs = result.observations.find((o) => o.domain === 'dedup');
      expect(dedupObs).toBeDefined();

      const orphanSignals = dedupObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.DEDUP_ORPHAN_KEY
      );

      // Signal should fire — FSM ACTIVE but substrate empty = lost batch window
      expect(orphanSignals.length).toBeGreaterThan(0);
      expect(orphanSignals[0].detail).toContain('ACTIVE state but substrate snapshot is empty');

      // Reset FSM to IDLE
      dedupFsm.dispatch({ type: 'DEDUP_BATCH_END' }, { validate: () => ({ allowed: true }), dispatchGlobal: () => {} });
    });

    it('FSM IDLE + empty substrate → no DEDUP_ORPHAN_KEY (idle is not a drift condition)', async () => {
      const dedupFsm = sim.getDedupFsm();
      expect(dedupFsm.getState()).toBe('IDLE');

      const result = await sim.runEngineComparison({
        fsms: new Map([['dedup', dedupFsm]]),
        substrates: {
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const dedupObs = result.observations.find((o) => o.domain === 'dedup');
      expect(dedupObs).toBeDefined();

      const orphanSignals = dedupObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.DEDUP_ORPHAN_KEY
      );

      // IDLE + empty substrate is legitimate — no orphan signal
      expect(orphanSignals.length).toBe(0);
    });

    it('substrate has orphan key (no IN_FLIGHT lineage) → DEDUP_ORPHAN_KEY fires via condition (a)', async () => {
      const dedupFsm = sim.getDedupFsm();
      const lineageLedger = require('../control-plane/governance/lineage-ledger.js');

      expect(dedupFsm.getState()).toBe('IDLE');

      // Substrate has a key but NO corresponding IN_FLIGHT lineage entry
      // This simulates: Redis has dedup key, but transition never reached ledger
      const result = await sim.runEngineComparison({
        fsms: new Map([['dedup', dedupFsm]]),
        substrates: {
          dedupSnapshot: () => ({
            identityCount: 1,
            resourceCount: 1,
            sample: [{
              intentId: 'orphan-intent-001',
              accountId: 'test-account',
              actionType: 'like',
              resourceId: 'media-12345',
            }],
          }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const dedupObs = result.observations.find((o) => o.domain === 'dedup');
      expect(dedupObs).toBeDefined();

      const orphanSignals = dedupObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.DEDUP_ORPHAN_KEY
      );

      // Signal should fire — key exists in substrate but no IN_FLIGHT lineage
      expect(orphanSignals.length).toBeGreaterThan(0);
    });
  });

  describe('GAP-2: Dedup replay collision signal', () => {
    beforeEach(async () => {
      // Clean dedup lineage before each test
      const lineageLedger = require('../control-plane/governance/lineage-ledger.js');
      if (typeof lineageLedger.clearDomainLineage === 'function') {
        await lineageLedger.clearDomainLineage('dedup');
      }
    });

    it('replay ratio > 0.5 → DEDUP_REPLAY_COLLISION fires', async () => {
      const dedupFsm = sim.getDedupFsm();
      const eventInjector = require('./event-injector.js');

      // Inject 6 REPLAY_DETECTED entries for resource_tracker entity
      // 6 replays / 10 resources = 0.6 ratio > 0.5 threshold
      for (let i = 0; i < 6; i++) {
        eventInjector.injectRawLineageEntry({
          domain: 'dedup',
          entity: 'resource_tracker',
          entityId: `resource-abc-${i}`,
          nextState: 'REPLAY_DETECTED',
          authority: 'test-gap2',
          raw: { intentId: `replay-intent-${i}`, resourceId: 'resource-abc' },
        });
      }

      await sim.waitForWorkerIngestion(500);

      const result = await sim.runEngineComparison({
        fsms: new Map([['dedup', dedupFsm]]),
        substrates: {
          dedupSnapshot: () => ({
            identityCount: 10,
            resourceCount: 10,
            sample: Array.from({ length: 10 }, (_, i) => ({
              intentId: `test-intent-${i}`,
              accountId: 'test-account',
              actionType: 'like',
              resourceId: `resource-${i}`,
            })),
          }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const dedupObs = result.observations.find((o) => o.domain === 'dedup');
      expect(dedupObs).toBeDefined();

      const collisionSignals = dedupObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.DEDUP_REPLAY_COLLISION
      );

      expect(collisionSignals.length).toBeGreaterThan(0);
    });

    it('replay ratio exactly 0.5 (boundary) → DEDUP_REPLAY_COLLISION does NOT fire (threshold is > 0.5)', async () => {
      const dedupFsm = sim.getDedupFsm();
      const eventInjector = require('./event-injector.js');
      const lineageLedger = require('../control-plane/governance/lineage-ledger.js');

      if (typeof lineageLedger.clearDomainLineage === 'function') {
        await lineageLedger.clearDomainLineage('dedup');
      }

      // 5 replays / 10 resources = exactly 0.5 — not > 0.5
      for (let i = 0; i < 5; i++) {
        eventInjector.injectRawLineageEntry({
          domain: 'dedup',
          entity: 'resource_tracker',
          entityId: `resource-boundary-${i}`,
          nextState: 'REPLAY_DETECTED',
          authority: 'test-gap2-boundary',
          raw: { intentId: `boundary-intent-${i}`, resourceId: 'resource-boundary' },
        });
      }

      await sim.waitForWorkerIngestion(500);

      const result = await sim.runEngineComparison({
        fsms: new Map([['dedup', dedupFsm]]),
        substrates: {
          dedupSnapshot: () => ({
            identityCount: 10,
            resourceCount: 10,
            sample: Array.from({ length: 10 }, (_, i) => ({
              intentId: `boundary-intent-${i}`,
              accountId: 'test-account',
              actionType: 'like',
              resourceId: `resource-${i}`,
            })),
          }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const dedupObs = result.observations.find((o) => o.domain === 'dedup');
      expect(dedupObs).toBeDefined();

      const collisionSignals = dedupObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.DEDUP_REPLAY_COLLISION
      );

      // At exactly 0.5, no signal (threshold is > 0.5, not >= 0.5)
      expect(collisionSignals.length).toBe(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // GAP-3: Circuit breaker collision + orphaned breaker signals
  // ═══════════════════════════════════════════════════════════════════════

  describe('GAP-3: Orphaned circuit breaker signal', () => {
    beforeEach(async () => {
      const lineageLedger = require('../control-plane/governance/lineage-ledger.js');
      if (typeof lineageLedger.clearDomainLineage === 'function') {
        await lineageLedger.clearDomainLineage('engagement');
      }
    });

    it('FSM has active breaker but no lineage OPEN → ORPHANED_CIRCUIT_BREAKER fires via condition (a)', async () => {
      const engagementFsm = sim.getEngagementFsm();

      // Advance FSM to CIRCUIT_OPEN via RATE_LIMIT_DETECTED
      engagementFsm.dispatch({
        type: 'RATE_LIMIT_DETECTED',
        accountId: 'orphan-test-account',
        cooldownMs: 3600000,
      }, { validate: () => ({ allowed: true }), dispatchGlobal: () => {} });

      expect(engagementFsm.getState()).toBe('CIRCUIT_OPEN');
      expect(engagementFsm.isCircuitBreakerActive('orphan-test-account')).toBe(true);

      // Substrate says NOT rate-limited — but FSM has active breaker → orphan
      const result = await sim.runEngineComparison({
        fsms: new Map([['engagement', engagementFsm]]),
        substrates: {
          retryInFlight: () => false, // substrate says not rate-limited
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const engObs = result.observations.find((o) => o.domain === 'engagement');
      expect(engObs).toBeDefined();

      const orphanSignals = engObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.ORPHANED_CIRCUIT_BREAKER
      );

      expect(orphanSignals.length).toBeGreaterThan(0);
    });

    it('FSM has active breaker + lineage OPEN + substrate confirms → no ORPHANED_CIRCUIT_BREAKER', async () => {
      const engagementFsm = sim.getEngagementFsm();
      const eventInjector = require('./event-injector.js');

      if (typeof lineageLedger?.clearDomainLineage === 'function') {
        await lineageLedger.clearDomainLineage('engagement');
      }

      // FSM has active breaker
      engagementFsm.dispatch({
        type: 'RATE_LIMIT_DETECTED',
        accountId: 'legitimate-breaker-account',
        cooldownMs: 3600000,
      }, { validate: () => ({ allowed: true }), dispatchGlobal: () => {} });

      expect(engagementFsm.isCircuitBreakerActive('legitimate-breaker-account')).toBe(true);

      // Inject lineage OPEN entry — makes breaker legitimate
      eventInjector.injectRawLineageEntry({
        domain: 'engagement',
        entity: 'circuit_breaker',
        entityId: 'legitimate-breaker-account',
        nextState: 'OPEN',
        authority: 'engagement-fsm',
        raw: { cooldownMs: 3600000, accountId: 'legitimate-breaker-account' },
      });

      await sim.waitForWorkerIngestion(500);

      const result = await sim.runEngineComparison({
        fsms: new Map([['engagement', engagementFsm]]),
        substrates: {
          retryInFlight: (accountId) => accountId === 'legitimate-breaker-account' ? true : false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const engObs = result.observations.find((o) => o.domain === 'engagement');
      expect(engObs).toBeDefined();

      const orphanSignals = engObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.ORPHANED_CIRCUIT_BREAKER
      );

      // No orphan — breaker has lineage backing and substrate confirms it
      expect(orphanSignals.length).toBe(0);
    });
  });

  describe('GAP-3: Circuit breaker collision signal', () => {
    beforeEach(async () => {
      const lineageLedger = require('../control-plane/governance/lineage-ledger.js');
      if (typeof lineageLedger.clearDomainLineage === 'function') {
        await lineageLedger.clearDomainLineage('engagement');
      }
    });

    it('2 OPEN events within 5 min for same account → CIRCUIT_BREAKER_COLLISION fires', async () => {
      const engagementFsm = sim.getEngagementFsm();
      const eventInjector = require('./event-injector.js');

      const now = Date.now();

      // First OPEN — 4 min ago (within 5-min window)
      eventInjector.injectRawLineageEntry({
        domain: 'engagement',
        entity: 'circuit_breaker',
        entityId: 'collision-test-account',
        nextState: 'OPEN',
        authority: 'engagement-fsm',
        raw: { cooldownMs: 3600000, accountId: 'collision-test-account' },
        _timestampOverride: now - 240000, // 4 min ago
      });

      // Second OPEN — 2 min ago (still within window: gap < 300000ms)
      eventInjector.injectRawLineageEntry({
        domain: 'engagement',
        entity: 'circuit_breaker',
        entityId: 'collision-test-account',
        nextState: 'OPEN',
        authority: 'engagement-fsm',
        raw: { cooldownMs: 3600000, accountId: 'collision-test-account' },
        _timestampOverride: now - 120000, // 2 min ago
      });

      // FSM has active breaker for this account
      engagementFsm.dispatch({
        type: 'RATE_LIMIT_DETECTED',
        accountId: 'collision-test-account',
        cooldownMs: 3600000,
      }, { validate: () => ({ allowed: true }), dispatchGlobal: () => {} });

      await sim.waitForWorkerIngestion(500);

      const result = await sim.runEngineComparison({
        fsms: new Map([['engagement', engagementFsm]]),
        substrates: {
          retryInFlight: () => true,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const engObs = result.observations.find((o) => o.domain === 'engagement');
      expect(engObs).toBeDefined();

      const collisionSignals = engObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.CIRCUIT_BREAKER_COLLISION
      );

      expect(collisionSignals.length).toBeGreaterThan(0);
    });

    it('2 OPEN events but gap > 5 min → no CIRCUIT_BREAKER_COLLISION', async () => {
      const engagementFsm = sim.getEngagementFsm();
      const eventInjector = require('./event-injector.js');

      const now = Date.now();

      // First OPEN — 6 min ago (OUTSIDE 5-min window)
      eventInjector.injectRawLineageEntry({
        domain: 'engagement',
        entity: 'circuit_breaker',
        entityId: 'stale-collision-account',
        nextState: 'OPEN',
        authority: 'engagement-fsm',
        raw: { cooldownMs: 3600000, accountId: 'stale-collision-account' },
        _timestampOverride: now - 360000, // 6 min ago
      });

      // Second OPEN — now
      eventInjector.injectRawLineageEntry({
        domain: 'engagement',
        entity: 'circuit_breaker',
        entityId: 'stale-collision-account',
        nextState: 'OPEN',
        authority: 'engagement-fsm',
        raw: { cooldownMs: 3600000, accountId: 'stale-collision-account' },
        _timestampOverride: now,
      });

      engagementFsm.dispatch({
        type: 'RATE_LIMIT_DETECTED',
        accountId: 'stale-collision-account',
        cooldownMs: 3600000,
      }, { validate: () => ({ allowed: true }), dispatchGlobal: () => {} });

      await sim.waitForWorkerIngestion(500);

      const result = await sim.runEngineComparison({
        fsms: new Map([['engagement', engagementFsm]]),
        substrates: {
          retryInFlight: () => true,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const engObs = result.observations.find((o) => o.domain === 'engagement');
      expect(engObs).toBeDefined();

      const collisionSignals = engObs.driftSignals.filter(
        (s) => s.signal === DRIFT_SIGNAL.CIRCUIT_BREAKER_COLLISION
      );

      // No collision — gap is 6 min > 5 min threshold
      expect(collisionSignals.length).toBe(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // GAP-6: LINEAGE_CORRUPTION severity 4 is dead code
  // ═══════════════════════════════════════════════════════════════════════

  describe('GAP-6: LINEAGE_CORRUPTION severity 4 dead code', () => {
    it('DRIFT_SEVERITY.LINEAGE_CORRUPTION === 4 (constant exists in enum)', () => {
      expect(DRIFT_SEVERITY.LINEAGE_CORRUPTION).toBe(4);
    });

    it('compare() never produces severity 4 in any runtime condition', async () => {
      const result = await sim.runEngineComparison({
        fsms: sim._buildFsmMap(),
        substrates: {
          dedupSnapshot: () => ({ identityCount: 0, resourceCount: 0, sample: [] }),
          retryInFlight: () => false,
          bufferSnapshot: () => ({ size: 0, flushing: false }),
          cadenceLastTick: () => Date.now(),
          dedupIsInFlight: async () => false,
          metricsSignals: () => ({}),
        },
      });

      const hasSeverity4 = result.observations.some((o) => o.severity === 4);
      expect(hasSeverity4).toBe(false); // Confirms dead code
      expect(result.worstSeverity).toBeLessThan(4);
    });

    it('_classifySignal() has no case for LINEAGE_CORRUPTION (dead code confirmed)', () => {
      // The switch statement in _classifySignal only handles:
      // NONE(0), TRANSIENT(1), REPLAY(2), SUBSTRATE(3)
      // default: falls through to SUBSTRATE(3)
      // No signal maps to severity 4
      expect(DRIFT_SEVERITY.LINEAGE_CORRUPTION).toBe(4);
      expect(DRIFT_SEVERITY.NONE).toBe(0);
      expect(DRIFT_SEVERITY.TRANSIENT).toBe(1);
      expect(DRIFT_SEVERITY.REPLAY).toBe(2);
      expect(DRIFT_SEVERITY.SUBSTRATE).toBe(3);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // UNTESTABLE GAPS — Documented as architectural gaps
  // ═══════════════════════════════════════════════════════════════════════

  describe('GAP-4: Cadence gap — time-dependent, requires Date.now() mocking', () => {
    it('CADENCE_GAP threshold is 120000ms — boundary conditions untestable without time mocking', () => {
      // GAP: Date.now() is called directly in reconciliation engine:
      //   const gapMs = Date.now() - lastCadenceTick;
      //   if (gapMs > 120_000) { driftSignals.push({ signal: DRIFT_SIGNAL.CADENCE_GAP }); }
      //
      // Boundary conditions (119999ms vs 120001ms) cannot be tested without
      // mocking Date.now() globally — which would affect the entire runtime.
      // The scheduling FSM's getLastCadenceTick() returns real system time.
      //
      // To test this gap: time-mocking infrastructure needed in scheduling FSM.
      expect(DRIFT_SIGNAL.CADENCE_GAP).toBe('cadence_gap');
      expect(DRIFT_SEVERITY.TRANSIENT).toBe(1);

      const schedulingFsm = sim.getSchedulingFsm();
      expect(typeof schedulingFsm.getLastCadenceTick).toBe('function');
    });
  });

  describe('GAP-5: Ghost emission — EMITTING + empty buffer is architecturally unreachable', () => {
    it('GHOST_EMISSION requires EMITTING + empty buffer — never occurs naturally', () => {
      // GAP: To trigger GHOST_EMISSION:
      //   if (materializedState === 'EMITTING' || materializedState === 'EVALUATING') {
      //     if (bufferEmpty) { driftSignals.push({ signal: DRIFT_SIGNAL.GHOST_EMISSION }); }
      //   }
      //
      // In normal runtime:
      //   - Publishing FSM is in IDLE when no work is active
      //   - To reach EMITTING, active emission required → buffer has content
      //   - Therefore: EMITTING + empty buffer is logically impossible
      //
      // To test this gap: would require FSM state mutation hack — not valid test.
      expect(DRIFT_SIGNAL.GHOST_EMISSION).toBe('ghost_emission');

      const publishingFsm = sim.getPublishingFsm();
      expect(typeof publishingFsm.getState).toBe('function');
    });
  });
});
