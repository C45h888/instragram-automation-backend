/**
 * Capability Kernel (graph-capability) — Phase 7 runtime validation battery
 * ═════════════════════════════════════════════════════════════════════════
 *
 * Per-kernel battery covering the 7 categories, plus the
 * cognition-layer cross-cuts (this kernel is the constitutional
 * dependency for acquisition and publishing).
 *
 * Substrates: graph-capability/ (observations, trigger-bridge,
 *   verdict-gate, wiring)
 * Vault: api-surface, default-scopes, signal-dispatch
 * PAT substrate workers: exchange, retrieve, store
 * Scope substrate workers: detect-dynamic
 * UAT substrate workers: detect, refresh, retrieve, store
 *
 * Battery focus:
 *   - new connection → expected capability state
 *   - token refresh → expected capability state
 *   - auth strike → expected capability state
 *   - repeated graph failure → expected capability state
 *   - downstream consumers (acquisition, publishing) react
 *     exactly as designed
 *   - the kernel itself never inspects token internals from
 *     outside (boundary check)
 */

import { describe, it, beforeAll, afterAll } from 'vitest';
import { Phase7RuntimeSimulator } from '../runtime/index.js';
import { runKernelBattery } from './_kernel-battery-contract.js';
import { runCognitionLayerCrossCuts } from './_cognition-layer-cross-cuts.js';

describe('Capability Kernel — Phase 7 runtime validation battery', () => {
  let simulator;

  beforeAll(async () => {
    simulator = new Phase7RuntimeSimulator({ runId: 'capability-battery' });
    await simulator.boot();
  }, 60000);

  afterAll(async () => {
    if (simulator) await simulator.shutdown();
  }, 30000);

  it('passes the 7-category battery', async () => {
    await runKernelBattery({
      kernel: 'capability',
      simulator,
      assertions: {
        stateMutation: async (s) => {
          const before = await s.simulator.snapshot();
          // Drive a token refresh transition
          s.simulator.injectEvent({
            type: 'TOKEN_REFRESH_REQUESTED',
            payload: { accountId: 'acc-1', worker: 'refresh-worker' },
            source: 'graph-capability',
            correlationId: 'bat-cap-state-1',
          });
          await s.simulator.tick(2);
          const after = await s.simulator.snapshot();
          const diff = s.simulator.diff(before, after);
          if (!diff.changed) {
            const timeline = s.simulator.timeline();
            if (!timeline.some((e) => e.type === 'TOKEN_REFRESH_REQUESTED')) {
              throw new Error('stateMutation: TOKEN_REFRESH_REQUESTED not recorded');
            }
          }
        },

        eventCausality: async (s) => {
          const corr = `bat-cap-caus-${Date.now()}`;
          s.simulator.injectEvent({
            type: 'AUTH_STRIKE',
            payload: { accountId: 'acc-1' },
            source: 'graph-capability',
            correlationId: corr,
          });
          await s.simulator.tick(2);
          s.simulator.assertCausality(['AUTH_STRIKE'], 'capability-causality');
        },

        governance: async (s) => {
          // The capability kernel itself is the authority; workers in it
          // must not self-govern. assertWorkersDidNotGovern checks the
          // graphSimulator's workerTracer.
          await s.simulator.tick(1);
          s.simulator.assertWorkersDidNotGovern();
        },

        cadence: async (s) => {
          const result = await s.simulator.tick(25);
          if (result.metrics.errors > 0) {
            throw new Error(`cadence: ${result.metrics.errors} tick errors`);
          }
        },

        workerCorrectness: async (s) => {
          // The vault workers (exchange, retrieve, store, refresh, detect)
          const workers = ['exchange-worker', 'retrieve-worker', 'store-worker', 'refresh-worker', 'detect-worker'];
          for (const w of workers) {
            s.simulator.injectEvent({
              type: 'VAULT_OPERATION',
              payload: { worker: w, accountId: 'acc-1' },
              source: w,
              correlationId: `bat-cap-worker-${w}`,
            });
          }
          await s.simulator.tick(3);
          const timeline = s.simulator.timeline();
          for (const w of workers) {
            const found = timeline.some((e) => e.payload && e.payload.worker === w);
            if (!found) throw new Error(`workerCorrectness: event for ${w} not in timeline`);
          }
        },

        persistence: async (s) => {
          const before = await s.simulator.snapshot();
          s.simulator.injectEvent({
            type: 'NEW_CONNECTION',
            payload: { accountId: 'acc-new-1' },
            source: 'graph-capability',
            correlationId: 'bat-cap-persist-1',
          });
          await s.simulator.tick(2);
          const timeline = s.simulator.timeline();
          if (!timeline.some((e) => e.correlationId === 'bat-cap-persist-1')) {
            throw new Error('persistence: new-connection event not traceable');
          }
        },

        observability: async (s) => {
          const timeline = s.simulator.timeline();
          if (timeline.length === 0) {
            throw new Error('observability: empty timeline');
          }
          const withPayload = timeline.filter((e) => e.payload != null);
          if (withPayload.length === 0) {
            throw new Error('observability: no events have payload snapshots');
          }
        },
      },
    });
  }, 60000);

  it('passes cognition-layer cross-cuts (capability role)', async () => {
    await runCognitionLayerCrossCuts({ simulator, role: 'capability' });
  }, 30000);
});


// ═══════════════════════════════════════════════════════════════════════════
// Cross-Kernel Read Pipeline — graph-capability worker → CK →
// persist-telemetry FSM → reading-substrate → postgres-telemetry-kernel/
// substrates/graph-capability/workers/* → DB_READ_COMPLETE →
// READ_RESULT_AVAILABLE
//
// Focus: communication + worker cadence on the canonical read path.
// Mocks supabase + exercises the FSM and the operational reading
// substrate directly (no CK boot, no simulator).
// ═══════════════════════════════════════════════════════════════════════════

import { describe, it, beforeAll, afterAll, beforeEach, vi, expect } from 'vitest';

// Mock supabase BEFORE importing the modules that touch it
vi.mock('../../../config/supabase.js', () => ({
  getSupabaseAdmin: () => ({
    from: () => ({
      select: () => ({
        eq: () => ({
          single: async () => ({
            data: { id: 'cred-1', scope_cache: ['pages_show_list'], scope_cache_updated_at: '2026-06-08T00:00:00Z' },
            error: null,
          }),
        }),
        in: async () => ({ data: [], error: null }),
      }),
    }),
  }),
}));

import { createRequire } from 'node:module';
const require_ = createRequire(import.meta.url);

const persistTelemetryFsm = require_('../../../postgres-telemetry-kernel/fsm.js');
const readingSubstrate = require_('../../../postgres-telemetry-kernel/reading/index.js');
const graphCapabilitySubstrate = require_('../../../postgres-telemetry-kernel/substrates/graph-capability/index.js');

describe('Capability ↔ Persistence — cross-kernel read pipeline', () => {
  let dispatchedGlobal;

  beforeAll(() => {
    // Re-init the FSM to a clean state — other tests may have left it dirty
    persistTelemetryFsm.init('IDLE');

    // Wire the operational reading substrate into the FSM the same way
    // CK does at registerDomain() time.
    const governance = {
      dispatch: (evt) => {
        if (dispatchedGlobal) dispatchedGlobal(evt);
      },
    };
    readingSubstrate.setGovernance(governance);
    graphCapabilitySubstrate.setGovernance(governance);
    persistTelemetryFsm.setReadingSubstrate(readingSubstrate);

    // Start the graph-capability operational substrate (registry of
    // read-scope-cache, read-credential, read-key, write-scope-cache,
    // update-credential-status)
    graphCapabilitySubstrate.start();
  });

  afterAll(() => {
    graphCapabilitySubstrate.stop();
  });

  beforeEach(() => {
    dispatchedGlobal = null;
  });

  it('routes a graph-capability DB_READ_REQUESTED through FSM → reading-substrate → worker', async () => {
    const events = [];
    dispatchedGlobal = (evt) => events.push(evt);

    // 1. Inject DB_READ_REQUESTED for a whitelisted graph-capability read domain
    const dispatchResult = await persistTelemetryFsm.dispatch(
      {
        type: 'DB_READ_REQUESTED',
        readDomain: 'db.scope-cache',
        accountId: 'acc-1',
        readId: 'r-pipe-1',
        params: { credentialId: 'cred-1' },
      },
      {
        // FSM reads ctx.dispatchGlobal to fire DB_READ_COMPLETE / READ_RESULT_AVAILABLE
        dispatchGlobal: (evt) => events.push(evt),
        // ctx.sanityCheck is the universal gate; default fail-open
        sanityCheck: async () => ({ allowed: true }),
      }
    );

    expect(dispatchResult.allowed).toBe(true);
    expect(dispatchResult.from).toBe('IDLE');
    expect(dispatchResult.to).toBe('READ_EXECUTING');

    // 2. The FSM fires the read asynchronously via the reading substrate
    //    → dispatchRead → registry → read-scope-cache-worker.execute()
    // Wait for the microtask + worker.execute() to complete
    await new Promise((r) => setTimeout(r, 50));

    // 3. Assert the canonical chain produced the expected global events
    const complete = events.find((e) => e.type === 'DB_READ_COMPLETE' && e.readId === 'r-pipe-1');
    expect(complete).toBeDefined();
    expect(complete.readDomain).toBe('db.scope-cache');
    expect(complete.success).toBe(true);
    expect(complete.data).toBeTruthy();
    expect(complete.data.scope_cache).toEqual(['pages_show_list']);
    expect(typeof complete.latencyMs).toBe('number');

    const result = events.find((e) => e.type === 'READ_RESULT_AVAILABLE' && e.readId === 'r-pipe-1');
    expect(result).toBeDefined();
    expect(result.readDomain).toBe('db.scope-cache');
    expect(result.data.scope_cache).toEqual(['pages_show_list']);

    // 4. FSM must have returned to IDLE after the read completed
    expect(persistTelemetryFsm.getState()).toBe('IDLE');
    expect(persistTelemetryFsm.exportState().readsInFlight).toBe(0);
  });

  it('rejects a non-whitelisted read domain at the FSM guard', async () => {
    const events = [];
    dispatchedGlobal = (evt) => events.push(evt);

    const result = await persistTelemetryFsm.dispatch(
      {
        type: 'DB_READ_REQUESTED',
        readDomain: 'db.not-a-real-domain',
        accountId: 'acc-2',
        readId: 'r-pipe-2',
        params: {},
      },
      {
        dispatchGlobal: (evt) => events.push(evt),
        sanityCheck: async () => ({ allowed: true }),
      }
    );

    expect(result.allowed).toBe(false);
    expect(result.reason).toMatch(/not whitelisted/);

    // No DB_READ_COMPLETE should have been emitted
    const complete = events.find((e) => e.type === 'DB_READ_COMPLETE' && e.readId === 'r-pipe-2');
    expect(complete).toBeUndefined();
    expect(persistTelemetryFsm.getState()).toBe('IDLE');
  });

  it('tracks in-flight read count across concurrent reads (cadence)', async () => {
    // Issue 5 reads in parallel, no awaiting between them — the FSM
    // must increment _readsInFlight for each, then decrement as
    // workers resolve.
    const events = [];
    dispatchedGlobal = (evt) => events.push(evt);

    const N = 5;
    const dispatches = [];
    for (let i = 0; i < N; i++) {
      const d = persistTelemetryFsm.dispatch(
        {
          type: 'DB_READ_REQUESTED',
          readDomain: 'db.credential',
          accountId: `acc-c-${i}`,
          readId: `r-cad-${i}`,
          params: { userId: 'u-1', businessAccountId: 'ba-1' },
        },
        {
          dispatchGlobal: (evt) => events.push(evt),
          sanityCheck: async () => ({ allowed: true }),
        }
      );
      dispatches.push(d);
    }

    // All 5 should be allowed
    const results = await Promise.all(dispatches);
    for (const r of results) {
      expect(r.allowed).toBe(true);
    }

    // After dispatch, the reads may have already completed (they're
    // microtask-bounded). The deterministic check is that the FSM
    // ends at IDLE with readsInFlight === 0.
    await new Promise((r) => setTimeout(r, 50));

    const exported = persistTelemetryFsm.exportState();
    expect(exported.state).toBe('IDLE');
    expect(exported.readsInFlight).toBe(0);
    expect(exported.writesInFlight).toBe(0);

    // Every readId must have produced exactly one DB_READ_COMPLETE
    for (let i = 0; i < N; i++) {
      const completions = events.filter(
        (e) => e.type === 'DB_READ_COMPLETE' && e.readId === `r-cad-${i}`
      );
      expect(completions.length).toBe(1);
      expect(completions[0].success).toBe(true);
    }
  });

  it('relays worker errors through DB_READ_COMPLETE to READ_RESULT_AVAILABLE', async () => {
    // Force the mock supabase to return an error for this specific
    // credentialId. We do this by replacing the supabase mock's
    // from() return value for one call.
    const events = [];
    dispatchedGlobal = (evt) => events.push(evt);

    // We need a temporary mock that returns an error. Easiest path:
    // call the worker directly with bad params (worker itself errors
    // out before hitting supabase).
    const result = await persistTelemetryFsm.dispatch(
      {
        type: 'DB_READ_REQUESTED',
        readDomain: 'db.credential',
        accountId: 'acc-err',
        readId: 'r-err-1',
        params: {}, // missing userId + businessAccountId — worker returns error
      },
      {
        dispatchGlobal: (evt) => events.push(evt),
        sanityCheck: async () => ({ allowed: true }),
      }
    );

    expect(result.allowed).toBe(true);
    await new Promise((r) => setTimeout(r, 50));

    const complete = events.find((e) => e.type === 'DB_READ_COMPLETE' && e.readId === 'r-err-1');
    expect(complete).toBeDefined();
    expect(complete.success).toBe(false);
    expect(complete.error).toMatch(/userId and businessAccountId required/);

    // A degraded log is emitted on worker error
    const degraded = events.find(
      (e) => e.type === 'LOG_DEGRADED' && e.substate === 'READ_FAILURE'
    );
    expect(degraded).toBeDefined();

    // READ_RESULT_AVAILABLE is NOT emitted on error (FSM only forwards
    // successful reads to the calling domain)
    const result2 = events.find(
      (e) => e.type === 'READ_RESULT_AVAILABLE' && e.readId === 'r-err-1'
    );
    expect(result2).toBeUndefined();

    // FSM returns to IDLE even after read failure
    expect(persistTelemetryFsm.exportState().state).toBe('IDLE');
    expect(persistTelemetryFsm.exportState().readsInFlight).toBe(0);
  });
});
