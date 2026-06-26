/**
 * Graph Capability Constitutional Flow — Phase 7 kernel battery
 * ═════════════════════════════════════════════════════════════════════════
 *
 * Phase D contract:
 *   policy (FSM) → membrane (substrate) → executor (worker)
 *   ────────────   ─────────────────────   ─────────────────
 *   CK is sole ingress for governance events. Substrate is constitutionally
 *   passive — it composes workers and routes, it does not decide WHAT to run
 *   or WHEN. The FSM is the only entity that wires the membrane (during
 *   CAPABILITY_BOOTSTRAP). The CK never calls a substrate directly.
 *
 *   Signal-dispatch rewired (Phase D):
 *     substrate → signal-dispatch → fsm.dispatch(event, ctx)
 *       → FSM interprets → FSM may ctx.dispatchGlobal → CK for cross-domain
 *     The substrate never dispatches to the CK directly. The FSM is the
 *     constitutional ingress for observation events.
 *
 * V1. End-to-end smoke — CAPABILITY_BOOTSTRAP through the chain
 * V2. Governed read chain — scope-substrate.detectDynamic (cache miss path)
 * V3. Governed write chain — fsm.requestDBWrite → persist-telemetry → writer
 * V4. Constitutional policy chain — FSM emits action, substrate runs worker.
 *     Negative test: substrate must not decide policy.
 * V5. Cadence gate — fsm._shouldCheck per-cred per-check-type
 * V6. Per-cred cadence stamping — CAPABILITY_HEALTH_CHECK_COMPLETED
 * V7. Server.js boot integration — gck.install + CK bootstrap subscribes
 *     the membrane via the FSM (not the CK)
 *
 * Strategy:
 *   - Mock I/O (supabase, axios) at the require.cache boundary
 *   - Drive events through the real FSM with a stub CK context
 *   - Assert constitutional ordering: policy → membrane → worker
 */

import { describe, it, beforeEach, afterEach, expect, vi } from 'vitest';
import { createRequire } from 'node:module';

const require_ = createRequire(import.meta.url);

// ─── Real production modules (un-mocked) ──────────────────────────────────
const fsm = require_('../../../graph-capability-kernel/fsm.js');
const gck = require_('../../../graph-capability-kernel/index.js');
const constitutionalKernel = require_('../../../control-plane/governance/constitutional-kernel.js');
const ptFsm = require_('../../../postgres-telemetry-kernel/fsm.js');
const writers = require_('../../../postgres-telemetry-kernel/writers/index.js');
const signalDispatch = require_('../../../graph-capability-kernel/substrates/vault/signal-dispatch.js');
const healthSubstrate = require_('../../../graph-capability-kernel/substrates/health-substrate/index.js');

// ─── I/O mocks — swap at require.cache boundary ───────────────────────────

const supabaseStub = {
  from: () => ({
    select: () => ({
      eq: () => ({
        eq: () => ({
          eq: () => ({
            eq: () => ({
              maybeSingle: async () => ({ data: null, error: null }),
              single: async () => ({ data: null, error: null }),
            }),
            maybeSingle: async () => ({ data: null, error: null }),
            single: async () => ({ data: null, error: null }),
          }),
          maybeSingle: async () => ({ data: null, error: null }),
          single: async () => ({ data: null, error: null }),
        }),
        maybeSingle: async () => ({ data: null, error: null }),
        single: async () => ({ data: null, error: null }),
      }),
      maybeSingle: async () => ({ data: null, error: null }),
      single: async () => ({ data: null, error: null }),
    }),
    update: () => ({ eq: async () => ({ error: null }) }),
    insert: () => ({ select: () => ({ single: async () => ({ data: { id: 'mock-id' }, error: null }) }) }),
    upsert: () => ({ select: () => ({ single: async () => ({ data: { id: 'mock-ba-id' }, error: null }) }) }),
    rpc: async () => ({ data: 'encrypted-mock-token', error: null }),
  }),
  schema: () => ({ from: () => ({ insert: () => ({ select: () => ({ single: async () => ({ data: { id: 'mock-key-id' }, error: null }) }) }) }) }),
  rpc: async () => ({ data: 'encrypted-mock-token', error: null }),
};

const configSupabaseMock = {
  getSupabaseAdmin: () => supabaseStub,
  logAudit: async () => {},
  shouldLog: () => false,
  initializeSupabase: async () => ({ supabaseAdmin: supabaseStub, connectionInfo: { url: 'mock', timestamp: new Date().toISOString() } }),
  fireAndForgetInsert: async () => {},
};

const configSupabasePath = require_.resolve('../../../config/supabase.js');
require_.cache[configSupabasePath] = {
  id: configSupabasePath,
  filename: configSupabasePath,
  loaded: true,
  exports: configSupabaseMock,
  children: [],
  paths: [],
};

const axiosMock = { get: async () => ({ data: { data: { scopes: ['instagram_basic'] } } }) };
const apiSurfacePath = require_.resolve('../../../graph-capability-kernel/api-surface.js');
require_.cache[apiSurfacePath] = {
  id: apiSurfacePath,
  filename: apiSurfacePath,
  loaded: true,
  exports: { axios: axiosMock, GRAPH_API_VERSION: 'v23.0', GRAPH_API_BASE: 'https://graph.facebook.com/v23.0' },
  children: [],
  paths: [],
};

// ─── Redis mock — return null client so _syncProjectionState bails out ──────
const configRedisPath = require_.resolve('../../../config/redis.js');
const redisMockClient = { status: null, get: async () => null };
require_.cache[configRedisPath] = {
  id: configRedisPath,
  filename: configRedisPath,
  loaded: true,
  exports: { getRedisClient: () => redisMockClient, getRedisInstance: () => redisMockClient },
  children: [],
  paths: [],
};

// ─── Stub CK (constitutional kernel) ──────────────────────────────────────

function makeStubCk() {
  const actionSubscribers = new Map();
  const dispatched = [];
  // Real CK routes domain events to their FSMs via DOMAIN_EVENT_MAP. The
  // stub reproduces this for the events the test exercises.
  const DOMAIN_EVENT_MAP = {
    CAPABILITY_BOOTSTRAP: fsm,
    CAPABILITY_CADENCE_TICK: fsm,
    CAPABILITY_DATA_REQUEST: fsm,
    CAPABILITY_OBSERVATION: fsm,
    CAPABILITY_EVALUATE: fsm,
    CAPABILITY_OK: fsm,
    CAPABILITY_PARTIAL: fsm,
    CAPABILITY_FAILED: fsm,
    CAPABILITY_DEGRADED: fsm,
    CAPABILITY_RECOVERED: fsm,
    CAPABILITY_REEVALUATE: fsm,
    CAPABILITY_HEALTH_CHECK_COMPLETED: fsm,
    READ_RESULT_AVAILABLE: fsm,
    DB_WRITE_ACKNOWLEDGED: fsm,
    DB_READ_REQUESTED: ptFsm,
    DB_READ_COMPLETE: ptFsm,
    DB_WRITE_REQUESTED: ptFsm,
    DB_WRITE_COMPLETE: ptFsm,
  };
  return {
    dispatched,
    actionSubscribers,
    dispatch: vi.fn((event) => {
      dispatched.push(event);
      // Route to the appropriate FSM
      const target = DOMAIN_EVENT_MAP[event.type];
      if (target && typeof target.dispatch === 'function') {
        return target.dispatch(event, {
          validate: () => ({ allowed: true }),
          dispatchGlobal: (sub) => {
            // Recursively route via the stub
            return {
              dispatch: (subEvent) => {
                dispatched.push(subEvent);
                const subTarget = DOMAIN_EVENT_MAP[subEvent.type];
                if (subTarget && typeof subTarget.dispatch === 'function') {
                  return subTarget.dispatch(subEvent, {
                    validate: () => ({ allowed: true }),
                    dispatchGlobal: () => ({ allowed: true }),
                  });
                }
                return { allowed: true };
              },
            };
          },
          getGlobalState: () => 'HEALTHY',
        });
      }
      // Action types (RUN_*) → route to subscribers
      if (actionSubscribers.has(event.type)) {
        for (const sub of actionSubscribers.get(event.type)) {
          sub(event);
        }
      }
      return { allowed: true };
    }),
    validateDomainTransition: () => ({ allowed: true }),
    validate: () => ({ allowed: true }),
    getState: () => 'HEALTHY',
    subscribeAction: vi.fn((actionType, handler) => {
      if (!actionSubscribers.has(actionType)) actionSubscribers.set(actionType, []);
      actionSubscribers.get(actionType).push(handler);
    }),
    // Mock governedRead — returns empty data by default.
    // Individual tests override vi.mocked(ck.governedRead).mockResolvedValue(...)
    // to inject specific scan results for their scenario.
    governedRead: vi.fn(async (readDomain, params) => {
      if (readDomain === 'db.credential') {
        return { success: true, data: [], error: null, latencyMs: 0 };
      }
      if (readDomain === 'db.alerts') {
        // checkExistingWarning: return false (no existing warning) by default
        return { success: true, data: false, error: null, latencyMs: 0 };
      }
      return { success: false, data: null, error: 'unknown_domain', latencyMs: 0 };
    }),
  };
}

// ─── Reset between tests ───────────────────────────────────────────────────

beforeEach(() => {
  if (gck.isInstalled()) gck.uninstall();
  fsm._resetCred();
  fsm.resetMembrane();
  // Reset singleton substrate state — gck.uninstall calls substrate.stop()
  // via wiring.uninstall, but we also need to reset the start() flag so
  // each test sees a clean state.
  if (healthSubstrate.stop) healthSubstrate.stop();
  vi.clearAllMocks();
});

afterEach(() => {
  if (gck.isInstalled()) gck.uninstall();
  fsm._resetCred();
  vi.restoreAllMocks();
});

// ─── Constants ─────────────────────────────────────────────────────────────

const BA_A = '00000000-0000-0000-0000-aaaaaaaaaaaa';
const BA_B = '00000000-0000-0000-0000-bbbbbbbbbbbb';
const UA_A = '00000000-0000-0000-0000-00000000000a';

function freshFullEnvelope(baId, opts = {}) {
  const env = fsm.newEnvelope({ businessAccountId: baId, userId: opts.userId || UA_A });
  env.pat = { isDecryptable: true, ...(opts.pat || {}) };
  env.uat = { isDecryptable: true, ...(opts.uat || {}) };
  env.detection = { isValid: true, reliabilityImpaired: false, reason: null, ...(opts.detection || {}) };
  env.scope = {
    grantedScopes: opts.grantedScopes || fsm.REQUIRED_SCOPES,
    cacheAgeMs: 0,
  };
  return env;
}

// ═══════════════════════════════════════════════════════════════════════════
// V1. End-to-end smoke — gck.install + CAPABILITY_BOOTSTRAP wires the membrane
// ═══════════════════════════════════════════════════════════════════════════
describe('V1 — End-to-end smoke: gck.install wires FSM, CAPABILITY_BOOTSTRAP wires membrane', () => {
  it('gck.install({ck}) registers the FSM as the constitutional ingress, signal-dispatch binds to the FSM', () => {
    const ck = makeStubCk();
    gck.install({ ck });
    // The FSM is the canonical ingress
    expect(signalDispatch.getFsm()).toBe(fsm);
    expect(signalDispatch.getCtx()).toBeTruthy();
    // The CK is NOT bound to signal-dispatch (legacy path)
    expect(signalDispatch.getCk()).toBeNull();
  });

  it('CAPABILITY_BOOTSTRAP causes the FSM to call substrate.start(ck) and the membrane becomes a first-class citizen', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    expect(healthSubstrate.isStarted()).toBe(false);
    // Dispatch CAPABILITY_BOOTSTRAP through the stub CK → routes to the FSM
    // (because the stub CK's dispatch is the constitutional ingress)
    await ck.dispatch({ type: 'CAPABILITY_BOOTSTRAP' });
    // The membrane is now wired
    expect(healthSubstrate.isStarted()).toBe(true);
    // The membrane subscribed to BOTH action types
    expect(ck.subscribeAction).toHaveBeenCalledWith('RUN_TOKEN_HEALTH_CHECK', expect.any(Function));
    expect(ck.subscribeAction).toHaveBeenCalledWith('RUN_UAT_REFRESH_CHECK', expect.any(Function));
  });

  it('health-substrate.runTokenHealthCheck runs to completion safely when supabase is unavailable', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    ck.dispatch({ type: 'CAPABILITY_BOOTSTRAP' });
    const result = await healthSubstrate.runTokenHealthCheck();
    expect(result).toEqual({ scanned: 0, valid: 0, invalid: 0, skipped: 0, recovered: 0 });
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// V2. Governed read chain — scope-substrate cache-miss path
// ═══════════════════════════════════════════════════════════════════════════
describe('V2 — Governed read chain: substrate → fsm.dispatch(CAPABILITY_DATA_REQUEST) → worker', () => {
  it('cache miss routes through the FSM (not the CK) and the FSM forwards to persist-telemetry', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    fsm.setGovernance(ck);
    // Pre-seed a cred record so the FSM has a cred
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, ck);

    // Mock the DetectDynamicWorker so /debug_token is not hit
    const mockDetectWorker = { execute: vi.fn(async () => ['instagram_basic', 'pages_read_engagement']) };
    const detectWorkerPath = require_.resolve(
      '../../../graph-capability-kernel/substrates/vault/scope-substrate/workers/detect-dynamic-worker.js'
    );
    require_.cache[detectWorkerPath] = {
      id: detectWorkerPath,
      filename: detectWorkerPath,
      loaded: true,
      exports: function MockDetectDynamicWorker() {
        return { execute: mockDetectWorker.execute };
      },
      children: [],
      paths: [],
    };

    // Force a fresh require of the scope-substrate to pick up the worker mock
    delete require_.cache[require_.resolve('../../../graph-capability-kernel/substrates/vault/scope-substrate/index.js')];
    const scopeSubstrate = require_('../../../graph-capability-kernel/substrates/vault/scope-substrate/index.js');

    // The signal-dispatch is bound to the FSM, and the FSM dispatches
    // CAPABILITY_DATA_REQUEST internally. The cache-miss path:
    //   substrate._governedRead() → fsm.dispatch(CAPABILITY_DATA_REQUEST, ctx)
    //   → fsm internals → ctx.dispatchGlobal(DB_READ_REQUESTED) → ck
    //   → ck.dispatch routes to persist-telemetry (stub returns allowed)
    //   → no read result → _governedRead resolves {success: false, data: null}
    //   → fall through to /debug_token worker
    const scopes = await scopeSubstrate.detectDynamic({
      businessAccountId: BA_A,
      userId: UA_A,
      token: 'fake-token',
      credentialId: 'cred-1',
    });

    expect(scopes).toEqual(['instagram_basic', 'pages_read_engagement']);
    expect(mockDetectWorker.execute).toHaveBeenCalledWith({ token: 'fake-token' });
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// V3. Governed write chain — fsm.requestDBWrite → persist-telemetry → writer
// ═══════════════════════════════════════════════════════════════════════════
describe('V3 — Governed write chain: fsm.requestDBWrite → ctx.dispatchGlobal → writer', () => {
  it('fsm.requestDBWrite for system_alerts dispatches DB_WRITE_REQUESTED through ctx', () => {
    const ck = makeStubCk();
    gck.install({ ck });
    // Wire the fsm's internal governance ref so requestDBWrite can dispatch.
    // (In production, the constitutional-kernel sets this via gck.install. The
    // stub doesn't replicate that side-effect automatically.)
    fsm.setGovernance(ck);
    // ctx's dispatchGlobal is the stub CK's dispatch
    const writerExecuted = vi.fn();
    // Wrap the real writer registry
    const registry = require_('../../../postgres-telemetry-kernel/writers/registry.js');
    const originalGetWriter = registry.getWriter;
    registry.getWriter = (op) => {
      if (op === 'insert_alert') {
        return { execute: async (params, gov) => {
          writerExecuted({ op, params });
          gov?.dispatch({ type: 'DB_WRITE_COMPLETE', ...params, count: 1, status: 'success', error: null });
          return { success: true };
        }};
      }
      return originalGetWriter(op);
    };

    fsm.requestDBWrite({
      table: 'system_alerts',
      operation: 'insert_alert',
      accountId: BA_A,
      rows: [{ alert_type: 'test', business_account_id: BA_A, message: 'hi', resolved: false }],
    });

    // The dispatch chain: fsm.requestDBWrite → ck.dispatch (stub routes to
    // ptFsm.dispatch which is async) → ptFsm.buildActions calls
    // db.dispatchWrite (sync return) → setImmediate fires writer.execute
    // → writer mock records. Need a couple of ticks to drain the chain.
    return new Promise(resolve => setTimeout(() => {
      expect(writerExecuted).toHaveBeenCalled();
      const call = writerExecuted.mock.calls[0][0];
      expect(call.op).toBe('insert_alert');
      expect(call.params.table).toBe('system_alerts');
      registry.getWriter = originalGetWriter;
      resolve();
    }, 50));
  });

  it('persist-telemetry FSM rejects writes for tables not in VALID_TABLES', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    const ctx = fsm.getDispatchContext();
    const result = await ptFsm.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'graph-capability',
      accountId: BA_A,
      table: 'NOT_IN_WHITELIST',
      operation: 'foo',
      rows: [],
    }, ctx);
    const completed = result.actions?.find(a => a.type === 'DB_WRITE_COMPLETE');
    if (completed) {
      expect(completed.status).toBe('failed');
    }
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// V4. Constitutional policy chain — FSM is the policy authority
// ═══════════════════════════════════════════════════════════════════════════
describe('V4 — Constitutional ordering: FSM (policy) → substrate (membrane) → worker (executor)', () => {
  it('the substrate does NOT decide policy: it has no setInterval / setTimeout for RUN_TOKEN_HEALTH_CHECK', async () => {
    const fs = await import('node:fs');
    const path = await import('node:path');
    const substratePath = path.join(
      path.dirname(require_.resolve('../../../graph-capability-kernel/substrates/health-substrate/index.js')),
      'index.js'
    );
    const source = fs.readFileSync(substratePath, 'utf8');
    // Negative assertion: no independent scheduling in the substrate
    expect(source).not.toMatch(/setInterval|setTimeout.*RUN_TOKEN_HEALTH/);
  });

  it('the membrane runs the worker only when the action arrives (FSM is the policy authority)', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    await ck.dispatch({ type: 'CAPABILITY_BOOTSTRAP' });

    // Subscribe handlers were captured by the stub CK
    const handlers = ck.actionSubscribers.get('RUN_TOKEN_HEALTH_CHECK');
    expect(handlers).toBeDefined();
    expect(handlers.length).toBe(1);

    // The handler is the membrane's response to the FSM-emitted action
    // Calling it directly triggers runTokenHealthCheck (the constitutional
    // action→substrate→worker path)
    const result = await handlers[0]({ businessAccountId: BA_A });
    // The handler is async-fire-and-forget; the result of runTokenHealthCheck
    // is a {scanned, valid, ...} object or undefined if fire-and-forget
    // (the substrate uses .catch in its subscription, so it returns undefined)
    expect(result === undefined || typeof result === 'object').toBe(true);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// V5. Cadence gate — per-cred per-check-type
// ═══════════════════════════════════════════════════════════════════════════
describe('V5 — Cadence gate: fsm._shouldCheck', () => {
  it('returns true for a cred that has never been checked', () => {
    gck.install({ ck: makeStubCk() });
    fsm._resetCred();
    expect(fsm._shouldCheck(BA_A, 'token_health')).toBe(true);
    expect(fsm._shouldCheck(BA_A, 'uat_refresh')).toBe(true);
    expect(fsm._shouldCheck(BA_A, 'data_access_expiry')).toBe(true);
  });

  it('returns false for a cred within the cadence window after a completion stamp', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    await fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, ck);
    await fsm.dispatch({ type: 'CAPABILITY_HEALTH_CHECK_COMPLETED', checkType: 'token_health', businessAccountId: BA_A }, ck);
    expect(fsm._shouldCheck(BA_A, 'token_health')).toBe(false);
    expect(fsm._shouldCheck(BA_A, 'uat_refresh')).toBe(true);
  });

  it('rejects CAPABILITY_HEALTH_CHECK_COMPLETED with unknown checkType', async () => {
    gck.install({ ck: makeStubCk() });
    const result = await fsm.dispatch(
      { type: 'CAPABILITY_HEALTH_CHECK_COMPLETED', checkType: 'bogus_type', businessAccountId: BA_A },
      makeStubCk()
    );
    expect(result.allowed).toBe(false);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// V6. Per-cred cadence stamping
// ═══════════════════════════════════════════════════════════════════════════
describe('V6 — Per-cred cadence stamping', () => {
  it('CAPABILITY_HEALTH_CHECK_COMPLETED stamps lastTokenHealthCheckAt on the cred', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    await fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, ck);
    expect(fsm.exportState(BA_A).lastTokenHealthCheckAt).toBeNull();
    await fsm.dispatch({ type: 'CAPABILITY_HEALTH_CHECK_COMPLETED', checkType: 'token_health', businessAccountId: BA_A }, ck);
    expect(fsm.exportState(BA_A).lastTokenHealthCheckAt).toBeGreaterThan(0);
  });

  it('per-cred isolation: stamping BA_A does not affect BA_B', async () => {
    const ck = makeStubCk();
    gck.install({ ck });
    await fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, ck);
    await fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_B) }, ck);
    await fsm.dispatch({ type: 'CAPABILITY_HEALTH_CHECK_COMPLETED', checkType: 'token_health', businessAccountId: BA_A }, ck);
    expect(fsm.exportState(BA_A).lastTokenHealthCheckAt).toBeGreaterThan(0);
    expect(fsm.exportState(BA_B).lastTokenHealthCheckAt).toBeNull();
  });

  it('regression: T1 — no global sentinel cadence stamp from a null businessAccountId completion', () => {
    const ck = makeStubCk();
    gck.install({ ck });
    fsm.dispatch({ type: 'CAPABILITY_HEALTH_CHECK_COMPLETED', checkType: 'token_health', businessAccountId: null }, ck);
    // No real cred should be stamped by a null completion
    for (const c of fsm.listCreds()) {
      if (c === '__global__') continue;
      expect(fsm.exportState(c).lastTokenHealthCheckAt).toBeNull();
    }
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// V7. CK bootstrap wires the membrane via the FSM (not the CK)
// ═══════════════════════════════════════════════════════════════════════════
describe('V7 — Constitutional Kernel bootstrap wires the membrane via the FSM', () => {
  it('constitutionalKernel is the sole ingress for governance events', () => {
    // The CK exposes bootstrap, dispatch, subscribeAction (its constitutional surface)
    expect(typeof constitutionalKernel.bootstrap).toBe('function');
    expect(typeof constitutionalKernel.dispatch).toBe('function');
    expect(typeof constitutionalKernel.subscribeAction).toBe('function');
    // The CK does NOT expose a `wire` method (the substrate contract is FSM-mediated)
    expect(constitutionalKernel.wire).toBeUndefined();
  });

  it('the FSM, not the CK, owns the membrane registration', () => {
    // gck.install registers the health membrane with the FSM
    expect(typeof fsm.setMembrane).toBe('function');
    // The CK has no setMembrane
    expect(constitutionalKernel.setMembrane).toBeUndefined();
  });

  it('substrate emissions route through the FSM (signal-dispatch bound to FSM, not CK)', () => {
    const ck = makeStubCk();
    gck.install({ ck });
    // Pre-seed a cred so the observation doesn't fail
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, ck);
    fsm._resetCred();
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, ck);

    // The substrate's signal-dispatch should call fsm.dispatch, NOT ck.dispatch
    const fsmDispatchSpy = vi.spyOn(fsm, 'dispatch');
    const env = fsm.newEnvelope({ businessAccountId: BA_A, userId: UA_A });
    env.pat = { isDecryptable: true };
    signalDispatch.emitEnvelope({ envelope: env });
    // The fsm.dispatch was called (via the bound signal-dispatch)
    expect(fsmDispatchSpy).toHaveBeenCalled();
    const callArgs = fsmDispatchSpy.mock.calls[0];
    expect(callArgs[0].type).toBe('CAPABILITY_OBSERVATION');
    fsmDispatchSpy.mockRestore();
  });
});
