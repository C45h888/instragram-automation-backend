/**
 * Governed Read — scope-substrate façade and its dependency on the
 * graph-capability FSM _governedRead helper.
 *
 * Production code under test:
 *   graph-capability-kernel/substrates/vault/scope-substrate/index.js
 *     - _governedRead(ck, businessAccountId, readDomain, params)
 *     - detectDynamic() pre-flight cache lookup → fallback to worker
 *
 * Strategy:
 *   - Mock signal-dispatch.getCk() so detectDynamic sees a stub CK
 *   - Mock the DetectDynamicWorker module so /debug_token is not hit
 *   - Drive _governedRead with a mock CK that wraps the real FSM
 *     (scenario 8) and a mock CK that rejects (scenario 9)
 *   - For detectDynamic, mock _governedRead's return value via the
 *     mock CK's dispatch and assert the cache-hit / cache-miss /
 *     timeout fallback paths
 */

import { describe, it, beforeEach, expect, vi } from 'vitest';
import { createRequire } from 'node:module';

const require_ = createRequire(import.meta.url);

// The scope-substrate façade requires '../signal-dispatch' (used in
// production via signalDispatch.getCk()) and the DetectDynamicWorker
// class (which transitively requires api-surface → axios). Both are
// mocked here so the real production code loads and runs unmodified.
import Module from 'node:module';
const realFsm = require_('../../../graph-capability-kernel/fsm.js');
const { createStubCk } = require_('../runtime/stub-ck.js');

// Mock signal-dispatch BEFORE importing scope-substrate
const mockSignalDispatch = {
  getCk: vi.fn(),
  emitEvaluate: vi.fn(),
  emitEnvelope: vi.fn(),
  bindCk: vi.fn(),
};
require_.cache[require_.resolve('../../../graph-capability-kernel/substrates/vault/signal-dispatch.js')] = {
  exports: mockSignalDispatch,
  id: 'signal-dispatch-mock',
  filename: 'signal-dispatch-mock',
  loaded: true,
  children: [],
  paths: [],
};

// Mock the DetectDynamicWorker class so axios/api-surface is never imported.
const mockDetectDynamicWorkerInstance = { execute: vi.fn() };
// vi.fn returns a plain function that can be `new`-ed but won't
// return our instance. Use a real class so `new MockDetectDynamicWorker()`
// returns the spy-backed instance.
class MockDetectDynamicWorker {
  constructor() {
    // Copy spy methods onto `this` so callers using the instance see them
    this.execute = mockDetectDynamicWorkerInstance.execute;
  }
}
const MockDetectDynamicWorkerSpy = vi.fn(MockDetectDynamicWorker);
require_.cache[
  require_.resolve('../../../graph-capability-kernel/substrates/vault/scope-substrate/workers/detect-dynamic-worker.js')
] = {
  exports: MockDetectDynamicWorkerSpy,
  id: 'detect-dynamic-worker-mock',
  filename: 'detect-dynamic-worker-mock',
  loaded: true,
  children: [],
  paths: [],
};

const scopeSubstrate = require_('../../../graph-capability-kernel/substrates/vault/scope-substrate/index.js');

function makeMockCk() {
  return {
    dispatch: vi.fn(),
  };
}

describe('Scope Substrate — _governedRead helper (driven via detectDynamic cache path)', () => {
  beforeEach(() => {
    realFsm._resetCred();
    vi.clearAllMocks();
  });

  it('SCENARIO 8 — detectDynamic cache hit dispatches CAPABILITY_DATA_REQUEST with correct shape and resolves via FSM', async () => {
    // Build a mockCk whose dispatch, on CAPABILITY_DATA_REQUEST, hands the
    // event to the real FSM and resolves the Promise controllers by
    // replaying a READ_RESULT_AVAILABLE through the FSM. detectDynamic's
    // _governedRead is the function under test — we exercise it via
    // detectDynamic's pre-flight branch (cache hit).
    const freshTimestamp = new Date().toISOString();
    const capturedEvents = [];
    const mockCk = {
      dispatch: vi.fn((event) => {
        capturedEvents.push(event);
        if (event.type === 'CAPABILITY_DATA_REQUEST') {
          return realFsm.dispatch(event, {
            dispatchGlobal: (sub) => {
              if (sub.type === 'DB_READ_REQUESTED') {
                realFsm.dispatch(
                  {
                    type: 'READ_RESULT_AVAILABLE',
                    businessAccountId: event.businessAccountId,
                    accountId: event.businessAccountId,
                    readId: event.readId,
                    readDomain: event.readDomain,
                    data: { scope_cache: ['scope1'], scope_cache_updated_at: freshTimestamp },
                  },
                  { dispatchGlobal: vi.fn(), validate: () => ({ allowed: true }) }
                );
              }
            },
            validate: () => ({ allowed: true }),
          });
        }
        return { allowed: true };
      }),
    };
    mockSignalDispatch.getCk.mockReturnValue(mockCk);

    const scopes = await scopeSubstrate.detectDynamic({
      businessAccountId: 'ba-1',
      userId: 'u1',
      token: 'fake-token',
      credentialId: 'c1',
    });

    expect(scopes).toEqual(['scope1']);

    // _governedRead dispatched exactly one CAPABILITY_DATA_REQUEST
    // with the expected shape:
    const reqEvent = capturedEvents.find((e) => e.type === 'CAPABILITY_DATA_REQUEST');
    expect(reqEvent).toBeDefined();
    expect(reqEvent.readDomain).toBe('db.scope-cache');
    expect(reqEvent.businessAccountId).toBe('ba-1');
    expect(reqEvent.readId).toMatch(/^[0-9a-f-]{36}$/);
    expect(reqEvent.source).toBe('scope-substrate');
    expect(reqEvent.params).toEqual({ credentialId: 'c1' });
    expect(typeof reqEvent._resolve).toBe('function');
    expect(typeof reqEvent._reject).toBe('function');
  });

  it('SCENARIO 9 — _governedRead handles dispatch rejection: cache-miss falls through, no crash', async () => {
    // MockCk that returns { allowed: false, reason: 'guard blocked' }
    // on CAPABILITY_DATA_REQUEST. detectDynamic's pre-flight branch
    // will receive { success: false } from _governedRead, treat it as
    // a cache miss, and fall through to the /debug_token worker.
    const mockCk = {
      dispatch: vi.fn((event) => {
        if (event.type === 'CAPABILITY_DATA_REQUEST') {
          return { allowed: false, reason: 'guard blocked' };
        }
        return { allowed: true };
      }),
    };
    mockSignalDispatch.getCk.mockReturnValue(mockCk);
    mockDetectDynamicWorkerInstance.execute.mockResolvedValue(['scope-fallback']);

    const scopes = await scopeSubstrate.detectDynamic({
      businessAccountId: 'ba-2',
      userId: 'u2',
      token: 'fake-token',
      credentialId: 'c2',
    });

    // Cache-miss fallback path: worker was called, returned scopes
    expect(scopes).toEqual(['scope-fallback']);
    expect(mockDetectDynamicWorkerInstance.execute).toHaveBeenCalledWith({ token: 'fake-token' });

    // The CAPABILITY_DATA_REQUEST was dispatched but rejected
    const reqCall = mockCk.dispatch.mock.calls.find((c) => c[0].type === 'CAPABILITY_DATA_REQUEST');
    expect(reqCall).toBeDefined();
  });
});

describe('Scope Substrate — detectDynamic()', () => {
  beforeEach(() => {
    realFsm._resetCred();
    vi.clearAllMocks();
    mockDetectDynamicWorkerInstance.execute.mockReset();
  });

  it('SCENARIO 10 — cache hit returns scopes from governed read, no /debug_token call', async () => {
    // Make signalDispatch.getCk() return a mock CK that resolves
    // _governedRead's underlying CAPABILITY_DATA_REQUEST with a cache hit
    const freshTimestamp = new Date().toISOString();
    const mockCk = {
      dispatch: vi.fn((event) => {
        if (event.type !== 'CAPABILITY_DATA_REQUEST') return { allowed: true };
        return realFsm.dispatch(event, {
          dispatchGlobal: (sub) => {
            if (sub.type === 'DB_READ_REQUESTED') {
              realFsm.dispatch(
                {
                  type: 'READ_RESULT_AVAILABLE',
                  businessAccountId: event.businessAccountId,
                  accountId: event.businessAccountId,
                  readId: event.readId,
                  readDomain: event.readDomain,
                  data: { scope_cache: ['scope1'], scope_cache_updated_at: freshTimestamp },
                },
                { dispatchGlobal: vi.fn(), validate: () => ({ allowed: true }) }
              );
            }
          },
          validate: () => ({ allowed: true }),
        });
      }),
    };
    mockSignalDispatch.getCk.mockReturnValue(mockCk);

    const scopes = await scopeSubstrate.detectDynamic({
      businessAccountId: 'ba-1',
      userId: 'u1',
      token: 'fake-token',
      credentialId: 'c1',
    });

    expect(scopes).toEqual(['scope1']);
    // DetectDynamicWorker was NOT called (cache hit)
    expect(MockDetectDynamicWorkerSpy).not.toHaveBeenCalled();
    expect(mockDetectDynamicWorkerInstance.execute).not.toHaveBeenCalled();
    // No DB_WRITE_REQUESTED was dispatched (cache hit path skips write)
    const writeDispatched = mockCk.dispatch.mock.calls.some((c) => c[0].type === 'DB_WRITE_REQUESTED');
    expect(writeDispatched).toBe(false);
  });

  it('SCENARIO 11 — cache miss falls through to /debug_token worker and dispatches DB_WRITE_REQUESTED', async () => {
    // Use stub-ck so DB_WRITE_REQUESTED is routed to persist-telemetry (F2b fix)
    const { stubCk, teardown } = createStubCk({ realFsm });
    mockSignalDispatch.getCk.mockReturnValue(stubCk);

    // Worker returns scopes
    mockDetectDynamicWorkerInstance.execute.mockResolvedValue(['scope2']);

    const scopes = await scopeSubstrate.detectDynamic({
      businessAccountId: 'ba-1',
      userId: 'u1',
      token: 'fake-token',
      credentialId: 'c1',
    });

    expect(scopes).toEqual(['scope2']);
    // Worker was called
    expect(mockDetectDynamicWorkerInstance.execute).toHaveBeenCalledWith({ token: 'fake-token' });
    // emitEvaluate was called
    expect(mockSignalDispatch.emitEvaluate).toHaveBeenCalledWith({
      businessAccountId: 'ba-1',
      userId: 'u1',
      source: 'vault.scope.detectDynamic',
    });
    // DB_WRITE_REQUESTED was dispatched to write the cache
    const writeCall = stubCk.findEvents('DB_WRITE_REQUESTED')[0];
    expect(writeCall).toBeDefined();
    expect(writeCall.table).toBe('instagram_credentials');
    expect(writeCall.operation).toBe('write_scope_cache');
    expect(writeCall.accountId).toBe('ba-1');
    teardown();
  });

  it('SCENARIO 12 — governed read timeout falls through to /debug_token worker', async () => {
    // Mock CK that throws (timeout simulation) on CAPABILITY_DATA_REQUEST
    const mockCk = {
      dispatch: vi.fn((event) => {
        if (event.type === 'CAPABILITY_DATA_REQUEST') {
          // Simulate timeout: do NOT invoke the FSM at all, return
          // a guard rejection so the Promise resolves with an error.
          // The _governedRead code resolves with { success: false, ... }
          // in that case — but detectDynamic wraps _governedRead in a
          // try/catch and falls through. We need it to REJECT, so we
          // make dispatch throw instead.
          throw new Error('Read timed out after 15s');
        }
        return { allowed: true };
      }),
    };
    mockSignalDispatch.getCk.mockReturnValue(mockCk);

    // Worker returns scopes
    mockDetectDynamicWorkerInstance.execute.mockResolvedValue(['scope3']);

    const scopes = await scopeSubstrate.detectDynamic({
      businessAccountId: 'ba-1',
      userId: 'u1',
      token: 'fake-token',
      credentialId: 'c1',
    });

    expect(scopes).toEqual(['scope3']);
    // Worker was called (governed read failed → fell through)
    expect(mockDetectDynamicWorkerInstance.execute).toHaveBeenCalledWith({ token: 'fake-token' });
  });
});
