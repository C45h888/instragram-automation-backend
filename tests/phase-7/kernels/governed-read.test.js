/**
 * Governed Read — graph-capability FSM transitions for the
 * CAPABILITY_DATA_REQUEST / READ_RESULT_AVAILABLE / CAPABILITY_DATA_TIMEOUT
 * chain.
 *
 * Production code under test:
 *   graph-capability-kernel/fsm.js
 *     - CAPABILITY_DATA_REQUEST transition (guard + buildActions)
 *     - READ_RESULT_AVAILABLE transition (resolves pending Promise)
 *     - CAPABILITY_DATA_TIMEOUT transition (rejects pending Promise)
 *
 * Strategy: import the real FSM module, drive it via fsm.dispatch(),
 * observe side effects through the public API only:
 *   - ctx.dispatchGlobal spy  (verifies DB_READ_REQUESTED routing)
 *   - mockResolve / mockReject (verifies Promise resolution/rejection)
 *   - result.actions           (verifies emitted DATA_AVAILABLE)
 *   - result.reason            (verifies guard rejections)
 *   - A follow-up dispatch that returns 'no pending read for r1'
 *     is the proof that the prior pendingReads entry was cleaned up.
 *
 * The FSM does not export _credRecord — pendingReads is private state.
 * Observable cleanup is verified by the next dispatch's guard result.
 */

import { describe, it, beforeEach, expect, vi } from 'vitest';
import { createRequire } from 'node:module';

const require_ = createRequire(import.meta.url);
const fsm = require_('../../../graph-capability-kernel/fsm.js');

function makeCtx() {
  return {
    dispatchGlobal: vi.fn(),
    validate: vi.fn(() => ({ allowed: true })),
  };
}

describe('Governed Read — graph-capability FSM transitions', () => {
  beforeEach(() => {
    // Clear the entire per-cred map between tests
    fsm._resetCred();
  });

  it('SCENARIO 1 — CAPABILITY_DATA_REQUEST routes DB_READ_REQUESTED and tracks the read (resolvable on completion)', () => {
    const ctx = makeCtx();
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    const result = fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        businessAccountId: 'ba-1',
        readDomain: 'db.scope-cache',
        readId: 'r1',
        params: { credentialId: 'c1' },
        source: 'scope-substrate',
        _resolve: mockResolve,
        _reject: mockReject,
      },
      ctx
    );

    expect(result.allowed).toBe(true);
    expect(result.actions).toEqual([]);

    // ctx.dispatchGlobal was called with the routed DB_READ_REQUESTED
    // (the FSM now attaches lineageId/lineageDomain per the
    // canonical-source gate; assertions must accept these extra fields)
    expect(ctx.dispatchGlobal).toHaveBeenCalledTimes(1);
    expect(ctx.dispatchGlobal).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'DB_READ_REQUESTED',
        readDomain: 'db.scope-cache',
        accountId: 'ba-1',
        readId: 'r1',
        params: { credentialId: 'c1' },
      })
    );

    // Proof the read was tracked: dispatch READ_RESULT_AVAILABLE and
    // assert the Promise controller is invoked. If the FSM had not
    // stored the controllers, the guard would reject with
    // 'no pending read for r1'.
    fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-1',
        accountId: 'ba-1',
        readId: 'r1',
        readDomain: 'db.scope-cache',
        data: { scope_cache: ['x'] },
      },
      ctx
    );
    expect(mockResolve).toHaveBeenCalledWith({
      success: true,
      data: { scope_cache: ['x'] },
      error: null,
      readDomain: 'db.scope-cache',
    });
    expect(mockReject).not.toHaveBeenCalled();
  });

  it('SCENARIO 2 — CAPABILITY_DATA_REQUEST guard rejects when fields are missing', () => {
    const ctx = makeCtx();
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    // No readDomain, no businessAccountId, no readId
    const result = fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        _resolve: mockResolve,
        _reject: mockReject,
      },
      ctx
    );

    expect(result.allowed).toBe(false);
    expect(result.reason).toMatch(/required/);
    expect(ctx.dispatchGlobal).not.toHaveBeenCalled();
  });

  it('SCENARIO 3 — CAPABILITY_DATA_REQUEST succeeds when a fresh cred record is lazy-created', () => {
    // The guard's "no cred record" path is unreachable in the current
    // FSM: _resolveCred always lazy-creates a record. This test pins
    // the observable contract — a fresh baId still produces an
    // allowed dispatch (proving the FSM created and stored the
    // pendingReads entry).
    const ctx = makeCtx();
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    const result = fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        businessAccountId: 'ba-fresh',
        readDomain: 'db.scope-cache',
        readId: 'r-fresh',
        _resolve: mockResolve,
        _reject: mockReject,
      },
      ctx
    );
    expect(result.allowed).toBe(true);
    // follow-up dispatch resolves the read
    fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-fresh',
        accountId: 'ba-fresh',
        readId: 'r-fresh',
        readDomain: 'db.scope-cache',
        data: { scope_cache: [] },
      },
      ctx
    );
    expect(mockResolve).toHaveBeenCalledTimes(1);
  });

  it('SCENARIO 4 — READ_RESULT_AVAILABLE resolves pending read and emits DATA_AVAILABLE', () => {
    const ctx = makeCtx();
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    // Set up a pending read via CAPABILITY_DATA_REQUEST, then resolve it
    fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        businessAccountId: 'ba-1',
        readDomain: 'db.scope-cache',
        readId: 'r1',
        params: {},
        source: 'scope-substrate',
        _resolve: mockResolve,
        _reject: mockReject,
      },
      ctx
    );

    const result = fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-1',
        accountId: 'ba-1',
        readId: 'r1',
        readDomain: 'db.scope-cache',
        data: { scope_cache: ['scope1'] },
      },
      ctx
    );

    // Promise was resolved with success
    expect(mockResolve).toHaveBeenCalledTimes(1);
    expect(mockResolve).toHaveBeenCalledWith({
      success: true,
      data: { scope_cache: ['scope1'] },
      error: null,
      readDomain: 'db.scope-cache',
    });
    expect(mockReject).not.toHaveBeenCalled();

    // The transition emitted a DATA_AVAILABLE action for observability
    expect(result.actions).toEqual([
      expect.objectContaining({
        type: 'DATA_AVAILABLE',
        readDomain: 'db.scope-cache',
        readId: 'r1',
        data: { scope_cache: ['scope1'] },
        error: null,
        source: 'scope-substrate',
        businessAccountId: 'ba-1',
      }),
    ]);

    // Proof of cleanup: a second READ_RESULT_AVAILABLE for r1 must be
    // rejected by the guard with 'no pending read for r1'.
    const second = fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-1',
        accountId: 'ba-1',
        readId: 'r1',
        readDomain: 'db.scope-cache',
        data: { scope_cache: ['x'] },
      },
      ctx
    );
    expect(second.allowed).toBe(false);
    expect(second.reason).toBe('no pending read for r1');
  });

  it('SCENARIO 5 — READ_RESULT_AVAILABLE with error resolves with success=false', () => {
    const ctx = makeCtx();
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        businessAccountId: 'ba-1',
        readDomain: 'db.scope-cache',
        readId: 'r1',
        params: {},
        source: 'scope-substrate',
        _resolve: mockResolve,
        _reject: mockReject,
      },
      ctx
    );

    fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-1',
        accountId: 'ba-1',
        readId: 'r1',
        readDomain: 'db.scope-cache',
        data: null,
        error: 'supabase_unavailable',
      },
      ctx
    );

    expect(mockResolve).toHaveBeenCalledWith({
      success: false,
      data: null,
      error: 'supabase_unavailable',
      readDomain: 'db.scope-cache',
    });
    expect(mockReject).not.toHaveBeenCalled();
  });

  it('SCENARIO 6 — READ_RESULT_AVAILABLE guard rejects when no pending read exists', () => {
    const ctx = makeCtx();

    const result = fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-1',
        readId: 'r1',
        readDomain: 'db.scope-cache',
        data: { scope_cache: ['x'] },
      },
      ctx
    );

    expect(result.allowed).toBe(false);
    expect(result.reason).toBe('no pending read for r1');
  });

  it('SCENARIO 7 — CAPABILITY_DATA_TIMEOUT rejects the pending read and cleans up', () => {
    const ctx = makeCtx();
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        businessAccountId: 'ba-1',
        readDomain: 'db.scope-cache',
        readId: 'r1',
        params: {},
        source: 'scope-substrate',
        _resolve: mockResolve,
        _reject: mockReject,
      },
      ctx
    );

    fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_TIMEOUT',
        businessAccountId: 'ba-1',
        readId: 'r1',
      },
      ctx
    );

    expect(mockReject).toHaveBeenCalledTimes(1);
    expect(mockReject.mock.calls[0][0]).toBeInstanceOf(Error);
    expect(mockReject.mock.calls[0][0].message).toBe('Read timed out');
    expect(mockResolve).not.toHaveBeenCalled();

    // Proof of cleanup: subsequent READ_RESULT_AVAILABLE for r1 must
    // be rejected with 'no pending read for r1'.
    const second = fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-1',
        readId: 'r1',
        readDomain: 'db.scope-cache',
        data: { scope_cache: ['x'] },
      },
      ctx
    );
    expect(second.allowed).toBe(false);
    expect(second.reason).toBe('no pending read for r1');
  });

  it('SCENARIO 15 — concurrent reads on different creds resolve independently', () => {
    const ctx = makeCtx();
    const resA = vi.fn();
    const rejA = vi.fn();
    const resB = vi.fn();
    const rejB = vi.fn();

    fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        businessAccountId: 'ba-1',
        readDomain: 'db.scope-cache',
        readId: 'r1',
        params: {},
        source: 'scope-substrate',
        _resolve: resA,
        _reject: rejA,
      },
      ctx
    );
    fsm.dispatch(
      {
        type: 'CAPABILITY_DATA_REQUEST',
        businessAccountId: 'ba-2',
        readDomain: 'db.scope-cache',
        readId: 'r2',
        params: {},
        source: 'scope-substrate',
        _resolve: resB,
        _reject: rejB,
      },
      ctx
    );

    // Resolve r1 on ba-1
    fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-1',
        accountId: 'ba-1',
        readId: 'r1',
        readDomain: 'db.scope-cache',
        data: { scope_cache: ['A'] },
      },
      ctx
    );

    // Resolve r2 on ba-2
    fsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId: 'ba-2',
        accountId: 'ba-2',
        readId: 'r2',
        readDomain: 'db.scope-cache',
        data: { scope_cache: ['B'] },
      },
      ctx
    );

    expect(resA).toHaveBeenCalledWith(expect.objectContaining({ data: { scope_cache: ['A'] } }));
    expect(resB).toHaveBeenCalledWith(expect.objectContaining({ data: { scope_cache: ['B'] } }));
    expect(rejA).not.toHaveBeenCalled();
    expect(rejB).not.toHaveBeenCalled();

    // Proof both were cleaned up
    const secondA = fsm.dispatch(
      { type: 'READ_RESULT_AVAILABLE', businessAccountId: 'ba-1', readId: 'r1', readDomain: 'db.scope-cache', data: {} },
      ctx
    );
    const secondB = fsm.dispatch(
      { type: 'READ_RESULT_AVAILABLE', businessAccountId: 'ba-2', readId: 'r2', readDomain: 'db.scope-cache', data: {} },
      ctx
    );
    expect(secondA.reason).toBe('no pending read for r1');
    expect(secondB.reason).toBe('no pending read for r2');
  });
});
