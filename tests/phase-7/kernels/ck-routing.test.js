/**
 * Governed Read — Constitutional Kernel gate + routing.
 *
 * Production code under test:
 *   control-plane/governance/constitutional-kernel.js
 *     - INTERNAL_DOMAIN_EVENTS whitelist
 *     - DOMAIN_EVENT_MAP routing
 *     - dispatch() canonical source gate
 *
 * Strategy: drive the REAL CK. Register only the graph-capability FSM
 * (not persist-telemetry, so the reading-substrate require chain
 * is not pulled in — that path is tested separately). Assertions
 * are made via observable side effects on the FSM, not via spies
 * (since `vi.spyOn` on a CJS module export reference is fragile).
 */

import { describe, it, beforeEach, expect, vi } from 'vitest';
import { createRequire } from 'node:module';

const require_ = createRequire(import.meta.url);
const ck = require_('../../../control-plane/governance/constitutional-kernel.js');
const fsm = require_('../../../graph-capability-kernel/fsm.js');

describe('Constitutional Kernel — gate + routing for governed read', () => {
  beforeEach(() => {
    fsm._resetCred();
    ck.registerDomain(fsm);
  });

  it('SCENARIO 13 — INTERNAL_DOMAIN_EVENTS lets CAPABILITY_DATA_REQUEST pass the canonical source gate (no lineageId needed)', () => {
    // CAPABILITY_DATA_REQUEST is in INTERNAL_DOMAIN_EVENTS so the
    // canonical source gate must let it through without a lineageId.
    // The FSM has a transition for it that creates a pendingReads
    // entry — observable side effect proves the event reached the FSM.
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    const result = ck.dispatch({
      type: 'CAPABILITY_DATA_REQUEST',
      businessAccountId: 'ba-1',
      readDomain: 'db.scope-cache',
      readId: 'r-gate-1',
      params: { credentialId: 'c1' },
      source: 'scope-substrate',
      _resolve: mockResolve,
      _reject: mockReject,
    });

    // Gate passed and FSM accepted (guard satisfied)
    expect(result.allowed).toBe(true);

    // Observable proof the FSM received the event: a follow-up
    // READ_RESULT_AVAILABLE resolves the stored Promise controllers.
    ck.dispatch({
      type: 'READ_RESULT_AVAILABLE',
      businessAccountId: 'ba-1',
      accountId: 'ba-1',
      readId: 'r-gate-1',
      readDomain: 'db.scope-cache',
      data: { scope_cache: ['x'] },
    });
    expect(mockResolve).toHaveBeenCalledWith({
      success: true,
      data: { scope_cache: ['x'] },
      error: null,
      readDomain: 'db.scope-cache',
    });
  });

  it('SCENARIO 13b — A non-internal domain event without lineageId IS rejected by the canonical source gate', () => {
    // NEW_ACCOUNT_CONNECTED is in DOMAIN_EVENT_MAP but NOT in
    // INTERNAL_DOMAIN_EVENTS — must require a lineageId. The proof
    // that the gate (not the FSM) is the rejection point: a
    // non-internal event reaches the gate even before the FSM
    // consults its transition map.
    const result = ck.dispatch({
      type: 'NEW_ACCOUNT_CONNECTED',
      businessAccountId: 'ba-1',
      userId: 'u1',
    });

    expect(result.allowed).toBe(false);
    expect(result.reason).toMatch(/canonical source required/);
  });

  it('SCENARIO 14 — DOMAIN_EVENT_MAP routes CAPABILITY_DATA_REQUEST to the graph-capability FSM only', () => {
    // Dispatch CAPABILITY_DATA_REQUEST through CK. The graph-capability
    // FSM is the only registered domain in this test. Observable
    // side effect: the FSM stores the Promise controllers in
    // pendingReads (proven by resolving via READ_RESULT_AVAILABLE).
    // If routing went to any other domain, the side effect wouldn't
    // happen — there is no other domain FSM registered.
    const mockResolve = vi.fn();
    const mockReject = vi.fn();

    const result = ck.dispatch({
      type: 'CAPABILITY_DATA_REQUEST',
      businessAccountId: 'ba-1',
      readDomain: 'db.scope-cache',
      readId: 'r-route-1',
      params: { credentialId: 'c1' },
      source: 'scope-substrate',
      _resolve: mockResolve,
      _reject: mockReject,
    });

    expect(result.allowed).toBe(true);

    // Route confirmation: the event reached the graph-capability FSM
    // because the Promise controllers are storable / resolvable.
    ck.dispatch({
      type: 'READ_RESULT_AVAILABLE',
      businessAccountId: 'ba-1',
      accountId: 'ba-1',
      readId: 'r-route-1',
      readDomain: 'db.scope-cache',
      data: { scope_cache: ['routed'] },
    });
    expect(mockResolve).toHaveBeenCalledWith(
      expect.objectContaining({ data: { scope_cache: ['routed'] } })
    );
  });
});
