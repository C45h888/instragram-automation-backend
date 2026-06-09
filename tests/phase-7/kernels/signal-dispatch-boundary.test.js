// ============================================
// Signal-Dispatch Authority Boundary — Unit Tests (Layer 1)
// ============================================
// Phase D rewiring: signal-dispatch is the SINGLE authority boundary for
// substrate emissions. The substrate's emit* calls now route through the
// FSM (the constitutional ingress), not the CK. The CK is downstream of
// the FSM in the observation direction (for cross-domain work).
//
// New contract:
//   - bindFsm(fsm, ctx) — canonical binding
//   - getFsm() — read the bound FSM
//   - getCtx() — read the dispatch ctx
//   - emit* → fsm.dispatch(event, ctx) (not ck.dispatch)
//   - bindCk/getCk — no-op retained for import-compatibility
//
// Scope: signal-dispatch module only. No runtime simulator required.
// ============================================

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const signalDispatch = require('../../../graph-capability-kernel/substrates/vault/signal-dispatch');
const fsm = require('../../../graph-capability-kernel/fsm');

function makeFakeCk() {
  return {
    dispatch: vi.fn(() => ({ allowed: true })),
    validateDomainTransition: () => ({ allowed: true }),
    getState: () => 'HEALTHY',
  };
}

function makeCtx() {
  return {
    validate: () => ({ allowed: true }),
    dispatchGlobal: vi.fn(),
    getGlobalState: () => 'HEALTHY',
  };
}

describe('Signal-Dispatch Authority Boundary (Layer 1, Phase D)', () => {
  let fakeCk;
  let ctx;
  let fsmDispatchSpy;
  let warnSpy;

  beforeEach(() => {
    fakeCk = makeFakeCk();
    ctx = makeCtx();
    fsm._resetCred();
    // Spy on fsm.dispatch to observe substrate emissions
    fsmDispatchSpy = vi.spyOn(fsm, 'dispatch');
    warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    signalDispatch.bindFsm(null, null);
    fsmDispatchSpy.mockRestore();
    warnSpy.mockRestore();
  });

  describe('Pre-install: no FSM bound', () => {
    it('emitEvaluate returns undefined and warns once', () => {
      const r = signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', userId: 'U-1', source: 'test' });
      expect(r).toBeUndefined();
      // fsm.dispatch was NOT called
      expect(fsmDispatchSpy).not.toHaveBeenCalled();
      expect(warnSpy).toHaveBeenCalledTimes(1);
    });

    it('emits are warn-once-latched (multiple calls produce one warning)', () => {
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'a' });
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-2', source: 'b' });
      signalDispatch.emitNewAccountConnected({ businessAccountId: 'BA-1' });
      signalDispatch.emitTokenRefreshed({ businessAccountId: 'BA-1' });
      expect(warnSpy).toHaveBeenCalledTimes(1);
      expect(fsmDispatchSpy).not.toHaveBeenCalled();
    });
  });

  describe('Install: FSM bound', () => {
    it('bindFsm(fsm, ctx) binds FSM into signal-dispatch', () => {
      signalDispatch.bindFsm(fsm, ctx);
      expect(signalDispatch.getFsm()).toBe(fsm);
      expect(signalDispatch.getCtx()).toBe(ctx);
    });

    it('emitEvaluate routes through the bound FSM', () => {
      signalDispatch.bindFsm(fsm, ctx);
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', userId: 'U-1', source: 'test.post-install' });
      expect(fsmDispatchSpy).toHaveBeenCalledTimes(1);
      const event = fsmDispatchSpy.mock.calls[0][0];
      expect(event.type).toBe('CAPABILITY_EVALUATE');
      expect(event.source).toBe('test.post-install');
    });

    it('emitNewAccountConnected routes through the bound FSM', () => {
      signalDispatch.bindFsm(fsm, ctx);
      signalDispatch.emitNewAccountConnected({ businessAccountId: 'BA-1', userId: 'U-1' });
      expect(fsmDispatchSpy).toHaveBeenCalledTimes(1);
      expect(fsmDispatchSpy.mock.calls[0][0].type).toBe('NEW_ACCOUNT_CONNECTED');
    });

    it('emitTokenRefreshed routes through the bound FSM', () => {
      signalDispatch.bindFsm(fsm, ctx);
      signalDispatch.emitTokenRefreshed({ businessAccountId: 'BA-1', userId: 'U-1' });
      expect(fsmDispatchSpy).toHaveBeenCalledTimes(1);
      expect(fsmDispatchSpy.mock.calls[0][0].type).toBe('TOKEN_REFRESHED');
    });

    it('emitEnvelope routes through the bound FSM as CAPABILITY_OBSERVATION', () => {
      signalDispatch.bindFsm(fsm, ctx);
      const env = fsm.newEnvelope({ businessAccountId: 'BA-1' });
      env.pat = { isDecryptable: true };
      signalDispatch.emitEnvelope({ envelope: env });
      expect(fsmDispatchSpy).toHaveBeenCalledTimes(1);
      const event = fsmDispatchSpy.mock.calls[0][0];
      expect(event.type).toBe('CAPABILITY_OBSERVATION');
      expect(event.envelope).toBe(env);
    });

    it('explicit fsm param overrides the bound FSM', () => {
      signalDispatch.bindFsm(fsm, ctx);
      const otherFsm = { dispatch: vi.fn(() => ({ allowed: true })) };
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', fsm: otherFsm, source: 'test' });
      expect(otherFsm.dispatch).toHaveBeenCalledTimes(1);
      // The bound fsm was NOT called
      expect(fsmDispatchSpy).not.toHaveBeenCalled();
    });

    it('emissions include the ctx as the second argument to fsm.dispatch', () => {
      signalDispatch.bindFsm(fsm, ctx);
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' });
      const callArgs = fsmDispatchSpy.mock.calls[0];
      expect(callArgs[1]).toBe(ctx);
    });
  });

  describe('Idempotency', () => {
    it('re-install with the same FSM is idempotent (no error)', () => {
      signalDispatch.bindFsm(fsm, ctx);
      expect(() => signalDispatch.bindFsm(fsm, ctx)).not.toThrow();
      expect(signalDispatch.getFsm()).toBe(fsm);
    });

    it('re-install with a different FSM replaces the binding', () => {
      signalDispatch.bindFsm(fsm, ctx);
      const otherFsm = { dispatch: vi.fn(() => ({ allowed: true })) };
      signalDispatch.bindFsm(otherFsm, ctx);
      expect(signalDispatch.getFsm()).toBe(otherFsm);
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' });
      expect(otherFsm.dispatch).toHaveBeenCalledTimes(1);
      expect(fsmDispatchSpy).not.toHaveBeenCalled();
    });
  });

  describe('Uninstall: binding released', () => {
    it('bindFsm(null, null) releases the FSM binding', () => {
      signalDispatch.bindFsm(fsm, ctx);
      signalDispatch.bindFsm(null, null);
      expect(signalDispatch.getFsm()).toBeNull();
      expect(signalDispatch.getCtx()).toBeNull();
    });

    it('post-uninstall emits warn again (warning latch reset on real install)', () => {
      signalDispatch.bindFsm(fsm, ctx);
      signalDispatch.bindFsm(null, null);
      warnSpy.mockClear();
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' });
      expect(fsmDispatchSpy).not.toHaveBeenCalled();
      expect(warnSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('Error handling: never throws to caller', () => {
    it('fsm.dispatch throwing is swallowed and warn is logged', () => {
      const throwingFsm = { dispatch: () => { throw new Error('fsm offline'); } };
      signalDispatch.bindFsm(throwingFsm, ctx);
      expect(() => signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' })).not.toThrow();
    });
  });

  describe('Legacy CK binding is a no-op (kept for import-compatibility)', () => {
    it('bindCk is a no-op — does not wire emissions to CK and does not restore a fsm binding', () => {
      // Start with a bound FSM
      signalDispatch.bindFsm(fsm, ctx);
      expect(signalDispatch.getFsm()).toBe(fsm);
      // Call bindCk — should not change anything
      signalDispatch.bindCk(fakeCk);
      // The FSM is still bound
      expect(signalDispatch.getFsm()).toBe(fsm);
      // getCk returns null (the old contract is no longer supported)
      expect(signalDispatch.getCk()).toBeNull();
      // Emissions still go to the FSM, not the CK
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' });
      expect(fakeCk.dispatch).not.toHaveBeenCalled();
      // The bound fsm received the emission
      expect(fsmDispatchSpy).toHaveBeenCalledTimes(1);
    });

    it('unbind via bindFsm(null, null) — bindCk is truly inert (does not un-latch warn)', () => {
      // First: bind fsm, then unbind — no warn yet
      signalDispatch.bindFsm(fsm, ctx);
      signalDispatch.bindFsm(null, null);
      // First emit after unbind: warns once
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'first' });
      expect(warnSpy).toHaveBeenCalledTimes(1);
      // Now call bindCk — should NOT reset the warn latch
      signalDispatch.bindCk(fakeCk);
      // The warn was already issued; the latch is still set
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'second' });
      // Still 1 (latched — bindCk did not un-latch)
      expect(warnSpy).toHaveBeenCalledTimes(1);
      // Re-binding the fsm resets the latch
      signalDispatch.bindFsm(fsm, ctx);
      warnSpy.mockClear();
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'after-refsm' });
      // No warn this time — fsm is bound
      expect(warnSpy).not.toHaveBeenCalled();
    });
  });
});
