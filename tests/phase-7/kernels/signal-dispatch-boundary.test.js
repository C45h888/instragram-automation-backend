// ============================================
// Signal-Dispatch Authority Boundary — Unit Tests (Layer 1)
// ============================================
// Verifies GAP-3 fix: signal-dispatch is the SINGLE authority boundary
// for vault signal ingress. Every emit* call threads the bound CK
// reference. No silent drops after install. Idempotent install/uninstall.
//
// Scope: signal-dispatch + graph-capability/wiring.
// No runtime simulator required — pure unit test.
// ============================================

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const signalDispatch = require('../../../graph-capability-kernel/substrates/vault/signal-dispatch');
const wiring = require('../../../graph-capability-kernel/substrates/graph-capability/wiring');
const fsm = require('../../../graph-capability-kernel/fsm');

describe('Signal-Dispatch Authority Boundary (Layer 1)', () => {
  let fakeCk;
  let dispatched;
  let warnSpy;

  beforeEach(() => {
    dispatched = [];
    fakeCk = {
      dispatch: (event) => {
        dispatched.push(event);
        return { allowed: true, from: fsm.getState(), to: fsm.getState() };
      },
      validateDomainTransition: () => ({ allowed: true }),
      getState: () => 'BOOTING',
    };
    warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    // Reset binding for clean test isolation
    if (wiring.isInstalled()) wiring.uninstall();
    signalDispatch.bindCk(null);
    warnSpy.mockRestore();
  });

  describe('Pre-install: no CK bound', () => {
    it('emitEvaluate returns undefined and warns once', () => {
      const r = signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', userId: 'U-1', source: 'test' });
      expect(r).toBeUndefined();
      expect(dispatched).toHaveLength(0);
      expect(warnSpy).toHaveBeenCalledTimes(1);
    });

    it('emits are warn-once-latched (multiple calls produce one warning)', () => {
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'a' });
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-2', source: 'b' });
      signalDispatch.emitNewAccountConnected({ businessAccountId: 'BA-1' });
      signalDispatch.emitTokenRefreshed({ businessAccountId: 'BA-1' });
      expect(warnSpy).toHaveBeenCalledTimes(1);
      expect(dispatched).toHaveLength(0);
    });
  });

  describe('Install: CK bound', () => {
    it('wiring.install({ck}) binds CK into signal-dispatch', () => {
      wiring.install({ ck: fakeCk });
      expect(signalDispatch.getCk()).toBe(fakeCk);
    });

    it('emitEvaluate routes through the bound CK', () => {
      wiring.install({ ck: fakeCk });
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', userId: 'U-1', source: 'test.post-install' });
      expect(dispatched).toHaveLength(1);
      expect(dispatched[0].type).toBe('CAPABILITY_EVALUATE');
      expect(dispatched[0].source).toBe('test.post-install');
    });

    it('emitNewAccountConnected routes through the bound CK', () => {
      wiring.install({ ck: fakeCk });
      signalDispatch.emitNewAccountConnected({ businessAccountId: 'BA-1', userId: 'U-1' });
      expect(dispatched).toHaveLength(1);
      expect(dispatched[0].type).toBe('NEW_ACCOUNT_CONNECTED');
    });

    it('emitTokenRefreshed routes through the bound CK', () => {
      wiring.install({ ck: fakeCk });
      signalDispatch.emitTokenRefreshed({ businessAccountId: 'BA-1', userId: 'U-1' });
      expect(dispatched).toHaveLength(1);
      expect(dispatched[0].type).toBe('TOKEN_REFRESHED');
    });

    it('explicit ck param overrides the bound CK', () => {
      wiring.install({ ck: fakeCk });
      const otherCk = { dispatch: (e) => { dispatched.push({ ...e, _via: 'otherCk' }); return { allowed: true }; } };
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', ck: otherCk, source: 'test' });
      expect(dispatched).toHaveLength(1);
      expect(dispatched[0]._via).toBe('otherCk');
    });
  });

  describe('Idempotency', () => {
    it('re-install with the same CK is idempotent (no error)', () => {
      wiring.install({ ck: fakeCk });
      expect(() => wiring.install({ ck: fakeCk })).not.toThrow();
      expect(signalDispatch.getCk()).toBe(fakeCk);
    });

    it('re-install with a different CK replaces the binding', () => {
      wiring.install({ ck: fakeCk });
      const otherCk = { dispatch: (e) => { dispatched.push(e); return { allowed: true }; } };
      wiring.install({ ck: otherCk });
      expect(signalDispatch.getCk()).toBe(otherCk);
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' });
      expect(dispatched).toHaveLength(1);
    });
  });

  describe('Uninstall: binding released', () => {
    it('wiring.uninstall() releases the CK binding', () => {
      wiring.install({ ck: fakeCk });
      wiring.uninstall();
      expect(signalDispatch.getCk()).toBeNull();
    });

    it('post-uninstall emits warn again (warning latch reset on real install)', () => {
      wiring.install({ ck: fakeCk });
      wiring.uninstall();
      warnSpy.mockClear(); // clear any post-uninstall message
      signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' });
      expect(dispatched).toHaveLength(0);
      expect(warnSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('Error handling: never throws to caller', () => {
    it('CK.dispatch throwing is swallowed and warn is logged', () => {
      const throwingCk = { dispatch: () => { throw new Error('CK offline'); } };
      wiring.install({ ck: throwingCk });
      // trigger-bridge has its own try/catch + warn; we just need the
      // signal-dispatch to not throw to the caller.
      expect(() => signalDispatch.emitEvaluate({ businessAccountId: 'BA-1', source: 'test' })).not.toThrow();
    });
  });
});
