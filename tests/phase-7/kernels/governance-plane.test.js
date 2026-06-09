// ============================================
// Governance Plane — Layers 2/3/4 integration tests
// ============================================
// Verifies the formalization gap fixes:
//   Layer 2 — canonical observation envelope shape contract
//   Layer 3 — CAPABILITY_OBSERVATION transition + aggregator
//   Layer 4 — health substrate emits through signal-dispatch
//
// Scope: fsm.js, observations.js, signal-dispatch.js, substrate façades,
// health-substrate. Pure unit tests. No runtime simulator required.
// ============================================

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const signalDispatch = require('../../../graph-capability-kernel/substrates/vault/signal-dispatch');
// Phase D: observations.js was migrated into the FSM. Alias its surface
// (newEnvelope, emptyEnvelope, normalize) onto the fsm module for this test
// so we don't have to rewrite every call site.
const fsmForObs = require('../../../graph-capability-kernel/fsm');
const observations = {
  newEnvelope: fsmForObs.newEnvelope,
  emptyEnvelope: () => fsmForObs.newEnvelope({}),
  normalize: (env) => fsmForObs.inferStateFromEnvelope(env),
};
const fsm = require('../../../graph-capability-kernel/fsm');
const wiring = require('../../../graph-capability-kernel/substrates/graph-capability/wiring');
// Phase D: health-substrate/wiring.js was deleted. The FSM now orchestrates
// the membrane (gck.install + CAPABILITY_BOOTSTRAP). No more healthWiring
// module to require.
const gck = require('../../../graph-capability-kernel');

describe('Governance Plane — Layer 2 (envelope contract)', () => {
  describe('observations envelope constructors', () => {
    it('newEnvelope() returns a fresh envelope with all inner slots null', () => {
      const env = observations.newEnvelope({ businessAccountId: 'BA-1', userId: 'U-1' });
      expect(env.businessAccountId).toBe('BA-1');
      expect(env.userId).toBe('U-1');
      expect(env.pat).toBeNull();
      expect(env.uat).toBeNull();
      expect(env.detection).toBeNull();
      expect(env.scope).toBeNull();
      expect(typeof env.envelopeId).toBe('string');
      expect(typeof env.observedAt).toBe('number');
    });

    it('emptyEnvelope() returns an envelope with all inner slots null (observedAt is set by the factory)', () => {
      // Phase D: observations was migrated into the FSM. The old
      // emptyEnvelope() returned observedAt: null. The new newEnvelope({})
      // sets observedAt to Date.now() — by design, the FSM stamps
      // observedAt on every envelope at construction time. The test
      // reflects the new contract: inner slots are null, observedAt is set.
      const env = observations.emptyEnvelope();
      expect(env.pat).toBeNull();
      expect(env.uat).toBeNull();
      expect(env.detection).toBeNull();
      expect(env.scope).toBeNull();
      expect(typeof env.observedAt).toBe('number');
    });

    it('normalize() maps PAT not decryptable → UNAUTHORIZED', () => {
      const env = observations.newEnvelope({});
      env.pat = { isDecryptable: false };
      const result = observations.normalize(env);
      expect(result.state).toBe('UNAUTHORIZED');
      expect(result.reason).toBe('PAT not decryptable');
    });

    it('normalize() maps UAT not decryptable → UNAUTHORIZED', () => {
      const env = observations.newEnvelope({});
      env.uat = { isDecryptable: false };
      const result = observations.normalize(env);
      expect(result.state).toBe('UNAUTHORIZED');
      expect(result.reason).toBe('UAT not decryptable');
    });

    it('normalize() maps detection.isValid=false → UNAUTHORIZED', () => {
      const env = observations.newEnvelope({});
      env.detection = { isValid: false, reason: 'expired' };
      const result = observations.normalize(env);
      expect(result.state).toBe('UNAUTHORIZED');
      expect(result.reason).toBe('expired');
    });

    it('normalize() maps missing scopes → LIMITED (when all 4 slots populated)', () => {
      // Phase D: LIMITED requires all 4 observation slots to be present.
      // A partial envelope (only scope) is PAT_PENDING, not LIMITED. The
      // strengthened FSM is strict about envelope completeness.
      const env = observations.newEnvelope({});
      env.pat = { isDecryptable: true };
      env.uat = { isDecryptable: true };
      env.detection = { isValid: true, reliabilityImpaired: false };
      env.scope = { grantedScopes: ['pages_show_list'] }; // missing instagram_basic etc
      const result = observations.normalize(env);
      expect(result.state).toBe('LIMITED');
      expect(result.missingScopes.length).toBeGreaterThan(0);
    });

    it('normalize() maps reliabilityImpaired → DEGRADED (when all 4 slots populated)', () => {
      const env = observations.newEnvelope({});
      env.pat = { isDecryptable: true };
      env.uat = { isDecryptable: true };
      env.detection = { isValid: true, reliabilityImpaired: true };
      env.scope = { grantedScopes: ['instagram_basic', 'instagram_manage_comments', 'instagram_manage_insights', 'instagram_content_publish', 'pages_show_list', 'pages_read_engagement'] };
      const result = observations.normalize(env);
      expect(result.state).toBe('DEGRADED');
    });

    it('normalize() maps all-green envelope → AUTHORIZED', () => {
      const env = observations.newEnvelope({});
      env.pat = { isDecryptable: true };
      env.uat = { isDecryptable: true };
      env.detection = { isValid: true };
      env.scope = { grantedScopes: ['instagram_basic', 'instagram_manage_comments', 'instagram_manage_insights', 'instagram_content_publish', 'pages_show_list', 'pages_read_engagement'] };
      const result = observations.normalize(env);
      expect(result.state).toBe('AUTHORIZED');
    });

    it('normalize() maps partial envelope (some slots null) → PENDING, not UNKNOWN', () => {
      // Phase D contract: a partial envelope (pat populated, others null)
      // is UAT_PENDING (first missing slot). UNKNOWN is reserved for envelopes
      // with all 4 slots null. This is the inferential model.
      const env = observations.newEnvelope({});
      env.pat = { isDecryptable: true };
      // uat, detection, scope all null
      const result = observations.normalize(env);
      expect(result.state).toBe('UAT_PENDING');
    });

    it('normalize() maps all-4-null envelope → UNKNOWN', () => {
      const env = observations.newEnvelope({});
      // all inner slots null
      const result = observations.normalize(env);
      expect(result.state).toBe('UNKNOWN');
    });
  });
});

describe('Governance Plane — Layer 3 (FSM observation transition)', () => {
  let fakeCk;
  let dispatched;

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
    vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    if (wiring.isInstalled()) wiring.uninstall();
    signalDispatch.bindCk(null);
    vi.restoreAllMocks();
  });

  it('CAPABILITY_OBSERVATION guard requires event.envelope', () => {
    const result = fsm.dispatch({ type: 'CAPABILITY_OBSERVATION' });
    expect(result.allowed).toBe(false);
    expect(result.reason).toMatch(/requires event.envelope/);
  });

  it('CAPABILITY_OBSERVATION with valid envelope fires aggregator', () => {
    wiring.install({ ck: fakeCk });
    const env = observations.newEnvelope({ businessAccountId: 'BA-1' });
    env.pat = { isDecryptable: true };
    env.uat = { isDecryptable: true };
    env.detection = { isValid: true };
    env.scope = { grantedScopes: ['instagram_basic', 'instagram_manage_comments', 'instagram_manage_insights', 'instagram_content_publish', 'pages_show_list', 'pages_read_engagement'] };
    // Direct call to the FSM (not via ck, to avoid going through DOMAIN_EVENT_MAP)
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env });
    // Aggregator should have called _aggregateAndDispatch → derived CAPABILITY_OK
    // and that should have set the BA-1 cred to AUTHORIZED.
    expect(fsm.getState('BA-1')).toBe('AUTHORIZED');
  });

  it('CAPABILITY_OBSERVATION with isDecryptable=false → FSM goes UNAUTHORIZED', () => {
    wiring.install({ ck: fakeCk });
    const env = observations.newEnvelope({ businessAccountId: 'BA-1' });
    env.pat = { isDecryptable: false };
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env });
    expect(fsm.getState('BA-1')).toBe('UNAUTHORIZED');
  });

  it('CAPABILITY_OBSERVATION with missing scopes → FSM goes LIMITED (only when all 4 slots present)', () => {
    wiring.install({ ck: fakeCk });
    const env = observations.newEnvelope({ businessAccountId: 'BA-2' });
    // All 4 slots present, but scope has no granted scopes → LIMITED
    env.pat = { isDecryptable: true };
    env.uat = { isDecryptable: true };
    env.detection = { isValid: true };
    env.scope = { grantedScopes: [] };
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env });
    expect(fsm.getState('BA-2')).toBe('LIMITED');
  });

  it('evaluateTriggerCriteria recognises OBSERVATION_ARRIVED', () => {
    const result = fsm.evaluateTriggerCriteria({ trigger: 'OBSERVATION_ARRIVED' });
    expect(result.decision).toBe('APPROVED');
    expect(result.reason).toMatch(/Worker observation/);
  });

  it('emitEnvelope routes through fsm.dispatch(CAPABILITY_OBSERVATION)', () => {
    // Phase D: signal-dispatch is bound to the FSM, not the CK. The
    // substrate's emissions route to fsm.dispatch, which the FSM then
    // may forward to the CK via ctx.dispatchGlobal for cross-domain work.
    const dispatchSpy = vi.spyOn(fsm, 'dispatch');
    signalDispatch.bindFsm(fsm, { validate: () => ({ allowed: true }), dispatchGlobal: () => ({ allowed: true }) });
    const env = observations.newEnvelope({ businessAccountId: 'BA-1' });
    env.pat = { isDecryptable: true };
    env.uat = { isDecryptable: true };
    env.detection = { isValid: true };
    env.scope = { grantedScopes: ['instagram_basic', 'instagram_manage_comments', 'instagram_manage_insights', 'instagram_content_publish', 'pages_show_list', 'pages_read_engagement'] };
    signalDispatch.emitEnvelope({ envelope: env });
    const obsEvents = dispatchSpy.mock.calls.filter(c => c[0].type === 'CAPABILITY_OBSERVATION');
    expect(obsEvents.length).toBe(1);
    expect(obsEvents[0][0].envelope).toBeDefined();
    expect(obsEvents[0][0].envelope.envelopeId).toBe(env.envelopeId);
    dispatchSpy.mockRestore();
  });

  it('emitEnvelope without bound FSM is warn-once dropped (does not throw)', () => {
    signalDispatch.bindFsm(null, null);
    const env = observations.newEnvelope({});
    expect(() => signalDispatch.emitEnvelope({ envelope: env })).not.toThrow();
  });
});

describe('Governance Plane — Layer 4 (health substrate integration)', () => {
  let fakeCk;
  let dispatched;

  beforeEach(() => {
    dispatched = [];
    fakeCk = {
      dispatch: (event) => {
        dispatched.push(event);
        // Phase D: route to FSM via DOMAIN_EVENT_MAP (real CK does this)
        if (event.type === 'CAPABILITY_BOOTSTRAP' && typeof fsm.dispatch === 'function') {
          return fsm.dispatch(event, {
            validate: () => ({ allowed: true }),
            dispatchGlobal: () => ({ allowed: true }),
          });
        }
        return { allowed: true, from: fsm.getState(), to: fsm.getState() };
      },
      validateDomainTransition: () => ({ allowed: true }),
      getState: () => 'BOOTING',
      // Phase D: substrate.start(ck) calls ck.subscribeAction
      subscribeAction: () => {},
    };
    vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    if (gck.isInstalled()) gck.uninstall();
    vi.restoreAllMocks();
  });

  it('gck.install({ck}) registers the health membrane with the FSM (substrate is not started yet)', () => {
    gck.install({ ck: fakeCk });
    expect(gck.isInstalled()).toBe(true);
    // Phase D: the health substrate is a delegated executor orchestrated by
    // the FSM. The CK (via gck.install) does NOT start it directly. The
    // substrate becomes a "first-class citizen" only when the FSM wires it
    // during CAPABILITY_BOOTSTRAP. So isStarted() is false until then.
    expect(gck.health.isStarted()).toBe(false);
  });

  it('gck.uninstall() tears down the constitutional binding', () => {
    gck.install({ ck: fakeCk });
    gck.uninstall();
    expect(gck.isInstalled()).toBe(false);
    expect(gck.health.isStarted()).toBe(false);
    // signal-dispatch should be unbound after uninstall
    expect(signalDispatch.getFsm()).toBeNull();
  });

  it('standalone gck.install without CAPABILITY_BOOTSTRAP leaves the membrane unwired', () => {
    // (Phase D — replaces the old "wiring guard warns when CK not bound" test.
    // The old contract used healthWiring.install() as a no-op guard. The new
    // contract: the FSM is the only path that wires the membrane.)
    gck.install({ ck: fakeCk });
    // Without dispatching CAPABILITY_BOOTSTRAP, the substrate is not started
    expect(gck.health.isStarted()).toBe(false);
    // But the signal-dispatch is bound to the FSM (via gck.install)
    expect(signalDispatch.getFsm()).toBe(gck.fsm);
    // And emissions are routed to the FSM
    const env = observations.newEnvelope({});
    expect(() => signalDispatch.emitEnvelope({ envelope: env })).not.toThrow();
  });

  it('idempotent gck.install returns existing state on re-call', () => {
    gck.install({ ck: fakeCk });
    const r1 = gck.install({ ck: fakeCk });
    expect(r1.started).toBe(true);
    // healthStarted is false at install time — the FSM wires it during bootstrap
    expect(r1.healthStarted).toBe(false);
  });

  it('CAPABILITY_BOOTSTRAP wired by gck.install causes the FSM to call substrate.start(ck) (FSM orchestrates the membrane)', () => {
    gck.install({ ck: fakeCk });
    expect(gck.health.isStarted()).toBe(false);
    // Dispatch CAPABILITY_BOOTSTRAP — the FSM's buildActions calls _wireMembranes
    // which calls substrate.start(ck). This is the constitutional path: the FSM
    // is the only entity that wires the membrane.
    fakeCk.dispatch({ type: 'CAPABILITY_BOOTSTRAP' });
    expect(gck.health.isStarted()).toBe(true);
  });
});
