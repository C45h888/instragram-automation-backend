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
const observations = require('../../../graph-capability-kernel/substrates/graph-capability/observations');
const fsm = require('../../../graph-capability-kernel/fsm');
const wiring = require('../../../graph-capability-kernel/substrates/graph-capability/wiring');
const healthWiring = require('../../../graph-capability-kernel/substrates/health-substrate/wiring');
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

    it('emptyEnvelope() returns an all-null envelope with no observedAt', () => {
      const env = observations.emptyEnvelope();
      expect(env.pat).toBeNull();
      expect(env.observedAt).toBeNull();
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

    it('normalize() maps missing scopes → LIMITED', () => {
      const env = observations.newEnvelope({});
      env.scope = { grantedScopes: ['pages_show_list'] }; // missing instagram_basic etc
      const result = observations.normalize(env);
      expect(result.state).toBe('LIMITED');
      expect(result.missingScopes.length).toBeGreaterThan(0);
    });

    it('normalize() maps reliabilityImpaired → DEGRADED', () => {
      const env = observations.newEnvelope({});
      env.detection = { isValid: true, reliabilityImpaired: true };
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

    it('normalize() maps partial envelope → UNKNOWN', () => {
      const env = observations.newEnvelope({});
      env.pat = { isDecryptable: true };
      // uat, detection, scope all null
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
    // and that should have set local state to AUTHORIZED.
    expect(fsm.getState()).toBe('AUTHORIZED');
  });

  it('CAPABILITY_OBSERVATION with isDecryptable=false → FSM goes UNAUTHORIZED', () => {
    wiring.install({ ck: fakeCk });
    const env = observations.newEnvelope({});
    env.pat = { isDecryptable: false };
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env });
    expect(fsm.getState()).toBe('UNAUTHORIZED');
  });

  it('CAPABILITY_OBSERVATION with missing scopes → FSM goes LIMITED', () => {
    wiring.install({ ck: fakeCk });
    const env = observations.newEnvelope({});
    env.scope = { grantedScopes: [] };
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env });
    expect(fsm.getState()).toBe('LIMITED');
  });

  it('evaluateTriggerCriteria recognises OBSERVATION_ARRIVED', () => {
    const result = fsm.evaluateTriggerCriteria({ trigger: 'OBSERVATION_ARRIVED' });
    expect(result.decision).toBe('APPROVED');
    expect(result.reason).toMatch(/Worker observation/);
  });

  it('emitEnvelope routes through ck.dispatch(CAPABILITY_OBSERVATION)', () => {
    wiring.install({ ck: fakeCk });
    const env = observations.newEnvelope({ businessAccountId: 'BA-1' });
    env.pat = { isDecryptable: true };
    env.uat = { isDecryptable: true };
    env.detection = { isValid: true };
    env.scope = { grantedScopes: ['instagram_basic', 'instagram_manage_comments', 'instagram_manage_insights', 'instagram_content_publish', 'pages_show_list', 'pages_read_engagement'] };
    signalDispatch.emitEnvelope({ envelope: env });
    const obsEvents = dispatched.filter(e => e.type === 'CAPABILITY_OBSERVATION');
    expect(obsEvents.length).toBe(1);
    expect(obsEvents[0].envelope).toBeDefined();
    expect(obsEvents[0].envelope.envelopeId).toBe(env.envelopeId);
  });

  it('emitEnvelope without bound CK is warn-once dropped (does not throw)', () => {
    const env = observations.newEnvelope({});
    expect(() => signalDispatch.emitEnvelope({ envelope: env })).not.toThrow();
    expect(dispatched).toHaveLength(0);
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
        return { allowed: true, from: fsm.getState(), to: fsm.getState() };
      },
      validateDomainTransition: () => ({ allowed: true }),
      getState: () => 'BOOTING',
    };
    vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    if (gck.isInstalled()) gck.uninstall();
    vi.restoreAllMocks();
  });

  it('gck.install({ck}) starts health substrate', () => {
    gck.install({ ck: fakeCk });
    expect(gck.isInstalled()).toBe(true);
    expect(gck.health.isStarted()).toBe(true);
    expect(healthWiring.isInstalled()).toBe(true);
  });

  it('gck.uninstall() stops health before tearing down the constitutional binding', () => {
    gck.install({ ck: fakeCk });
    gck.uninstall();
    expect(gck.isInstalled()).toBe(false);
    expect(gck.health.isStarted()).toBe(false);
    expect(healthWiring.isInstalled()).toBe(false);
    expect(signalDispatch.getCk()).toBeNull();
  });

  it('health wiring guard warns when CK not bound (standalone install)', () => {
    expect(() => healthWiring.install()).not.toThrow();
    // After standalone install, signal-dispatch is still unbound — emits dropped.
    const env = observations.newEnvelope({});
    expect(() => signalDispatch.emitEnvelope({ envelope: env })).not.toThrow();
  });

  it('idempotent gck.install returns existing state on re-call', () => {
    gck.install({ ck: fakeCk });
    const r1 = gck.install({ ck: fakeCk });
    expect(r1.started).toBe(true);
    expect(r1.healthStarted).toBe(true);
  });
});
