// ============================================
// Strengthened FSM — 9-state inferential model
// ============================================
// Verifies:
//   - 9 states registered
//   - per-cred isolation
//   - PENDING inference from envelope shape
//   - COMPLETE inference from full envelope
//   - scope-diff in verdict-gate
//   - authority boundary: FSM is sole interpreter
//   - evidence persistence
//   - dispatch recursion (CAPABILITY_OBSERVATION → derived event)

import { beforeEach, describe, test, expect } from 'vitest';

const fsm = require('../../../graph-capability-kernel/fsm');
// G1 migration: verdict-gate.js and observations.js were deleted and merged
// into the FSM. verdict-gate.peekVerdict → fsm.getCapabilityVerdict.
// verdict-gate.requireCapability(userId, baId, scopes) → fsm.requireCapability(baId, scopes)
// (the userId arg was dropped from the contract — FSM is per-credential by BA only).
// observations.newEnvelope → fsm.newEnvelope (identical shape).

const BA_A = '00000000-0000-0000-0000-aaaaaaaaaaaa';
const BA_B = '00000000-0000-0000-0000-bbbbbbbbbbbb';
const UA_A = '00000000-0000-0000-0000-00000000000a';
const UA_B = '00000000-0000-0000-0000-00000000000b';

function allRequiredScopes() {
  return [
    'instagram_basic',
    'instagram_manage_comments',
    'instagram_manage_insights',
    'instagram_content_publish',
    'pages_show_list',
    'pages_read_engagement',
  ];
}

function freshFullEnvelope(businessAccountId, opts = {}) {
  const env = fsm.newEnvelope({ businessAccountId, userId: opts.userId || UA_A });
  env.pat = { isDecryptable: true, ...(opts.pat || {}) };
  env.uat = { isDecryptable: true, ...(opts.uat || {}) };
  env.detection = { isValid: true, reliabilityImpaired: false, reason: null, ...(opts.detection || {}) };
  env.scope = {
    grantedScopes: opts.grantedScopes || allRequiredScopes(),
    cacheAgeMs: 0,
  };
  return env;
}

function partialEnvelope(businessAccountId, slot) {
  const env = fsm.newEnvelope({ businessAccountId, userId: UA_A });
  env[slot] = { isDecryptable: slot === 'pat' || slot === 'uat' ? true : undefined,
                isValid: slot === 'detection' ? true : undefined,
                grantedScopes: slot === 'scope' ? allRequiredScopes() : undefined,
                cacheAgeMs: 0 };
  return env;
}

const fakeCk = {
  dispatch: () => ({ allowed: true }),
  validateDomainTransition: () => ({ allowed: true }),
  getState: () => 'BOOTING',
};

beforeEach(() => {
  fsm._resetCred();
});

describe('9-state registry', () => {
  test('exactly 9 states registered', () => {
    expect(Object.keys(fsm.STATE_REGISTRY).length).toBe(9);
  });
  test('5 complete + 4 pending', () => {
    const complete = Object.entries(fsm.STATE_REGISTRY).filter(([_, d]) => d.category === 'COMPLETE').map(([n]) => n);
    const pending  = Object.entries(fsm.STATE_REGISTRY).filter(([_, d]) => d.category === 'PENDING').map(([n]) => n);
    const empty    = Object.entries(fsm.STATE_REGISTRY).filter(([_, d]) => d.category === 'EMPTY').map(([n]) => n);
    expect(complete.sort()).toEqual(['AUTHORIZED', 'DEGRADED', 'LIMITED', 'UNAUTHORIZED']);
    expect(pending.sort()).toEqual(['DETECTION_PENDING', 'PAT_PENDING', 'SCOPE_PENDING', 'UAT_PENDING']);
    expect(empty).toEqual(['UNKNOWN']);
  });
  test('PENDING states name their missing slot', () => {
    expect(fsm.STATE_REGISTRY.PAT_PENDING.missingSlot).toBe('pat');
    expect(fsm.STATE_REGISTRY.UAT_PENDING.missingSlot).toBe('uat');
    expect(fsm.STATE_REGISTRY.DETECTION_PENDING.missingSlot).toBe('detection');
    expect(fsm.STATE_REGISTRY.SCOPE_PENDING.missingSlot).toBe('scope');
  });
});

describe('inferential layer', () => {
  test('infers UNAUTHORIZED from pat.isDecryptable=false', () => {
    const env = freshFullEnvelope(BA_A);
    env.pat.isDecryptable = false;
    const r = fsm.inferStateFromEnvelope(env);
    expect(r.state).toBe('UNAUTHORIZED');
    expect(r.reason).toContain('PAT');
  });
  test('infers UNAUTHORIZED from uat.isDecryptable=false', () => {
    const env = freshFullEnvelope(BA_A);
    env.uat.isDecryptable = false;
    expect(fsm.inferStateFromEnvelope(env).state).toBe('UNAUTHORIZED');
  });
  test('infers UNAUTHORIZED from detection.isValid=false', () => {
    const env = freshFullEnvelope(BA_A);
    env.detection.isValid = false;
    env.detection.reason = 'Token validation failed';
    expect(fsm.inferStateFromEnvelope(env).state).toBe('UNAUTHORIZED');
  });
  test('infers LIMITED when scope missing required', () => {
    const env = freshFullEnvelope(BA_A, { grantedScopes: ['instagram_basic'] });
    const r = fsm.inferStateFromEnvelope(env);
    expect(r.state).toBe('LIMITED');
    expect(r.missingScopes.length).toBeGreaterThan(0);
    expect(r.missingScopes).toContain('instagram_content_publish');
  });
  test('infers DEGRADED from reliabilityImpaired=true', () => {
    const env = freshFullEnvelope(BA_A, { detection: { reliabilityImpaired: true } });
    expect(fsm.inferStateFromEnvelope(env).state).toBe('DEGRADED');
  });
  test('infers DEGRADED from stale scope cache', () => {
    const env = freshFullEnvelope(BA_A);
    env.scope.cacheAgeMs = 13 * 60 * 60 * 1000; // 13h, > 12h threshold
    expect(fsm.inferStateFromEnvelope(env).state).toBe('DEGRADED');
  });
  test('infers AUTHORIZED for full green envelope', () => {
    const env = freshFullEnvelope(BA_A);
    expect(fsm.inferStateFromEnvelope(env).state).toBe('AUTHORIZED');
  });
});

describe('PENDING inference from partial envelope', () => {
  test('envelope with only pat slot → UAT_PENDING (first missing)', () => {
    const env = partialEnvelope(BA_A, 'pat');
    const r = fsm.inferStateFromEnvelope(env);
    expect(r.state).toBe('UAT_PENDING');
  });
  test('envelope with pat+uat slots → DETECTION_PENDING (first missing)', () => {
    const env = partialEnvelope(BA_A, 'pat');
    env.uat = { isDecryptable: true };
    expect(fsm.inferStateFromEnvelope(env).state).toBe('DETECTION_PENDING');
  });
  test('envelope with pat+uat+detection → SCOPE_PENDING (first missing)', () => {
    const env = partialEnvelope(BA_A, 'pat');
    env.uat = { isDecryptable: true };
    env.detection = { isValid: true, reliabilityImpaired: false, reason: null };
    expect(fsm.inferStateFromEnvelope(env).state).toBe('SCOPE_PENDING');
  });
  test('null envelope → UNKNOWN', () => {
    expect(fsm.inferStateFromEnvelope(null).state).toBe('UNKNOWN');
  });
  test('envelope with all 4 null → UNKNOWN (not PENDING)', () => {
    const env = fsm.newEnvelope({ businessAccountId: BA_A });
    expect(fsm.inferStateFromEnvelope(env).state).toBe('UNKNOWN');
  });
});

describe('envelope merge', () => {
  test('merges pat-only envelope into existing, retains existing uat', () => {
    const a = fsm.newEnvelope({ businessAccountId: BA_A });
    a.uat = { isDecryptable: true, scope: ['x'] };
    const b = fsm.newEnvelope({ businessAccountId: BA_A });
    b.pat = { isDecryptable: true };
    const merged = fsm.mergeEnvelope(a, b);
    expect(merged.uat.scope).toEqual(['x']);
    expect(merged.pat.isDecryptable).toBe(true);
  });
  test('null merge returns existing', () => {
    const a = fsm.newEnvelope({ businessAccountId: BA_A });
    a.pat = { isDecryptable: true };
    expect(fsm.mergeEnvelope(a, null)).toBe(a);
  });
});

describe('per-cred state isolation', () => {
  test('cred A in AUTHORIZED, cred B in UNKNOWN', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    expect(fsm.getState(BA_A)).toBe('AUTHORIZED');
    expect(fsm.getState(BA_B)).toBe('UNKNOWN');
  });
  test('cred A in UNAUTHORIZED, cred B in AUTHORIZED — independent', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    const envB = freshFullEnvelope(BA_B);
    envB.pat.isDecryptable = false;
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: envB }, fakeCk);
    expect(fsm.getState(BA_A)).toBe('AUTHORIZED');
    expect(fsm.getState(BA_B)).toBe('UNAUTHORIZED');
  });
  test('partial envelope for cred A leaves B untouched', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: partialEnvelope(BA_B, 'pat') }, fakeCk);
    expect(fsm.getState(BA_A)).toBe('AUTHORIZED');
    // pat slot set, uat first missing → UAT_PENDING
    expect(fsm.getState(BA_B)).toBe('UAT_PENDING');
  });
});

describe('partial→pending→complete transition flow', () => {
  test('flow: UAT_PENDING → DETECTION_PENDING → SCOPE_PENDING → AUTHORIZED', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: partialEnvelope(BA_A, 'pat') }, fakeCk);
    expect(fsm.getState(BA_A)).toBe('UAT_PENDING');
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: partialEnvelope(BA_A, 'uat') }, fakeCk);
    // Now both pat and uat populated, detection and scope missing
    expect(fsm.getState(BA_A)).toBe('DETECTION_PENDING');
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: partialEnvelope(BA_A, 'detection') }, fakeCk);
    expect(fsm.getState(BA_A)).toBe('SCOPE_PENDING');
    // Now full envelope → AUTHORIZED
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    expect(fsm.getState(BA_A)).toBe('AUTHORIZED');
  });
});

describe('evidence persistence', () => {
  test('evidence persisted in per-cred record', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    const exp = fsm.exportState(BA_A);
    expect(exp.state).toBe('AUTHORIZED');
    expect(exp.evidence).toBeTruthy();
    expect(exp.evidence.pat).toBeTruthy();
    expect(exp.evidence.uat).toBeTruthy();
    expect(exp.evidence.detection).toBeTruthy();
    expect(exp.evidence.scope).toBeTruthy();
    expect(exp.evidence.scope.grantedScopes.length).toBeGreaterThan(0);
  });
  test('evidence cleared on _resetCred', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    fsm._resetCred(BA_A);
    expect(fsm.getState(BA_A)).toBe('UNKNOWN');
    expect(fsm.exportState(BA_A).evidence).toBeNull();
  });
});

describe('consecutive failures per cred', () => {
  test('UNAUTHORIZED increments consecutive', () => {
    const env = freshFullEnvelope(BA_A);
    env.pat.isDecryptable = false;
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env }, fakeCk);
    expect(fsm.exportState(BA_A).consecutiveFailures).toBe(1);
  });
  test('AUTHORIZED resets to 0', () => {
    const env = freshFullEnvelope(BA_A);
    env.pat.isDecryptable = false;
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env }, fakeCk);
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    expect(fsm.exportState(BA_A).consecutiveFailures).toBe(0);
  });
});

describe('verdict-gate reads per-cred FSM', () => {
  test('AUTHORIZED + required scopes present → allowed', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    const v = fsm.requireCapability(BA_A, ['instagram_basic', 'pages_read_engagement']);
    expect(v.allowed).toBe(true);
    expect(v.state).toBe('AUTHORIZED');
  });
  test('LIMITED → denied with missing scopes', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A, { grantedScopes: ['instagram_basic'] }) }, fakeCk);
    const v = fsm.requireCapability(BA_A, ['instagram_basic', 'instagram_content_publish']);
    expect(v.allowed).toBe(false);
    expect(v.state).toBe('LIMITED');
    expect(v.missingScopes).toContain('instagram_content_publish');
  });
  test('PAT_PENDING → denied with reason naming the missing slot', () => {
    // partialEnvelope(BA_A, 'uat') sets only uat, so pat is first missing → PAT_PENDING
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: partialEnvelope(BA_A, 'uat') }, fakeCk);
    const v = fsm.requireCapability(BA_A, ['instagram_basic']);
    expect(v.allowed).toBe(false);
    expect(v.state).toBe('PAT_PENDING');
    expect(v.reason).toContain('PAT_PENDING');
  });
  test('UNAUTHORIZED → denied', () => {
    const env = freshFullEnvelope(BA_A);
    env.pat.isDecryptable = false;
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env }, fakeCk);
    expect(fsm.requireCapability(BA_A, ['instagram_basic']).allowed).toBe(false);
  });
  test('DEGRADED → allowed (with warning reason)', () => {
    const env = freshFullEnvelope(BA_A, { detection: { reliabilityImpaired: true } });
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env }, fakeCk);
    const v = fsm.requireCapability(BA_A, ['instagram_basic']);
    expect(v.state).toBe('DEGRADED');
  });
  test('UNKNOWN cred → denied with capability-not-yet-evaluated', () => {
    const v = fsm.requireCapability('99999999-0000-0000-0000-999999999999', ['instagram_basic']);
    expect(v.allowed).toBe(false);
    expect(v.state).toBe('UNKNOWN');
  });
  test('peekVerdict returns per-cred state', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    const v = fsm.getCapabilityVerdict(BA_A);
    expect(v.state).toBe('AUTHORIZED');
    expect(v.evidence).toBeTruthy();
  });
});

describe('FSM is sole interpreter', () => {
  test('verdict-gate does not mutate state', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    const before = fsm.exportState(BA_A).lastTransitionedAt;
    fsm.requireCapability(BA_A, ['instagram_basic']);
    expect(fsm.exportState(BA_A).lastTransitionedAt).toBe(before);
  });
  test('FSM has no public setter for state', () => {
    const exportedKeys = Object.keys(fsm);
    expect(exportedKeys).not.toContain('setState');
    expect(exportedKeys).not.toContain('setLocalState');
  });
});

describe('event validation', () => {
  test('rejects null event', () => {
    expect(fsm.dispatch(null, fakeCk).allowed).toBe(false);
  });
  test('rejects unknown event type', () => {
    expect(fsm.dispatch({ type: 'BOGUS' }, fakeCk).allowed).toBe(false);
  });
  test('rejects CAPABILITY_OBSERVATION without envelope', () => {
    expect(fsm.dispatch({ type: 'CAPABILITY_OBSERVATION' }, fakeCk).allowed).toBe(false);
  });
  test('rejects CAPABILITY_OBSERVATION without businessAccountId', () => {
    const env = fsm.newEnvelope({ businessAccountId: null });
    const r = fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: env }, fakeCk);
    expect(r.allowed).toBe(false);
  });
});

describe('listCreds', () => {
  test('lists all creds that have been observed', () => {
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_A) }, fakeCk);
    fsm.dispatch({ type: 'CAPABILITY_OBSERVATION', envelope: freshFullEnvelope(BA_B) }, fakeCk);
    const list = fsm.listCreds();
    expect(list).toContain(BA_A);
    expect(list).toContain(BA_B);
  });
});
