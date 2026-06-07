// graph-capability-kernel/fsm.js
// Graph Capability Domain FSM: federated state machine governing Graph capability lifecycle.
// Migrated from control-plane/governance/domains/graph-capability-fsm.js
//
// Owns: capability validation cadence, token verification cadence, scope verification cadence,
//        capability degradation lifecycle, capability recovery lifecycle, capability state transitions,
//        capability admission criteria, canonical capability outputs.
// Does NOT own: Graph API calls, token inspection implementation, scope evaluation implementation,
//               PAT validation implementation, worker dispatch, substrate execution, vault lifecycle.
//
// Reports to: constitutional kernel for transition validation + global observability.
// Signals HSM via ctx.dispatchGlobal() for auth failure, degradation, recovery.
//
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) recommends constitutional state changes
//                  HSM (CK) validates and decides — FSM never mutates CK state
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Workers ↓    → substrate workers perform mechanical observation only
//                  FSM governs lifecycle meaning, workers perform observation
//
// Domain FSMs emit state transitions through the observability plane.
// The lineage worker consumes from the observability plane and writes to the
// canonical lineage ledger. FSMs do NOT write to the lineage ledger directly.
//
// Local states:
//   UNAUTHORIZED  — required capability unavailable. Operation must be denied.
//   UNKNOWN       — capability cannot currently be determined. Operation enters evaluation flow.
//   AUTHORIZED    — account possesses required capability. Operation may proceed.
//   LIMITED       — account possesses partial capability. Operation may proceed with reduced functionality.
//   DEGRADED      — capability exists but reliability is impaired. Operation proceeds under degradation policy.

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

// ═══════════════════════════════════════════════════════════════════════════════

// ── Governance reference (set by CK at boot) ────────────────────────────
// The FSM holds a governance ref for worker invocation and event dispatch.
// engagement-fsm pattern: set at boot, passed through execution contexts.
let _governance = null;

function setGovernance(governance) {
  if (governance && typeof governance.dispatch === 'function') {
    _governance = governance;
  }
}

function getGovernance() {
  return _governance;
}

// ── Worker registry (local) ───────────────────────────────────────────
// Each FSM holds its own worker map. CK registration happens at boot
// via constitutional.registerWorker(fsmName, workerName, worker).
// The CTX gate (ctx.invokeWorker) validates ownership through CK.
const _workers = new Map();

function registerWorker(name, worker) {
  _workers.set(name, worker);
}

function getWorker(name) {
  return _workers.get(name) || null;
}

function getWorkers() {
  return _workers;
}


// 0. Governance Policy Constants — domain-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const OBSERVATION_FRESHNESS_MS = 30 * 60 * 1000; // 30 min — observation envelope must be this fresh to authorize transitions
const DEGRADED_OBSERVATION_MS  = 2 * 60 * 60 * 1000; // 2h — observation older than this signals degradation
const REQUIRED_SCOPES = [
  'instagram_basic',
  'instagram_manage_comments',
  'instagram_manage_insights',
  'instagram_content_publish',
  'pages_show_list',
  'pages_read_engagement',
];

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  UNAUTHORIZED: {
    description: 'Required capability unavailable — operation must be denied',
  },
  UNKNOWN: {
    description: 'Capability cannot currently be determined — evaluation flow active',
  },
  AUTHORIZED: {
    description: 'Account possesses required capability — operation may proceed',
  },
  LIMITED: {
    description: 'Account possesses partial capability — operation may proceed with reduced functionality',
  },
  DEGRADED: {
    description: 'Capability exists but reliability is impaired — operation proceeds under degradation policy',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map — event → target + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Entry: any external trigger starts evaluation ─────────────────────────
  CAPABILITY_EVALUATE: {
    target: 'UNKNOWN',
    guard: (event) => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'CAPABILITY_EVALUATION_STARTED',
      source: event.source || 'unknown',
      businessAccountId: event.businessAccountId || null,
    }],
  },

  // ── Layer 3: Aggregate worker observation arrives. The FSM consumes the
  //    canonical envelope (produced by observations.js), runs the normalizer,
  //    and dispatches a derived state event (OK / PARTIAL / FAILED). This
  //    transition does NOT change local state directly — it forwards to
  //    a synthesized state event via internal _aggregateAndDispatch().
  //    From UNAUTHORIZED / DEGRADED, observation is a no-op (recovery requires
  //    an explicit CAPABILITY_RECOVERED).
  CAPABILITY_OBSERVATION: {
    target: 'UNKNOWN', // placeholder; buildActions dispatches the real transition
    guard: (event) => {
      if (!event || !event.envelope) {
        return { allowed: false, reason: 'CAPABILITY_OBSERVATION requires event.envelope' };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      // Run the aggregator — emits a derived event via fsm.dispatch().
      // This is internal recursion, depth-bounded to 2.
      // We pass ctx if present (for validate gate); the recursive dispatch
      // runs without ctx gracefully if ctx is undefined.
      if (event.envelope) {
        _aggregateAndDispatch(event.envelope, ctx);
      }
      return [{
        type: 'CAPABILITY_OBSERVATION_RECEIVED',
        envelopeId: event.envelope.envelopeId || null,
        businessAccountId: event.envelope.businessAccountId || null,
      }];
    },
  },

  // ── Aggregate observation resolves to AUTHORIZED ──────────────────────────
  CAPABILITY_OK: {
    target: 'AUTHORIZED',
    guard: (event) => {
      if (_localState !== 'UNKNOWN') {
        return { allowed: false, reason: `Cannot authorize from ${_localState} (must be UNKNOWN)` };
      }
      if (!_isObservationFresh(event.observedAt)) {
        return { allowed: false, reason: `Observation stale: ${event.observedAt}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_AUTHORIZED',
      evidence: event.evidence || null,
      observedAt: event.observedAt,
    }],
  },

  // ── Aggregate observation resolves to LIMITED ─────────────────────────────
  CAPABILITY_PARTIAL: {
    target: 'LIMITED',
    guard: (event) => {
      if (_localState !== 'UNKNOWN') {
        return { allowed: false, reason: `Cannot mark partial from ${_localState} (must be UNKNOWN)` };
      }
      if (!_isObservationFresh(event.observedAt)) {
        return { allowed: false, reason: `Observation stale: ${event.observedAt}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_LIMITED',
      missingScopes: event.missingScopes || [],
      evidence: event.evidence || null,
    }],
  },

  // ── Degradation from stable states ───────────────────────────────────────
  CAPABILITY_DEGRADED: {
    target: 'DEGRADED',
    guard: (event) => {
      if (_localState !== 'AUTHORIZED' && _localState !== 'LIMITED' && _localState !== 'UNKNOWN') {
        return { allowed: false, reason: `Cannot degrade from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_DEGRADATION_DETECTED',
      reason: event.reason || 'reliability impaired',
      evidence: event.evidence || null,
    }],
  },

  // ── Failure: required capability unavailable ──────────────────────────────
  CAPABILITY_FAILED: {
    target: 'UNAUTHORIZED',
    guard: (event) => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'CAPABILITY_AUTH_FAILURE',
      reason: event.reason || 'required capability unavailable',
      evidence: event.evidence || null,
    }],
  },

  // ── Recovery: re-evaluation restored capability ──────────────────────────
  CAPABILITY_RECOVERED: {
    target: 'AUTHORIZED',
    guard: (event) => {
      if (_localState !== 'UNAUTHORIZED' && _localState !== 'DEGRADED') {
        return { allowed: false, reason: `Cannot recover from ${_localState} (must be UNAUTHORIZED or DEGRADED)` };
      }
      if (!_isObservationFresh(event.observedAt)) {
        return { allowed: false, reason: `Observation stale: ${event.observedAt}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_RECOVERED',
      previousState: _localState,
      evidence: event.evidence || null,
    }],
  },

  // ── Cadence tick: re-evaluate from stable states ─────────────────────────
  CAPABILITY_REEVALUATE: {
    target: 'UNKNOWN',
    guard: (event) => {
      if (_localState === 'UNKNOWN') {
        return { allowed: false, reason: 'Already in UNKNOWN — reevaluation in progress' };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'CAPABILITY_REEVALUATING',
      previousState: _localState,
      cadence: event.cadence || 'scheduled',
    }],
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'UNKNOWN';
let _lastTransitionedAt = null;
let _lastObservedAt = null;
let _lastEvidence = null;
let _consecutiveFailures = 0;

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch — process event, ask constitutional for validation, transition
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the graph capability FSM.
 *
 * The FSM governs lifecycle only. For HSM-level signals (auth failure, degradation, recovery),
 * it uses ctx.dispatchGlobal() to RECOMMEND constitutional state changes.
 * The HSM (CK) validates via GLOBAL_TRANSITION_MAP guards and makes the final decision.
 *
 * @param {{ type: string, [key: string]: any }} event — domain event
 * @param {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx — constitutional kernel context
 * @returns {{ allowed: boolean, from?: string, to?: string, actions?: Array, reason?: string }}
 */
function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const from = _localState;

  // 1. Run per-transition guard
  if (txn.guard) {
    const result = txn.guard(event);
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  // 2. Resolve target state
  const rawTarget = txn.target;
  const target = typeof rawTarget === 'function' ? rawTarget(event) : rawTarget;

  // 3. Ask constitutional kernel for transition approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // 4. Track consecutive failures for health signals
  if (target === 'UNAUTHORIZED') {
    _consecutiveFailures++;
  } else if (target === 'AUTHORIZED') {
    _consecutiveFailures = 0;
  }

  // 5. Cache observation metadata
  if (event.observedAt) _lastObservedAt = event.observedAt;
  if (event.evidence) _lastEvidence = event.evidence;

  // 6. Materialize state
  _localState = target;
  _lastTransitionedAt = Date.now();

  // 7. Emit observability transition for domain FSM state change
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'graph-capability',
        entity: 'fsm',
        entityId: 'graph-capability-fsm',
        previousState: from,
        nextState: target,
        authority: 'graph-capability-fsm',
        raw: {
          intent: event.type,
          observedAt: _lastObservedAt,
          consecutiveFailures: _consecutiveFailures,
        },
      });
    }
  } catch (_) {}

  // 8. Build actions
  const actions = txn.buildActions ? txn.buildActions(event, ctx) : [];

  // 9. HSM signaling — FSM recommends, HSM decides
  const filteredActions = [];
  for (const action of actions) {
    if (action.type === 'CAPABILITY_AUTH_FAILURE') {
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_AUTH_FAILURE',
          reason: action.reason,
          evidence: action.evidence,
        });
      }
      // Do not pass to subscribers — handled
    } else if (action.type === 'CAPABILITY_DEGRADATION_DETECTED') {
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_DEGRADED',
          reason: action.reason,
          evidence: action.evidence,
        });
      }
      // Do not pass to subscribers — handled
    } else if (action.type === 'CAPABILITY_RECOVERED') {
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'CAPABILITY_RECOVERED',
          reason: `Restored from ${action.previousState}`,
          evidence: action.evidence,
        });
      }
      // Do not pass to subscribers — handled
    } else {
      filteredActions.push(action);
    }
  }

  console.log(`[graph-capability-fsm] ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
    actions: filteredActions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Initialization — called by constitutional kernel on boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initialize the domain FSM with rehydrated state from lineage.
 * Called by the constitutional kernel after rehydrate() completes on boot.
 *
 * @param {string} rehydratedState — the domain state to restore
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string' && STATE_REGISTRY[rehydratedState]) {
    _localState = rehydratedState;
    console.log(`[graph-capability-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  } else {
    console.log(`[graph-capability-fsm] No valid rehydrated state — starting in UNKNOWN`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return {
    state: _localState,
    lastTransitionedAt: _lastTransitionedAt,
    lastObservedAt: _lastObservedAt,
    lastEvidence: _lastEvidence,
    consecutiveFailures: _consecutiveFailures,
  };
}

function getHealth() {
  const isFresh = _lastObservedAt
    ? (Date.now() - _lastObservedAt) < OBSERVATION_FRESHNESS_MS
    : false;
  const isStale = _lastObservedAt
    ? (Date.now() - _lastObservedAt) > DEGRADED_OBSERVATION_MS
    : true;
  return {
    ok: _localState === 'AUTHORIZED' && isFresh && _consecutiveFailures === 0,
    signals: {
      state: _localState,
      observationFresh: isFresh,
      observationStale: isStale,
      consecutiveFailures: _consecutiveFailures,
    },
  };
}

// ── Public capability verdict — the constitutional truth ──────────────────────

/**
 * Return the current canonical capability verdict for downstream consumers.
 * This is the single source of truth that acquisition/publishing/engagement consume.
 *
 * @returns {{ state: string, observedAt: number|null, evidence: object|null }}
 */
function getCapabilityVerdict() {
  return {
    state: _localState,
    observedAt: _lastObservedAt,
    evidence: _lastEvidence,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Trigger Criteria — cognitive interface to CK dispatch membrane
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Deterministic trigger evaluation — the FSM's cognitive interface to CK's dispatch membrane.
 * CK calls this before dispatching CAPABILITY_EVALUATE for every trigger condition.
 *
 * @param {{ trigger: string, forced?: boolean }} params
 * @returns {{ decision: 'APPROVED'|'DENIED'|'WAIT', reason: string, retryAt?: number }}
 */
function evaluateTriggerCriteria({ trigger = 'MANUAL', forced = false } = {}) {
  // Gate 0 (Layer 3.3): Worker observation arrived — must fire BEFORE the
  // UNKNOWN-WAIT check, because an observation IS the act of evaluating.
  // Without this priority, observations received while state is UNKNOWN
  // would never trigger the aggregator.
  if (trigger === 'OBSERVATION_ARRIVED') {
    return { decision: 'APPROVED', reason: 'Worker observation arrived — aggregate and derive' };
  }

  // Gate 1: Already in evaluation
  if (_localState === 'UNKNOWN' && !forced) {
    return { decision: 'WAIT', reason: 'Evaluation in progress (UNKNOWN)' };
  }

  // Gate 2: Force override
  if (forced) {
    return { decision: 'APPROVED', reason: 'Forced trigger — bypassing gates' };
  }

  // Gate 3: Auth failure trigger — always approved
  if (trigger === 'AUTH_FAILURE_STRIKE') {
    return { decision: 'APPROVED', reason: 'Auth failure strike — mandatory evaluation' };
  }

  // Gate 4: Repeated Graph failure — always approved
  if (trigger === 'REPEATED_GRAPH_FAILURE') {
    return { decision: 'APPROVED', reason: 'Repeated Graph failure — degradation assessment' };
  }

  // Gate 5: New account connected — always approved
  if (trigger === 'NEW_ACCOUNT_CONNECTED') {
    return { decision: 'APPROVED', reason: 'New account — full capability sweep' };
  }

  // Gate 6: Token refreshed — always approved
  if (trigger === 'TOKEN_REFRESHED') {
    return { decision: 'APPROVED', reason: 'Token refreshed — capability re-evaluation' };
  }

  // Gate 7: Cadence tick — approved unless UNAUTHORIZED and recent
  if (trigger === 'CADENCE_TICK') {
    if (_localState === 'UNAUTHORIZED' && _lastTransitionedAt) {
      const elapsed = Date.now() - _lastTransitionedAt;
      if (elapsed < OBSERVATION_FRESHNESS_MS) {
        return { decision: 'WAIT', reason: `Recent failure (${elapsed}ms ago) — awaiting cooldown` };
      }
    }
    return { decision: 'APPROVED', reason: 'Cadence tick — scheduled re-evaluation' };
  }

  // Default: approve for any recognized trigger
  return { decision: 'APPROVED', reason: `Trigger ${trigger} approved` };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Internal helpers
// ═══════════════════════════════════════════════════════════════════════════════

function _isObservationFresh(observedAt) {
  if (!observedAt) return false;
  const ts = typeof observedAt === 'number' ? observedAt : new Date(observedAt).getTime();
  if (isNaN(ts)) return false;
  return (Date.now() - ts) < OBSERVATION_FRESHNESS_MS;
}

// ── Layer 3.2: Aggregate observation → derived state event ─────────────────
// Called by the CAPABILITY_OBSERVATION transition's buildActions. Normalizes
// the envelope and dispatches a synthesized event (CAPABILITY_OK /
// CAPABILITY_PARTIAL / CAPABILITY_FAILED). Recursion is bounded to depth 2
// (CAPABILITY_OBSERVATION → derived event); the derived event's transition
// is terminal in the recursive sense.
//
// The synthesized event passes through the full dispatch pipeline
// (guard → validate → transition), so the constitutional gate still has
// veto authority over the derived transition.
//
// Note: observations.js is lazy-loaded to break a circular dependency
// (fsm.js ↔ observations.js — observations requires fsm.REQUIRED_SCOPES).
let _observations = null;
function _getObservations() {
  if (!_observations) {
    _observations = require('./substrates/graph-capability/observations');
  }
  return _observations;
}

function _aggregateAndDispatch(envelope, ctx) {
  let normalized;
  try {
    normalized = _getObservations().normalize(envelope);
  } catch (err) {
    // If normalization fails, do not derive — the observation is incomplete
    // and the FSM should stay where it is.
    return { allowed: false, reason: `normalization failed: ${err.message}` };
  }

  const { state, observedAt, evidence, reason, missingScopes } = normalized;
  const event = {
    type: null,
    observedAt,
    evidence,
    reason,
    missingScopes,
    businessAccountId: envelope.businessAccountId || null,
    userId: envelope.userId || null,
  };

  if (state === 'AUTHORIZED') {
    event.type = 'CAPABILITY_OK';
  } else if (state === 'LIMITED') {
    event.type = 'CAPABILITY_PARTIAL';
  } else if (state === 'UNAUTHORIZED') {
    event.type = 'CAPABILITY_FAILED';
  } else if (state === 'DEGRADED') {
    event.type = 'CAPABILITY_DEGRADED';
  } else {
    // UNKNOWN or any unhandled state — do not derive a transition.
    return { allowed: false, reason: `aggregation produced no derivable event (state=${state})` };
  }

  return dispatch(event, ctx);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 9. Public API
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  // Standard FSM contract
  setGovernance,
  getGovernance,
  registerWorker,
  getWorker,
  getWorkers,
  name: 'graph-capability-fsm',
  dispatch,
  getState,
  exportState,
  getHealth,
  init,
  evaluateTriggerCriteria,
  // Capability-specific public surface
  getCapabilityVerdict,
  // Introspection (for tests + migration verification)
  STATE_REGISTRY,
  REQUIRED_SCOPES,
  OBSERVATION_FRESHNESS_MS,
  DEGRADED_OBSERVATION_MS,
};
