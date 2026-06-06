// control-plane/governance/domains/acquisition-fsm.js
// Acquisition Domain FSM: federated state machine governing acquisition lifecycle.
//
// Owns: intent discovery → execution → completion lifecycle ONLY.
// Does NOT own: engagement signals (auth strikes, circuit breakers, retry counting),
//               cross-domain event emission, execution mechanics.
//
// Constitutional purity: acquisition-fsm is a PURE intent lifecycle domain.
// Engagement signals (AUTH_FAILURE_STRIKE, RATE_LIMIT_DETECTED, RETRY_EXHAUSTED,
// AUTH_SUCCESS, RETRY_COUNT_INCREMENTED) are emitted by retry-worker/execution-bridge
// directly to CK. DOMAIN_EVENT_MAP routes them to engagement-fsm independently.
// Acquisition-fsm never emits engagement-domain events.
//
// Reports to: constitutional kernel for transition validation + global observability.

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../control-plane/observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

//
// Architectural invariant:
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Membranes ↓  → actions returned to constitutional for emission to orchestrators
//
// Acquisition-fsm is a PURE intent lifecycle domain. It does NOT emit cross-domain
// events. Engagement signals are emitted by retry-worker/execution-bridge directly
// to CK and routed via DOMAIN_EVENT_MAP to engagement-fsm.
//
// Domain FSMs emit state transitions through the observability plane.
// The lineage worker consumes from the observability plane and writes to the
// canonical lineage ledger. FSMs do NOT write to the lineage ledger directly.
//
// Local states:
//   IDLE       — no acquisition in progress
//   ACQUIRING  — acquisition intent received, execution in flight

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Execution Policy Constants — retry policy moved to retry-cadence/policy.js
// ═══════════════════════════════════════════════════════════════════════════════

// MAX_ACQUISITION_RETRIES removed — retry-cadence substrate owns per-substrate policy.

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No acquisition in progress — ready for intents',
  },
  ACQUIRING: {
    description: 'Acquisition intent received, execution in flight',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map — event → target + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Intent received → begin acquisition ─────────────────────────────────
  // Gated by ctx.sanityCheck (universal gate). When the system
  // is DEGRADED, the gate can veto EXECUTE_ACQUISITION. The
  // orchastrator subscriber won't fire, the substrate won't run.
  // Telemetry is preserved via GATE_REJECTED emission.
  ACQUISITION_INTENT_RECEIVED: {
    target: 'ACQUIRING',
    guard: (event) => {
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot acquire from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const gate = await _resolveSanityCheck(ctx, {
        operation: 'execute_acquisition',
        accountId: event.accountId,
        domain: event.domain,
        intentId: event.intentId,
      });
      if (!gate.allowed) {
        return [{
          type: 'GATE_REJECTED',
          operation: 'execute_acquisition',
          accountId: event.accountId,
          domain: event.domain,
          intentId: event.intentId,
          reason: gate.reason || 'gate_rejected',
        }];
      }
      return [{
        type: 'EXECUTE_ACQUISITION',
        accountId: event.accountId,
        domain: event.domain,
        intentId: event.intentId,
        params: event.params,
      }];
    },
  },

  // ── Execution started → stop intent discovery ───────────────────────────
  // No gate on STOP_INTENT_DISCOVERY — system-level restart signal.
  // Gating it could leave the system stuck.
  ACQUISITION_EXECUTING: {
    target: 'ACQUIRING', // stays in ACQUIRING — execution in progress
    guard: (event) => {
      if (_localState !== 'ACQUIRING') {
        return { allowed: false, reason: `Cannot execute from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async () => [{
      type: 'STOP_INTENT_DISCOVERY',
    }],
  },

  // ── Acquisition complete (success or permanent failure) → back to IDLE ──
  // Gated on WRITE_ACQUISITION_RESULT emission. The orchastrator
  // subscriber writes to Redis; the gate veto means no write.
  // GATE_REJECTED preserves telemetry for the veto.
  ACQUISITION_COMPLETE: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'ACQUIRING') {
        return { allowed: false, reason: `Cannot complete from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const actions = [];
      if (event.result) {
        const gate = await _resolveSanityCheck(ctx, {
          operation: 'write_acquisition_result',
          accountId: event.accountId,
          domain: event.domain,
          intentId: event.intentId,
        });
        if (!gate.allowed) {
          actions.push({
            type: 'GATE_REJECTED',
            operation: 'write_acquisition_result',
            accountId: event.accountId,
            domain: event.domain,
            intentId: event.intentId,
            reason: gate.reason || 'gate_rejected',
          });
        } else {
          actions.push({
            type: 'WRITE_ACQUISITION_RESULT',
            accountId: event.accountId,
            domain: event.domain,
            intentId: event.intentId,
            result: event.result,
          });
        }
      }
      actions.push({ type: 'START_INTENT_DISCOVERY' });
      return actions;
    },
  },

  // ── Parsing dispatched → parsing worker is running asynchronously ───────────
  // No gate (no emission, just sets _pendingParsing Map).
  PARSING_DISPATCHED: {
    target: () => _localState, // stays in ACQUIRING — wait for PARSING_COMPLETE
    guard: (event) => {
      if (_localState !== 'ACQUIRING') {
        return { allowed: false, reason: `Cannot dispatch parsing from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event) => {
      _pendingParsing.set(event.intentId, {
        jobId: event.jobId,
        domain: event.domain,
        accountId: event.accountId,
        rawCount: event.rawCount || 0,
      });
      return [];
    },
  },

  // ── Parsing complete → worker finished, transition to IDLE ──────────────────
  // Gated on RETRY_EXHAUSTED emission (terminal failure path).
  // GATE_REJECTED preserves telemetry if the gate vetoes the
  // terminal signal.
  PARSING_COMPLETE: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'ACQUIRING') {
        return { allowed: false, reason: `Cannot complete parsing from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { accountId, domain, intentId, result } = event;
      _pendingParsing.delete(intentId);

      if (result.status === 'failed') {
        const gate = await _resolveSanityCheck(ctx, {
          operation: 'parsing_failed_retry_exhausted',
          accountId,
          domain,
          intentId,
        });
        if (!gate.allowed) {
          return [{
            type: 'GATE_REJECTED',
            operation: 'parsing_failed_retry_exhausted',
            accountId,
            domain,
            intentId,
            reason: gate.reason || 'gate_rejected',
          }];
        }
        return [{
          type: 'RETRY_EXHAUSTED',
          accountId,
          domain,
          intentId,
          error: result.error || 'parsing_failed',
        }];
      }

      return [{
        type: 'START_INTENT_DISCOVERY',
      }];
    },
  },

  // ── Execution observations — intent lifecycle only ──────────────────────────
// Constitutional purity: acquisition-fsm owns ONLY intent lifecycle (IDLE ↔ ACQUIRING).
// Engagement signals (auth_failure, rate_limit, retry_exhausted) are emitted by
// retry-worker/execution-bridge directly to CK. DOMAIN_EVENT_MAP routes them to
// engagement-fsm independently. Acquisition-fsm never emits engagement-domain events.

  // ── EXECUTION_OBSERVATION — REMOVED in Step 4 of authority centralisation ─
  // Acquisition-fsm no longer classifies worker outcomes. Workers emit
  // WORKER_OUTCOME_REPORTED, which routes to engagement-fsm. engagement-fsm
  // calls the classification-worker, decides the action, and emits the
  // downstream signal (RETRY_REQUESTED, AUTH_FAILURE_STRIKE, RATE_LIMIT_DETECTED,
  // RETRY_EXHAUSTED). Acquisition-fsm receives those via the existing handlers.
  //
  // The lifecycle here is:
  //   1. PARSING_DISPATCHED (worker reports parsing job started)
  //   2. PARSING_COMPLETE   (parsing job finished — success or failure)
  //   3. ACQUISITION_COMPLETE (terminal — success or permanent failure)
  //
  // RETRY_EXHAUSTED arriving from engagement-fsm carries the terminal
  // signal for the failure path. It routes to ACQUISITION_COMPLETE
  // via the existing subscriber wiring (acquisition-orchestrator).
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _lastTransitionedAt = null; // last state change timestamp for temporal alignment in reconciliation

// ── Execution state tracking ─────────────────────────────────────────────────
// _pendingParsing is the ONLY execution map acquisition-fsm still owns.
// It tracks parsing jobs that have been dispatched but not yet completed.
// _executionRetries and _executionState were removed in Step 3 — retry
// counting moved to retry-cadence substrate, error state to engagement-fsm.
const _pendingParsing = new Map();      // intentId → { jobId, domain, accountId, rawCount }

// ── Default fail-open sanity check (universal gate pattern) ─────────────
// The ctx.sanityCheck is the universal gate. The FSM calls it
// during emission. For tests / non-CK dispatch, the default is
// always-allowed (fail-open to preserve operational cadence).
const _defaultSanityCheck = async () => ({ allowed: true });

function _resolveSanityCheck(ctx, action) {
  if (ctx && typeof ctx.sanityCheck === 'function') {
    return ctx.sanityCheck(action);
  }
  return _defaultSanityCheck(action);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch — process event, ask constitutional for validation, transition
//
// Domain FSMs emit through observability plane (not lineage ledger).
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the acquisition FSM.
 *
 * @param {{ type: string, [key: string]: any }} event — domain event
 * @param {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx — constitutional kernel context
 * @returns {{ allowed: boolean, from?: string, to?: string, lineageId?: string, actions?: Array, reason?: string }}
 */
async function dispatch(event, ctx) {
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

  // null target = no state change
  if (target === null) {
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition' };
  }

  // 3. Ask constitutional kernel for transition approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // 4. THEN materialize state
  _localState = target;
  _lastTransitionedAt = Date.now();

  // 6. Emit observability transition for domain FSM state change
  // Fire-and-forget — observability failures never affect domain FSM behavior
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'acquisition',
        entity: 'fsm',
        entityId: 'acquisition-fsm',
        previousState: from,
        nextState: target,
        authority: 'acquisition-fsm',
        raw: { intent: event.type, intentId: event.intentId || null, accountId: event.accountId || null },
      });
    }
  } catch (_) {}

  // 7. Build actions
  const actions = (txn.buildActions ? await txn.buildActions(event, ctx) : []);

  console.log(`[acquisition-fsm] ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
    actions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Initialization — called by constitutional kernel on boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initialize the domain FSM with rehydrated state from lineage.
 * Called by the constitutional kernel after rehydrate() completes on boot.
 *
 * @param {string} rehydratedState — the domain state to restore (e.g., 'ACQUIRING', 'IDLE')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[acquisition-fsm] Initialized with rehydrated state: ${rehydratedState}`);
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
    pendingParsing: _pendingParsing.size,
  };
}

function getHealth() {
  // pendingParsing is the only execution map. A large count means
  // parsing workers are stuck (no PARSING_COMPLETE arriving). Step 3
  // of T5 in the plan: add timeout/cleanup. For now, signal at 10.
  return {
    ok: _pendingParsing.size < 10,
    signals: {
      pendingParsing: _pendingParsing.size,
    },
  };
}

// ── Reconciliation engine getters ───────────────────────────────────────────

function getLastTransitionedAt() {
  return _lastTransitionedAt;
}

function getPendingParsing() {
  return new Map(_pendingParsing);
}

module.exports = {
  name: 'acquisition',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getLastTransitionedAt,
  getPendingParsing,
};