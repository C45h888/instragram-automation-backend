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
    try { _observability = require('../../observability/emitters/transition-emitter'); }
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
// 0. Execution Policy Constants — domain-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const MAX_ACQUISITION_RETRIES = 1;

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
  ACQUISITION_INTENT_RECEIVED: {
    target: 'ACQUIRING',
    guard: (event) => {
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot acquire from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'EXECUTE_ACQUISITION',
      accountId: event.accountId,
      domain: event.domain,
      intentId: event.intentId,
      params: event.params,
    }],
  },

  // ── Execution started → stop intent discovery ───────────────────────────
  ACQUISITION_EXECUTING: {
    target: 'ACQUIRING', // stays in ACQUIRING — execution in progress
    guard: (event) => {
      if (_localState !== 'ACQUIRING') {
        return { allowed: false, reason: `Cannot execute from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: () => [{
      type: 'STOP_INTENT_DISCOVERY',
    }],
  },

  // ── Acquisition complete (success or permanent failure) → back to IDLE ──
  ACQUISITION_COMPLETE: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'ACQUIRING') {
        return { allowed: false, reason: `Cannot complete from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const actions = [];
      if (event.result) {
        actions.push({
          type: 'WRITE_ACQUISITION_RESULT',
          accountId: event.accountId,
          domain: event.domain,
          intentId: event.intentId,
          result: event.result,
        });
      }
      actions.push({ type: 'START_INTENT_DISCOVERY' });
      return actions;
    },
  },

  // ── Execution observations — intent lifecycle only ──────────────────────────
// Constitutional purity: acquisition-fsm owns ONLY intent lifecycle (IDLE ↔ ACQUIRING).
// Engagement signals (auth_failure, rate_limit, retry_exhausted) are emitted by
// retry-worker/execution-bridge directly to CK. DOMAIN_EVENT_MAP routes them to
// engagement-fsm independently. Acquisition-fsm never emits engagement-domain events.

  EXECUTION_OBSERVATION: {
    target: (event) => {
      if (event.status === 'completed') return 'IDLE';
      if (!event.retryable && event.status !== 'completed') return 'IDLE';
      return _localState; // stay in ACQUIRING for retryable
    },
    guard: (event) => {
      if (_localState !== 'ACQUIRING') {
        return { allowed: false };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { accountId, domain, intentId, status, error_category, retryable, count } = event;

      // Track execution state for retry decisions
      _executionState.set(intentId, { accountId, domain, lastError: event.error || null });

      // ── Success → acquisition lifecycle complete ───────────────────────
      if (status === 'completed') {
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        // AUTH_SUCCESS is emitted by retry-worker/execution-bridge directly to CK
        // DOMAIN_EVENT_MAP routes it to engagement-fsm independently
        return [{
          type: 'WRITE_ACQUISITION_RESULT',
          accountId, domain, intentId,
          result: { status: 'completed', count: count || 0 },
        }];
      }

      // ── Auth failure → engagement-fsm handles via CK routing ────────────
      if (error_category === 'auth_failure') {
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        // AUTH_FAILURE_STRIKE is emitted by retry-worker/execution-bridge directly to CK
        return [];
      }

      // ── Rate limit → engagement-fsm handles via CK routing ──────────────
      if (error_category === 'rate_limit') {
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        // RATE_LIMIT_DETECTED is emitted by retry-worker/execution-bridge directly to CK
        return [{ type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE',
                  reason: `Rate limit for ${accountId}/${domain} — engagement-fsm manages via CK routing` }];
      }

      // ── Transient/retryable → acquisition decides retry ─────────────────
      if (retryable) {
        const retryCount = (_executionRetries.get(intentId) || 0) + 1;
        _executionRetries.set(intentId, retryCount);

        if (retryCount <= MAX_ACQUISITION_RETRIES) {
          const delayMs = event.retryAfterMs || 30000;
          return [{
            type: 'RETRY_ACQUISITION',
            accountId, domain, intentId,
            params: { domain, intent_id: intentId, payload: event.params || {} },
            retryCount,
            delayMs: Math.min(delayMs, 300000),
          }];
        }

        // Retries exhausted → engagement-fsm receives RETRY_EXHAUSTED via CK routing
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        return [];
      }

      // ── Permanent failure (non-retryable, non-auth, non-rate) ───────────
      _executionRetries.delete(intentId);
      _executionState.delete(intentId);
      return [{ type: 'MARK_PERMANENT_FAILURE', accountId, domain, intentId,
                error: event.error || 'unknown' }];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';

// ── Execution state tracking ─────────────────────────────────────────────────
const _executionRetries = new Map();    // intentId → retry count
const _executionState = new Map();      // intentId → { accountId, domain, lastError }

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
  const actions = txn.buildActions ? txn.buildActions(event) : [];

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
    pendingRetries: _executionRetries.size,
  };
}

function getHealth() {
  return {
    ok: _executionRetries.size < 10,
    signals: {
      pendingRetries: _executionRetries.size,
    },
  };
}

// ── Reconciliation engine getters ───────────────────────────────────────────

function getExecutionRetries() {
  return new Map(_executionRetries);
}

module.exports = {
  name: 'acquisition',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getExecutionRetries,
};
