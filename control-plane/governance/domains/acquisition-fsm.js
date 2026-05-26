// control-plane/governance/domains/acquisition-fsm.js
// Acquisition Domain FSM: federated state machine governing acquisition lifecycle.
//
// Owns: intent discovery → execution → completion lifecycle,
//        retry decisions, auth strike tracking, circuit breaker engagement,
//        execution state tracking.
// Does NOT own: global lifecycle, cross-domain invariants, execution mechanics
//               (delegated to membranes and substrates).
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
//   Signals UP   → ctx.dispatchGlobal(event) reports degradation to constitutional
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Membranes ↓  → actions returned to constitutional for emission to orchestrators
//   Lineage      → ctx.recordLineage() writes to authoritative ledger (via CK mediation)
//
// Domain FSMs CANNOT directly access the lineage ledger.
// The constitutional kernel mediates all lineage writes.
//
// Local states:
//   IDLE       — no acquisition in progress
//   ACQUIRING  — acquisition intent received, execution in flight

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Execution Policy Constants — domain-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const AUTH_FAILURE_MAX_STRIKES = 3;
const MAX_ACQUISITION_RETRIES = 1;
const CIRCUIT_BREAKER_COOLDOWN_MS = 3600000; // 1 hour

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

  // ── Execution observations — domain-owned execution intelligence ─────────
  // Substrates emit raw observations upward. The domain FSM alone decides
  // retry, auth escalation, circuit breaker engagement, and permanent failure.
  // This is the constitutional boundary: substrates observe, FSM decides.

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
      const { accountId, domain, intentId, status, error_category, retryable, count, latencyMs } = event;

      // Track execution state for retry decisions
      _executionState.set(intentId, { accountId, domain, lastError: event.error || null });

      // ── Success → clear everything, return result ───────────────────────
      if (status === 'completed') {
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        if (_authFailureStrikes.has(accountId)) {
          _authFailureStrikes.delete(accountId);
        }
        return [{
          type: 'WRITE_ACQUISITION_RESULT',
          accountId, domain, intentId,
          result: { status: 'completed', count: count || 0 },
        }];
      }

      // ── Auth failure → increment strikes, evaluate disconnect threshold ─
      if (error_category === 'auth_failure') {
        const strikes = (_authFailureStrikes.get(accountId) || 0) + 1;
        _authFailureStrikes.set(accountId, strikes);
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);

        if (strikes >= AUTH_FAILURE_MAX_STRIKES) {
          return [
            { type: 'DISCONNECT_ACCOUNT', accountId, reason: `Auth failure strike ${strikes}/${AUTH_FAILURE_MAX_STRIKES}` },
            { type: 'CREATE_SYSTEM_ALERT', alertType: 'auth_failure', accountId,
              message: `Acquisition auth failure: ${event.error || 'unknown'}`,
              details: { source: 'execution_bridge', error: event.error, strikes } },
          ];
        }
        return [{ type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE', reason: `Auth failure strike ${strikes}/${AUTH_FAILURE_MAX_STRIKES} for ${accountId}` }];
      }

      // ── Rate limit → engage circuit breaker ─────────────────────────────
      if (error_category === 'rate_limit') {
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        return [
          { type: 'ENGAGE_CIRCUIT_BREAKER', accountId, domain, cooldownMs: CIRCUIT_BREAKER_COOLDOWN_MS },
          { type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE', reason: `Rate limit detected for ${accountId}/${domain}` },
        ];
      }

      // ── Transient/retryable → increment retry count, decide retry or exhaust
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

        // Retries exhausted
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        return [
          { type: 'MARK_PERMANENT_FAILURE', accountId, domain, intentId, error: 'max_retries_exceeded' },
          { type: 'CREATE_SYSTEM_ALERT', alertType: 'retry_exhausted', accountId,
            message: `Acquisition retries exhausted for ${domain}/${accountId} intent ${intentId}`,
            details: { domain, intentId, attempts: retryCount, lastError: event.error } },
        ];
      }

      // ── Permanent failure (non-retryable, non-auth, non-rate) ───────────
      _executionRetries.delete(intentId);
      _executionState.delete(intentId);
      return [{
        type: 'MARK_PERMANENT_FAILURE', accountId, domain, intentId, error: event.error || 'unknown',
      }];
    },
  },

  // ── Auth failure strike (emitted by retry substrate) ────────────────────
  AUTH_FAILURE_STRIKE: {
    target: (event) => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, error } = event;
      const strikes = (_authFailureStrikes.get(accountId) || 0) + 1;
      _authFailureStrikes.set(accountId, strikes);

      if (strikes >= AUTH_FAILURE_MAX_STRIKES) {
        return [
          { type: 'DISCONNECT_ACCOUNT', accountId, reason: `Auth failure strikes exceeded: ${strikes}` },
          { type: 'CREATE_SYSTEM_ALERT', alertType: 'auth_failure', accountId,
            message: `Account disconnected: ${strikes} auth failures. Last error: ${error || 'unknown'}`,
            details: { source: 'retry_substrate', error, strikes } },
        ];
      }
      return [];
    },
  },

  // ── Rate limit detected (emitted by retry substrate) ───────────────────
  RATE_LIMIT_DETECTED: {
    target: (event) => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, retryAfterSeconds } = event;
      const cooldownMs = (retryAfterSeconds || 3600) * 1000;
      return [
        { type: 'ENGAGE_CIRCUIT_BREAKER', accountId, cooldownMs },
        { type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE',
          reason: `Rate limit for ${accountId}, cooldown ${cooldownMs / 1000}s` },
      ];
    },
  },

  // ── Retry exhausted (emitted when all retries consumed) ─────────────────
  RETRY_EXHAUSTED: {
    target: (event) => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, domain, intentId, error } = event;
      _executionRetries.delete(intentId);
      _executionState.delete(intentId);
      return [
        { type: 'MARK_PERMANENT_FAILURE', accountId, domain, intentId, error: error || 'retry_exhausted' },
        { type: 'CREATE_SYSTEM_ALERT', alertType: 'retry_exhausted', accountId,
          message: `Retries exhausted for ${domain}/${accountId}`,
          details: { domain, intentId, error } },
      ];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';

// ── Execution state tracking ─────────────────────────────────────────────────
const _authFailureStrikes = new Map();  // accountId → strike count
const _circuitBreakers = new Map();     // accountId → { until: timestampMs }
const _executionRetries = new Map();    // intentId → retry count
const _executionState = new Map();      // intentId → { accountId, domain, lastError }

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch — process event, ask constitutional for validation, transition
//
// Write order invariant (Lineage-First):
//   1. ctx.recordLineage() — write to authoritative ledger via CK mediation
//   2. _localState mutation — then materialize domain state
//
// Domain FSMs CANNOT directly access the lineage ledger.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the acquisition FSM.
 *
 * @param {{ type: string, [key: string]: any }} event — domain event
 * @param {{ validate: Function, recordLineage: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx — constitutional kernel context
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

  // null target = no state change, record lineage only
  if (target === null) {
    // Record lineage first
    if (ctx && ctx.recordLineage) {
      ctx.recordLineage({
        authority: 'acquisition-fsm',
        layer: 'domain',
        intent: event.type,
        priorState: from,
        resultantState: from,
        meta: { accountId: event.accountId || null, domain: event.domain || null, intentId: event.intentId || null },
      });
    }
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition: event recorded' };
  }

  // 3. Ask constitutional kernel for transition approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // 4. LINEAGE FIRST — record to authoritative ledger before mutating state
  let lineageId = null;
  if (ctx && ctx.recordLineage) {
    const entry = {
      authority: 'acquisition-fsm',
      layer: 'domain',
      intent: event.type,
      priorState: from,
      resultantState: target,
      meta: { accountId: event.accountId || null, domain: event.domain || null, intentId: event.intentId || null },
    };
    const recorded = ctx.recordLineage(entry);
    lineageId = recorded.id || recorded.lineageId || null;
  }

  // 5. THEN materialize state
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
    lineageId,
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
    authFailureAccounts: _authFailureStrikes.size,
    activeCircuitBreakers: Array.from(_circuitBreakers.values()).filter(c => c.until > Date.now()).length,
    pendingRetries: _executionRetries.size,
  };
}

function getHealth() {
  const breakerCount = Array.from(_circuitBreakers.values()).filter(c => c.until > Date.now()).length;
  return {
    ok: _authFailureStrikes.size === 0 && breakerCount === 0 && _executionRetries.size < 10,
    signals: {
      authFailures: _authFailureStrikes.size,
      activeBreakers: breakerCount,
      pendingRetries: _executionRetries.size,
    },
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Domain-specific state queries (called by acquisition orchestrator membrane)
// ═══════════════════════════════════════════════════════════════════════════════

function isCircuitBreakerActive(accountId) {
  const breaker = _circuitBreakers.get(accountId);
  if (!breaker) return false;
  if (Date.now() >= breaker.until) {
    _circuitBreakers.delete(accountId);
    return false;
  }
  return true;
}

function getAuthStrikes(accountId) {
  return _authFailureStrikes.get(accountId) || 0;
}

function getRetryCount(intentId) {
  return _executionRetries.get(intentId) || 0;
}

function resetAuthStrikes(accountId) {
  _authFailureStrikes.delete(accountId);
}

function clearCircuitBreaker(accountId) {
  _circuitBreakers.delete(accountId);
}

// ── Reconciliation engine getters — expose domain state for three-reality comparison ──

function getCircuitBreakers() {
  return new Map(_circuitBreakers);
}

function getExecutionRetries() {
  return new Map(_executionRetries);
}

function getAuthStrikeMap() {
  return new Map(_authFailureStrikes);
}

module.exports = {
  name: 'acquisition',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  isCircuitBreakerActive,
  getAuthStrikes,
  getRetryCount,
  resetAuthStrikes,
  clearCircuitBreaker,
  getCircuitBreakers,
  getExecutionRetries,
  getAuthStrikeMap,
};
