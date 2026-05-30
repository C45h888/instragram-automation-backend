// control-plane/governance/domains/engagement-fsm.js
// Engagement Domain FSM: federated state machine governing engagement lifecycle.
//
// Owns: circuit breaker lifecycle (OPEN/COOLING/CLOSED),
//        auth strike tracking and escalation (0-3 strikes),
//        retry counting and exhaustion detection per intent.
// Does NOT own: acquisition lifecycle, publication pipeline,
//               scheduling cadence, dedup mechanics,
//               error classification (substrates), execution mechanics.
//
// Reports to: constitutional kernel for transition validation + global observability.
//
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) reports degradation to constitutional
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Substrate ↓  → retry-substrate performs mechanical mark/clear operations
//                  FSM governs lifecycle meaning, substrate performs mechanics
//
// Domain FSMs emit state transitions through the observability plane.
// The lineage worker consumes from the observability plane and writes to the
// canonical lineage ledger. FSMs do NOT write to the lineage ledger directly.
//
// Local states:
//   IDLE            — no active circuit breakers, no auth strikes, no retry exhaustion
//   CIRCUIT_OPEN    — rate limit detected, circuit breaker engaged, waiting cooldown
//   CIRCUIT_COOLING — cooldown elapsed, allowing test request through
//   AUTH_STRIKING   — auth failures accumulated (1-2 strikes), account at risk
//   AUTH_EXHAUSTED  — 3 auth strikes reached, account disconnected
//   RETRY_EXHAUST   — per-intent retry budget consumed, returning permanent failure

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
// 0. Governance Policy Constants — domain-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const AUTH_FAILURE_MAX_STRIKES = 3;
const CIRCUIT_BREAKER_COOLDOWN_MS = 3600000; // 1 hour default
const MAX_RETRY_COUNT = 1;

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No active circuit breakers, no auth strikes, no retry exhaustion pending',
  },
  CIRCUIT_OPEN: {
    description: 'Circuit breaker engaged — rate limit hit, cooldown period running',
  },
  CIRCUIT_COOLING: {
    description: 'Cooldown elapsed, allowing test request through',
  },
  AUTH_STRIKING: {
    description: 'Auth failures accumulated (1-2 strikes), account at risk of disconnect',
  },
  AUTH_EXHAUSTED: {
    description: '3 auth strikes reached — account disconnected',
  },
  RETRY_EXHAUST: {
    description: 'Per-intent retry budget consumed — permanent failure returned',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map — event → target + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Rate limit detected → circuit breaker lifecycle ──────────────────
  RATE_LIMIT_DETECTED: {
    target: 'CIRCUIT_OPEN',
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, cooldownMs = CIRCUIT_BREAKER_COOLDOWN_MS } = event;

      // Record or update circuit breaker state
      const existing = _circuitBreakers.get(accountId);
      _circuitBreakers.set(accountId, {
        until: Date.now() + cooldownMs,
        cooldownMs,
        openedAt: existing ? existing.openedAt : Date.now(),
        reopenedAt: existing ? Date.now() : null,
      });

      return [
        {
          type: 'ENGAGE_CIRCUIT_BREAKER',
          accountId,
          cooldownMs,
          authority: 'engagement-fsm',
        },
        {
          type: 'CLEAR_CREDENTIAL_CACHE',
          accountId,
          reason: 'rate_limit_detected',
        },
        {
          type: 'LOG_DEGRADED',
          substate: 'PARTIAL_FAILURE',
          reason: `Circuit breaker OPEN for ${accountId}, cooldown ${cooldownMs / 1000}s`,
        },
      ];
    },
  },

  // ── Circuit breaker cooldown elapsed → advance to cooling ──────────────
  CIRCUIT_COOLDOWN_ELAPSED: {
    target: 'CIRCUIT_COOLING',
    guard: (event) => {
      const { accountId } = event;
      const breaker = _circuitBreakers.get(accountId);
      if (!breaker) {
        return { allowed: false, reason: `No active circuit breaker for ${accountId}` };
      }
      if (Date.now() < breaker.until) {
        return { allowed: false, reason: `Cooldown not yet elapsed for ${accountId}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { accountId } = event;
      return [{ type: 'CIRCUIT_TEST_REQUEST', accountId }];
    },
  },

  // ── Circuit test succeeded → back to IDLE ───────────────────────────────
  CIRCUIT_TEST_SUCCESS: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'CIRCUIT_COOLING') {
        return { allowed: false, reason: `Can only succeed from CIRCUIT_COOLING, got ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      _circuitBreakers.delete(event.accountId);
      return [];
    },
  },

  // ── Circuit test failed → re-trip ──────────────────────────────────────
  CIRCUIT_TEST_FAIL: {
    target: 'CIRCUIT_OPEN',
    guard: (event) => {
      if (_localState !== 'CIRCUIT_COOLING') {
        return { allowed: false, reason: `Can only re-trip from CIRCUIT_COOLING, got ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { accountId } = event;
      const breaker = _circuitBreakers.get(accountId);
      const newCooldown = breaker?.cooldownMs || CIRCUIT_BREAKER_COOLDOWN_MS;
      const until = Date.now() + newCooldown;

      _circuitBreakers.set(accountId, {
        until,
        cooldownMs: newCooldown,
        openedAt: breaker?.openedAt || Date.now(),
        reopenedAt: Date.now(),
      });

      return [
        {
          type: 'ENGAGE_CIRCUIT_BREAKER',
          accountId,
          cooldownMs: newCooldown,
          authority: 'engagement-fsm',
        },
      ];
    },
  },

  // ── Manual circuit breaker cleared → IDLE ──────────────────────────────
  CIRCUIT_BREAKER_CLEARED: {
    target: 'IDLE',
    guard: (event) => {
      const { accountId } = event;
      if (!_circuitBreakers.has(accountId)) {
        return { allowed: false, reason: `No active circuit breaker for ${accountId}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      _circuitBreakers.delete(event.accountId);
      return [];
    },
  },

  // ── Auth failure strike accumulated → auth strike lifecycle ────────────
  AUTH_FAILURE_STRIKE: {
    target: (event) => {
      const strikes = (_authFailureStrikes.get(event.accountId) || 0) + 1;
      if (strikes >= AUTH_FAILURE_MAX_STRIKES) return 'AUTH_EXHAUSTED';
      return 'AUTH_STRIKING';
    },
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, error } = event;
      const strikes = (_authFailureStrikes.get(accountId) || 0) + 1;
      _authFailureStrikes.set(accountId, strikes);

      if (strikes >= AUTH_FAILURE_MAX_STRIKES) {
        return [
          {
            type: 'DISCONNECT_ACCOUNT',
            accountId,
            reason: `Auth failure strikes exhausted: ${strikes}`,
          },
          {
            type: 'CREATE_SYSTEM_ALERT',
            alertType: 'auth_failure',
            accountId,
            message: `Account disconnected: ${strikes} auth failures`,
            details: { source: 'engagement-fsm', error, strikes },
          },
        ];
      }

      return [
        {
          type: 'LOG_DEGRADED',
          substate: 'PARTIAL_FAILURE',
          reason: `Auth strike ${strikes}/${AUTH_FAILURE_MAX_STRIKES} for ${accountId}`,
        },
      ];
    },
  },

  // ── Auth strikes reset (success) → IDLE ───────────────────────────────
  AUTH_STRIKES_RESET: {
    target: 'IDLE',
    guard: (event) => {
      const { accountId } = event;
      if (!_authFailureStrikes.has(accountId) || _authFailureStrikes.get(accountId) === 0) {
        return { allowed: false, reason: `No auth strikes for ${accountId}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      _authFailureStrikes.delete(event.accountId);
      return [];
    },
  },

  // ── Acquisition succeeded → clear engagement state ────────────────────
  AUTH_SUCCESS: {
    target: 'IDLE',
    guard: (event) => ({
      allowed: ['AUTH_STRIKING', 'AUTH_EXHAUSTED', 'CIRCUIT_OPEN', 'CIRCUIT_COOLING'].includes(_localState),
    }),
    buildActions: (event) => {
      _authFailureStrikes.delete(event.accountId);
      _circuitBreakers.delete(event.accountId);
      return [];
    },
  },

  // ── Retry exhausted → retry budget consumed ─────────────────────────────
  RETRY_EXHAUSTED: {
    target: 'RETRY_EXHAUST',
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, domain, intentId, error } = event;
      _executionRetries.delete(intentId);

      return [
        {
          type: 'MARK_PERMANENT_FAILURE',
          accountId,
          domain,
          intentId,
          error: error || 'retry_exhausted',
        },
        {
          type: 'CREATE_SYSTEM_ALERT',
          alertType: 'retry_exhausted',
          accountId,
          message: `Retries exhausted for ${domain}/${accountId}`,
          details: { domain, intentId, error },
        },
      ];
    },
  },

  // ── Retry count incremented → stay in IDLE, track retry ────────────────
  RETRY_COUNT_INCREMENTED: {
    target: 'IDLE',
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { intentId, retryCount } = event;
      _executionRetries.set(intentId, retryCount);
      return [];
    },
  },

  // ── New acquisition intent → clear retry exhaustion state ─────────────
  ACQUISITION_INTENT_RECEIVED: {
    target: 'IDLE',
    guard: () => ({
      allowed: ['RETRY_EXHAUST', 'AUTH_EXHAUSTED', 'CIRCUIT_OPEN', 'CIRCUIT_COOLING', 'AUTH_STRIKING', 'IDLE'].includes(_localState),
    }),
    buildActions: (event) => {
      const { intentId } = event;
      _executionRetries.delete(intentId);
      return [];
    },
  },

  // ── Circuit breaker query — pre-flight check routed through FSM via CK ─
  // This replaces the direct isCircuitBreakerActive() call in execution-bridge
  // and retry-worker. The FSM is the authority; execution layers must dispatch
  // through CK to get the answer rather than querying state directly.
  // Returns { circuitBreakerActive: boolean } in the dispatch result.
  CIRCUIT_BREAKER_CHECK: {
    target: () => _localState,  // No state change — this is a query event
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      // Attach circuit breaker state to the return via actions — caller checks actions
      const active = _circuitBreakers.has(event.accountId) &&
        _circuitBreakers.get(event.accountId).until > Date.now();
      if (active) {
        return [{ type: 'CIRCUIT_BREAKER_ACTIVE', accountId: event.accountId }];
      }
      return [];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _lastTransitionedAt = null; // last state change timestamp for temporal alignment in reconciliation

// ── Circuit breaker state: accountId → { until, cooldownMs, openedAt, reopenedAt } ──
const _circuitBreakers = new Map();

// ── Auth strike state: accountId → strike count ──────────────────────────────
const _authFailureStrikes = new Map();

// ── Execution retry state: intentId → retry count ───────────────────────────
const _executionRetries = new Map();

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch — process event, ask constitutional for validation, transition
//
// Domain FSMs emit through observability plane (not lineage ledger).
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the engagement FSM.
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

  // 4. Materialize state
  _localState = target;
  _lastTransitionedAt = Date.now();

  // 5. Emit observability transition for domain FSM state change
  // Fire-and-forget — observability failures never affect domain FSM behavior
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'engagement',
        entity: 'fsm',
        entityId: 'engagement-fsm',
        previousState: from,
        nextState: target,
        authority: 'engagement-fsm',
        raw: {
          intent: event.type,
          accountId: event.accountId || null,
          intentId: event.intentId || null,
          cooldownMs: event.cooldownMs || null,
          strikeCount: event.accountId ? (_authFailureStrikes.get(event.accountId) || 0) : null,
        },
      });
    }
  } catch (_) {}

  // 6. Build actions
  const actions = txn.buildActions ? txn.buildActions(event) : [];

  console.log(`[engagement-fsm] ${from} → ${target}  (${event.type})`);

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
 * Circuit breakers and auth strikes are NOT rehydrated from FSM state alone —
 * they are reconstructed from lineage entries by the reconciliation engine.
 *
 * @param {string} rehydratedState — the domain state to restore (e.g., 'IDLE')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[engagement-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  const now = Date.now();
  const activeBreakers = Array.from(_circuitBreakers.entries())
    .filter(([, b]) => b.until > now)
    .map(([accountId, b]) => ({ accountId, until: b.until, cooldownMs: b.cooldownMs }));

  return {
    state: _localState,
    activeCircuitBreakers: activeBreakers.length,
    circuitBreakers: activeBreakers,
    authFailureAccounts: Array.from(_authFailureStrikes.entries()).map(([accountId, strikes]) => ({ accountId, strikes })),
    pendingRetries: _executionRetries.size,
  };
}

function getHealth() {
  const now = Date.now();
  const breakerCount = Array.from(_circuitBreakers.values()).filter(b => b.until > now).length;
  const strikeCount = _authFailureStrikes.size;
  const retryCount = _executionRetries.size;

  return {
    ok: strikeCount === 0 && breakerCount === 0 && retryCount < 10,
    signals: {
      activeBreakers: breakerCount,
      authFailureAccounts: strikeCount,
      pendingRetries: retryCount,
    },
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Domain-specific state queries — called by CK proxy methods
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Returns true if the account has an active (non-expired) circuit breaker.
 * Expired breakers are auto-cleared on query.
 *
 * @param {string} accountId
 * @returns {boolean}
 */
function isCircuitBreakerActive(accountId) {
  const breaker = _circuitBreakers.get(accountId);
  if (!breaker) return false;
  if (Date.now() >= breaker.until) {
    _circuitBreakers.delete(accountId);
    return false;
  }
  return true;
}

/**
 * Returns the number of auth failure strikes for an account.
 *
 * @param {string} accountId
 * @returns {number}
 */
function getAuthStrikes(accountId) {
  return _authFailureStrikes.get(accountId) || 0;
}

/**
 * Returns the retry count for an intent.
 *
 * @param {string} intentId
 * @returns {number}
 */
function getRetryCount(intentId) {
  return _executionRetries.get(intentId) || 0;
}

/**
 * Resets auth failure strikes for an account (e.g., after successful auth).
 *
 * @param {string} accountId
 */
function resetAuthStrikes(accountId) {
  _authFailureStrikes.delete(accountId);
}

/**
 * Manually clears a circuit breaker for an account.
 *
 * @param {string} accountId
 */
function clearCircuitBreaker(accountId) {
  _circuitBreakers.delete(accountId);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Reconciliation engine getters — expose domain state for three-reality comparison
// ═══════════════════════════════════════════════════════════════════════════════

function getCircuitBreakers() {
  return new Map(_circuitBreakers);
}

function getAuthStrikeMap() {
  return new Map(_authFailureStrikes);
}

function getExecutionRetries() {
  return new Map(_executionRetries);
}

/**
 * Returns a structured snapshot of all engagement state for the reconciliation engine.
 *
 * @returns {{ circuitBreakers: Array, authStrikes: Array, executionRetries: Array, fsmState: string }}
 */
function getEngagementSnapshot() {
  const now = Date.now();
  return {
    fsmState: _localState,
    circuitBreakers: Array.from(_circuitBreakers.entries())
      .filter(([, b]) => b.until > now)
      .map(([accountId, b]) => ({ accountId, until: b.until, cooldownMs: b.cooldownMs, openedAt: b.openedAt })),
    authStrikes: Array.from(_authFailureStrikes.entries()).map(([accountId, strikes]) => ({ accountId, strikes })),
    executionRetries: Array.from(_executionRetries.entries()).map(([intentId, count]) => ({ intentId, count })),
  };
}

function getLastTransitionedAt() {
  return _lastTransitionedAt;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Module export
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  name: 'engagement',
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
  getAuthStrikeMap,
  getExecutionRetries,
  getEngagementSnapshot,
  getLastTransitionedAt,
};
