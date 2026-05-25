// control-plane/governance/governance-kernel.js
// Governance Kernel: hierarchical state machine governing runtime legality.
//
// Owns: runtime state transitions, legality evaluation, lineage recording,
//        governance action emission.
// Does NOT own: Redis, Supabase, worker lifecycle, evaluation mechanics,
//               emission mechanics, buffering mechanics — those remain
//               implementation concerns of the runtime modules.
//
// Architecture invariant:
//   Runtime modules emit signals UPWARD   → governance.dispatch(event)
//   Governance emits authority DOWNWARD  → governance.subscribeAction(type, fn)
//   Constitutional orchestrators execute actions mechanically.
//
// Contract:
//   governance.dispatch(event)         → evaluate event, transition if legal, emit actions
//   governance.subscribeAction(type,fn)→ register per-action-type subscriber (multi supported)
//   governance.onAction(fn)            → legacy: register catch-all subscriber (backward compat)
//   governance.tick()                  → watchdog: checks state staleness, auto-recovers stuck states
//   governance.startLoop(ms)           → start internal watchdog interval (default 10s)
//   governance.stopLoop()              → stop internal watchdog interval
//   governance.status()                → { state, parentState, lineageSize, uptimeMs, stateDurationMs,
//                                          authFailureAccounts, activeCircuitBreakers, pendingRetries }
//   governance.getLineage([n])         → append-only lineage records
//   governance.getState()              → current full state string
//   governance.getAccountIds()         → active account UUIDs
//   governance.getAuthStrikes(id)      → auth failure strike count for account
//   governance.isCircuitBreakerActive(id) → whether circuit breaker is engaged for account
//   governance.getRetryCount(intentId) → retry count for an acquisition intent
//
// This is a PURE state machine — no I/O, no Redis, no Supabase, no side effects.
// The runtime is not "controlled" — it is GOVERNED. The governance kernel
// determines runtime meaning; the orchestrator executes runtime mechanics.

const crypto = require('crypto');

const STARTED_AT = Date.now();

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Execution Policy Constants — governance-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const AUTH_FAILURE_MAX_STRIKES = 3;           // strikes before account disconnect
const MAX_ACQUISITION_RETRIES = 1;            // max retries per acquisition intent
const CIRCUIT_BREAKER_COOLDOWN_MS = 3600000;  // 1 hour circuit breaker cooldown

// ═══════════════════════════════════════════════════════════════════════════════
// 1. State Registry — defines all legal runtime states and their hierarchy
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  // ── Parent states ──────────────────────────────────────────────────────────
  BOOTING: {
    parent: null,
    description: 'Runtime is initializing — modules, connections, and workers provisioning',
  },
  HEALTHY: {
    parent: null,
    description: 'Runtime is operating within normal parameters',
  },
  DEGRADED: {
    parent: null,
    description: 'Runtime is operating in a degraded capacity',
  },
  RECOVERY: {
    parent: null,
    description: 'Runtime is recovering from a degraded or halted state',
  },
  HALTED: {
    parent: null,
    description: 'Runtime has halted — manual intervention required',
  },

  // ── HEALTHY substates ──────────────────────────────────────────────────────
  'HEALTHY.IDLE': {
    parent: 'HEALTHY',
    description: 'Runtime is healthy but idle — no events currently processing',
  },
  'HEALTHY.BUFFERING': {
    parent: 'HEALTHY',
    description: 'Runtime is accumulating signal events in buffer',
  },
  'HEALTHY.EVALUATING': {
    parent: 'HEALTHY',
    description: 'Runtime is evaluating buffered events against publishing policy',
  },
  'HEALTHY.EMITTING': {
    parent: 'HEALTHY',
    description: 'Runtime is emitting publishing intents to Redis queues',
  },

  // ── DEGRADED substates ─────────────────────────────────────────────────────
  'DEGRADED.BACKPRESSURE': {
    parent: 'DEGRADED',
    description: 'Event buffer is backing up — buffer growing faster than evaluation',
  },
  'DEGRADED.RETRY_PRESSURE': {
    parent: 'DEGRADED',
    description: 'Worker retry rates are elevated — execution may be unstable',
  },
  'DEGRADED.PARTIAL_FAILURE': {
    parent: 'DEGRADED',
    description: 'One or more runtime modules have partially failed',
  },
  'DEGRADED.SIGNAL_DESYNC': {
    parent: 'DEGRADED',
    description: 'Signal intake is desynchronized — events may be lost',
  },

  // ── RECOVERY substates ─────────────────────────────────────────────────────
  'RECOVERY.RECONCILING': {
    parent: 'RECOVERY',
    description: 'Runtime is reconciling state between persistence and memory',
  },
  'RECOVERY.REHYDRATING': {
    parent: 'RECOVERY',
    description: 'Runtime is rehydrating state from persistence',
  },
  'RECOVERY.RESTARTING': {
    parent: 'RECOVERY',
    description: 'Runtime is restarting modules',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 1b. State TTL — maximum duration for each state before watchdog auto-recovers
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_TTL_MS = {
  BOOTING: 60_000,              // boot must complete within 60s
  'HEALTHY.IDLE': Infinity,     // idle can be indefinite
  'HEALTHY.BUFFERING': 30_000,  // buffer flush should happen quickly
  'HEALTHY.EVALUATING': 30_000, // evaluation should be fast
  'HEALTHY.EMITTING': 30_000,   // emission to Redis should be fast
  'DEGRADED.BACKPRESSURE': 120_000,
  'DEGRADED.RETRY_PRESSURE': 120_000,
  'DEGRADED.PARTIAL_FAILURE': 120_000,
  'DEGRADED.SIGNAL_DESYNC': 120_000,
  'RECOVERY.RECONCILING': 60_000,
  'RECOVERY.REHYDRATING': 60_000,
  'RECOVERY.RESTARTING': 60_000,
  HALTED: Infinity,             // manual intervention only
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Transition Map — event type → target state + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Boot ───────────────────────────────────────────────────────────────────
  BOOT_COMPLETE: {
    target: 'HEALTHY.IDLE',
    buildActions: () => [{ type: 'START_INTENT_DISCOVERY' }],
  },

  // ── Operational flow: IDLE → BUFFERING → EVALUATING → EMITTING → IDLE ─────
  BUFFER_EVENT_INGESTED: {
    target: 'HEALTHY.BUFFERING',
    guard: (event, ctx) => {
      if (!['HEALTHY.IDLE', 'HEALTHY.BUFFERING'].includes(ctx.state)) {
        return { allowed: false, reason: `Cannot buffer from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  BUFFER_FLUSH_READY: {
    target: 'HEALTHY.EVALUATING',
    guard: (event, ctx) => {
      if (!['HEALTHY.IDLE', 'HEALTHY.BUFFERING'].includes(ctx.state)) {
        return { allowed: false, reason: `Cannot evaluate from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'EVALUATE',
      accountId: event.accountId,
      events: event.events,
    }],
  },

  EMISSION_OBSERVATION: {
    target: (event) => {
      if (event.status === 'error') return 'DEGRADED.PARTIAL_FAILURE';
      return 'HEALTHY.IDLE';
    },
    guard: (event, ctx) => {
      if (!['HEALTHY.EVALUATING', 'HEALTHY.EMITTING'].includes(ctx.state)) {
        return { allowed: false };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      if (event.status === 'error') {
        return [
          { type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE', reason: event.metadata?.reason },
          { type: 'STOP_INTENT_DISCOVERY' },
        ];
      }
      return [{ type: 'START_INTENT_DISCOVERY' }];
    },
  },

  // ── HSM-governed acquisition: IDLE → EVALUATING → EMITTING → IDLE ─────────
  ACQUISITION_INTENT_RECEIVED: {
    target: 'HEALTHY.EVALUATING',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.IDLE') {
        return { allowed: false, reason: `Cannot acquire from ${ctx.state}` };
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

  ACQUISITION_EXECUTING: {
    target: 'HEALTHY.EMITTING',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EVALUATING') {
        return { allowed: false, reason: `Cannot execute from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'STOP_INTENT_DISCOVERY',
    }],
  },

  ACQUISITION_COMPLETE: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EMITTING') {
        return { allowed: false, reason: `Cannot complete from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const actions = event.result ? [{
        type: 'WRITE_ACQUISITION_RESULT',
        accountId: event.accountId,
        domain: event.domain,
        intentId: event.intentId,
        result: event.result,
      }] : [];
      actions.push({ type: 'START_INTENT_DISCOVERY' });
      return actions;
    },
  },

  // ── Degradation ────────────────────────────────────────────────────────────
  BACKPRESSURE_DETECTED: {
    target: 'DEGRADED.BACKPRESSURE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY')) {
        return { allowed: false, reason: `Backpressure only from HEALTHY, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'BACKPRESSURE',
      reason: event.reason || 'Buffer accumulation exceeding capacity',
    }],
  },

  BACKPRESSURE_CLEARED: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (ctx.state !== 'DEGRADED.BACKPRESSURE') {
        return { allowed: false, reason: `Backpressure not active (currently ${ctx.state})` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  // Generic pressure clear — any DEGRADED substate → HEALTHY.IDLE.
  // Used by the cadence health check when worker metrics return to normal.
  PRESSURE_CLEARED: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false, reason: `Pressure clear only from DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },



  // ── Recovery ───────────────────────────────────────────────────────────────
  RECOVERY_INITIATED: {
    target: 'RECOVERY.RECONCILING',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false, reason: `Recovery only from DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [{
      type: 'LOG_RECOVERY',
      substate: 'RECONCILING',
    }],
  },

  RECOVERY_COMPLETE: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'RECOVERY')) {
        return { allowed: false, reason: `Recovery completion only from RECOVERY, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [{ type: 'START_INTENT_DISCOVERY' }],
  },

  // ── Halt ───────────────────────────────────────────────────────────────────
  FATAL_ERROR: {
    target: 'HALTED',
    buildActions: (event) => [{
      type: 'LOG_HALT',
      reason: event.reason || 'Unspecified fatal error',
    }],
  },

  // ── Substate transitions (within DEGRADED / RECOVERY) ─────────────────────
  RETRY_PRESSURE_DETECTED: {
    target: 'DEGRADED.RETRY_PRESSURE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'DEGRADED') && !isDescendantOf(ctx.state, 'HEALTHY')) {
        return { allowed: false, reason: `Retry pressure only from HEALTHY or DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'RETRY_PRESSURE',
      reason: event.reason || 'Worker retry rate elevated',
    }],
  },

  SIGNAL_DESYNC_DETECTED: {
    target: 'DEGRADED.SIGNAL_DESYNC',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') && !isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false, reason: `Signal desync only from HEALTHY or DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'SIGNAL_DESYNC',
      reason: event.reason || 'Signal intake desynchronized',
    }],
  },

  // ── Metrics governance: HSM evaluates thresholds, not orchestrator ─────────
  // Raw worker telemetry flows in from the cadence loop via WORKER_METRICS_REPORTED.
  // The governance kernel applies policy — no pre-classification in the substrate.
  //
  // Policy: DEGRADED.RETRY_PRESSURE when total >= 5 samples AND failureRate >= 50%
  // Recovery: general guard PRESSURE_CLEARED handles any DEGRADED substate → HEALTHY.IDLE
  //          when subsequent healthy metrics are reported.
  WORKER_METRICS_REPORTED: {
    target: (event) => {
      if (event.total >= 5 && event.failureRate >= 0.5) return 'DEGRADED.RETRY_PRESSURE';
      return null; // no state change — lineage recorded, governance observes
    },
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') && !isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false, reason: `Metrics only from HEALTHY or DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      if (event.total >= 5 && event.failureRate >= 0.5) {
        return [{
          type: 'LOG_DEGRADED',
          substate: 'RETRY_PRESSURE',
          reason: `Worker failure rate ${Math.round(event.failureRate * 100)}% (${event.failed}/${event.total} in ${((event.windowMs || 60000) / 1000).toFixed(0)}s)`,
        }];
      }
      return [];
    },
  },

  // ── Cadence: maintenance loop tick — orchestration cadence, not lifecycle ──
  // The 90s cadence sends CADENCE_TICK. The kernel sequences maintenance actions.
  // No state change — lineage recorded, actions emitted for orchestrator to execute.
  CADENCE_TICK: {
    target: (event, ctx) => ctx.state, // no lifecycle transition
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') &&
          !isDescendantOf(ctx.state, 'DEGRADED') &&
          ctx.state !== 'RECOVERY.RECONCILING') {
        return { allowed: false };
      }
      return { allowed: true };
    },
    buildActions: () => [
      { type: 'SCAN_DATABASE' },
      { type: 'REFRESH_LIFECYCLE' },
      { type: 'CHECK_SAFETY' },
      { type: 'REPORT_METRICS' },
    ],
  },

  // ── Maintenance action acknowledgements — record lineage, no state change ──
  DATABASE_SCANNED: {
    target: (event, ctx) => ctx.state,
    guard: () => ({ allowed: true }),
    buildActions: () => [],
  },

  LIFECYCLE_REFRESHED: {
    target: (event, ctx) => ctx.state,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      if (event.accountIds && event.accountIds.length > 0) {
        return [{ type: 'UPDATE_ACCOUNTS', accountIds: event.accountIds }];
      }
      return [];
    },
  },

  SAFETY_CHECK_COMPLETE: {
    target: (event, ctx) => ctx.state,
    guard: () => ({ allowed: true }),
    buildActions: () => [],
  },

  // ── Execution observations (governance-owned execution intelligence) ───────
  // Substrates emit raw observations upward. Governance alone decides policy.
  // This is the constitutional boundary: substrates observe, governance decides.

  EXECUTION_OBSERVATION: {
    target: (event, ctx) => ctx.state, // no lifecycle state change
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') && !isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { accountId, domain, intentId, params, status, error_category, retryable, count, latencyMs } = event;

      // Track execution state for retry decisions
      _executionState.set(intentId, { accountId, domain, params, lastError: event.error || null });

      // Success → complete the acquisition normally
      if (status === 'completed') {
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        // Auth success clears strikes
        if (_authFailureStrikes.has(accountId)) {
          _authFailureStrikes.delete(accountId);
        }
        return [{
          type: 'WRITE_ACQUISITION_RESULT',
          accountId, domain, intentId,
          result: { status: 'completed', count: count || 0 },
        }];
      }

      // Auth failure → increment strikes, evaluate threshold
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

      // Rate limit → engage circuit breaker
      if (error_category === 'rate_limit') {
        _executionRetries.delete(intentId);
        _executionState.delete(intentId);
        return [
          { type: 'ENGAGE_CIRCUIT_BREAKER', accountId, domain, cooldownMs: CIRCUIT_BREAKER_COOLDOWN_MS },
          { type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE', reason: `Rate limit detected for ${accountId}/${domain}` },
        ];
      }

      // Transient/retryable → increment retry count, decide retry or exhaust
      if (retryable) {
        const retryCount = (_executionRetries.get(intentId) || 0) + 1;
        _executionRetries.set(intentId, retryCount);

        if (retryCount <= MAX_ACQUISITION_RETRIES) {
          const delayMs = event.retryAfterMs || 30000;
          return [{
            type: 'RETRY_ACQUISITION',
            accountId, domain, intentId, params,
            retryCount,
            delayMs: Math.min(delayMs, 300000), // cap at 5 min
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

      // Permanent failure (non-retryable, non-auth, non-rate)
      _executionRetries.delete(intentId);
      _executionState.delete(intentId);
      return [{
        type: 'MARK_PERMANENT_FAILURE', accountId, domain, intentId, error: event.error || 'unknown',
      }];
    },
  },

  // ── Auth failure strike (emitted by retry substrate on auth errors) ────────
  AUTH_FAILURE_STRIKE: {
    target: (event, ctx) => ctx.state,
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') && !isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false };
      }
      return { allowed: true };
    },
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

  // ── Rate limit detected (emitted by retry substrate) ───────────────────────
  RATE_LIMIT_DETECTED: {
    target: (event, ctx) => ctx.state,
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') && !isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false };
      }
      return { allowed: true };
    },
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

  // ── Retry exhausted (emitted when all retries consumed) ────────────────────
  RETRY_EXHAUSTED: {
    target: (event, ctx) => ctx.state,
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') && !isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false };
      }
      return { allowed: true };
    },
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
// 3. General Guards — system-wide legality checks evaluated before every transition
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Run all general-guard checks against a proposed transition.
 * Returns an array of guard results. If any guard fails, the transition
 * is blocked regardless of per-transition guard results.
 */
function runGeneralGuards(currentState, targetState) {
  const results = [];

  // Guard: HALTED can only transition to BOOTING (manual restart)
  if (currentState === 'HALTED' && targetState !== 'BOOTING') {
    results.push({
      name: 'halted_lockdown',
      passed: false,
      reason: `HALTED state only allows transition to BOOTING, not ${targetState}`,
    });
    return results; // terminal — no need to check other guards
  }

  // Guard: DEGRADED parent blocks HEALTHY operational substates
  // Exception: DEGRADED → HEALTHY.IDLE (degradation cleared via backpressure_cleared)
  if (isDescendantOf(currentState, 'DEGRADED') &&
      isDescendantOf(targetState, 'HEALTHY') &&
      targetState !== 'HEALTHY.IDLE' &&
      !isDescendantOf(targetState, 'RECOVERY') &&
      targetState !== 'HALTED') {
    results.push({
      name: 'degraded_blocks_operational',
      passed: false,
      reason: `Cannot enter ${targetState} while in degraded state ${currentState}`,
    });
  }

  // Guard: RECOVERY parent blocks all transitions except to HEALTHY.IDLE, other RECOVERY substates, or HALTED
  if (isDescendantOf(currentState, 'RECOVERY') &&
      targetState !== 'HEALTHY.IDLE' &&
      !isDescendantOf(targetState, 'RECOVERY') &&
      targetState !== 'HALTED' &&
      targetState !== 'BOOTING') {
    results.push({
      name: 'recovery_blocks_operational',
      passed: false,
      reason: `Cannot enter ${targetState} while recovering (${currentState})`,
    });
  }

  // Guard: Cannot enter EMITTING from any DEGRADED state
  if (targetState === 'HEALTHY.EMITTING' && isDescendantOf(currentState, 'DEGRADED')) {
    results.push({
      name: 'no_emit_while_degraded',
      passed: false,
      reason: `Cannot emit while in degraded state ${currentState}`,
    });
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. State helpers
// ═══════════════════════════════════════════════════════════════════════════════

function parentOf(state) {
  const entry = STATE_REGISTRY[state];
  return entry ? entry.parent : null;
}

function isDescendantOf(state, ancestor) {
  let current = state;
  while (current) {
    if (current === ancestor) return true;
    current = parentOf(current);
  }
  return false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Runtime state — module-level mutable state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _currentState = 'BOOTING';
let _stateEnteredAt = Date.now(); // when we entered _currentState — watchdog uses this
const _lineage = [];              // append-only
let _loopInterval = null;         // watchdog setInterval handle

let _accountIds = [];             // active account UUIDs, updated via UPDATE_ACCOUNTS action

// ── Multi-subscriber action dispatch ─────────────────────────────────────────
// Per-action-type subscribers: Map<actionType, Set<handlerFn>>
// Replaces the old single _actionSubscriber for constitutional orchestration.
const _actionSubscribers = new Map();

// Legacy catch-all subscriber — receives ALL actions regardless of type.
// Maintained for backward compatibility during migration.
let _legacyActionSubscriber = null;

// ── Execution state tracking (governance-owned, migrated from retry.js) ──────
const _authFailureStrikes = new Map();  // accountId → strike count
const _circuitBreakers = new Map();     // accountId → { until: timestampMs }
const _executionRetries = new Map();    // intentId → retry count
const _executionState = new Map();      // intentId → { accountId, domain, params, lastError }

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Lineage recorder
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Record a transition in the append-only lineage log.
 * Never mutated or deleted after append.
 */
function _recordLineage(from, to, trigger, guardResults, actions, meta) {
  const entry = {
    id: crypto.randomUUID(),
    ts: Date.now(),
    from,
    to,
    trigger,
    guardResults: guardResults.map(g => ({
      name: g.name,
      passed: g.passed,
      reason: g.reason || null,
    })),
    actions: actions.map(a => ({ type: a.type })),
    meta,
  };
  _lineage.push(entry);
  return entry;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Action dispatcher
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Emit governance actions after a successful transition.
 * Routes each action to:
 *   1. Per-action-type subscribers (constitutional orchestrators)
 *   2. Legacy catch-all subscriber (backward compat)
 * Kernel-internal actions (UPDATE_ACCOUNTS, DISCONNECT_ACCOUNT, ENGAGE_CIRCUIT_BREAKER)
 * mutate kernel state directly — no orchestrator involvement needed.
 */
function _emitActions(actions) {
  if (actions.length === 0) return;
  for (const action of actions) {
    // ── Kernel-internal actions — mutate state directly ──────────────────
    if (action.type === 'UPDATE_ACCOUNTS') {
      _accountIds = Array.isArray(action.accountIds) ? action.accountIds : [];
      continue;
    }
    if (action.type === 'DISCONNECT_ACCOUNT') {
      _authFailureStrikes.delete(action.accountId);
      _circuitBreakers.set(action.accountId, { until: Date.now() + CIRCUIT_BREAKER_COOLDOWN_MS });
      // Falls through to subscribers so lifecycle orchestrator can act
    }
    if (action.type === 'ENGAGE_CIRCUIT_BREAKER') {
      _circuitBreakers.set(action.accountId || action.domain, {
        until: Date.now() + (action.cooldownMs || CIRCUIT_BREAKER_COOLDOWN_MS),
      });
      // Falls through to subscribers so acquisition orchestrator can act
    }
    if (action.type === 'MARK_PERMANENT_FAILURE') {
      _executionRetries.delete(action.intentId);
      _executionState.delete(action.intentId);
      // Falls through to subscribers
    }

    // ── Route to per-action-type subscribers ──────────────────────────────
    const subscribers = _actionSubscribers.get(action.type);
    if (subscribers && subscribers.size > 0) {
      for (const fn of subscribers) {
        try {
          fn(action);
        } catch (err) {
          console.error(`[governance-kernel] Subscriber error for ${action.type}:`, err.message);
        }
      }
    }

    // ── Legacy catch-all subscriber ───────────────────────────────────────
    if (_legacyActionSubscriber) {
      try {
        _legacyActionSubscriber(action);
      } catch (err) {
        console.error(`[governance-kernel] Legacy subscriber error for ${action.type}:`, err.message);
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7b. Watchdog tick — state staleness detection and auto-recovery
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Watchdog tick — checks if the current state has exceeded its TTL and
 * triggers automatic recovery transitions.
 *
 * Called by the internal loop interval. Self-corrects when external events
 * stop arriving. Intent discovery is handled by the sync substrate —
 * not by the kernel directly.
 *
 * Dispatches events through the normal dispatch() path, so all transitions
 * are guarded, lineage is recorded, and actions are emitted.
 */
function tick() {
  // ── Staleness detection ────────────────────────────────────────────────
  const elapsed = Date.now() - _stateEnteredAt;
  const ttl = STATE_TTL_MS[_currentState];
  if (ttl == null || elapsed < ttl) return;

  const reason = `State TTL exceeded: ${_currentState} for ${elapsed}ms (limit ${ttl}ms)`;

  switch (_currentState) {
    case 'BOOTING':
      dispatch({ type: 'FATAL_ERROR', reason });
      break;
    case 'HEALTHY.BUFFERING':
      dispatch({ type: 'BACKPRESSURE_DETECTED', reason });
      break;
    case 'HEALTHY.EVALUATING':
    case 'HEALTHY.EMITTING':
      dispatch({ type: 'EMISSION_OBSERVATION', status: 'error', accountId: null, metadata: { reason, intentCount: 0, mutationsApplied: 0, latencyMs: 0 } });
      break;
    case 'DEGRADED.BACKPRESSURE':
    case 'DEGRADED.RETRY_PRESSURE':
    case 'DEGRADED.PARTIAL_FAILURE':
    case 'DEGRADED.SIGNAL_DESYNC':
      dispatch({ type: 'RECOVERY_INITIATED', reason });
      break;
    case 'RECOVERY.RECONCILING':
    case 'RECOVERY.REHYDRATING':
    case 'RECOVERY.RESTARTING':
      dispatch({ type: 'RECOVERY_COMPLETE' });
      break;
    // HEALTHY.IDLE and HALTED have Infinity TTL — never auto-transition
  }
}

/**
 * Start the internal watchdog loop. Calls tick() every intervalMs.
 * Idempotent — safe to call on an already-running loop.
 * @param {number} [intervalMs=10000] — tick interval in ms, must be >= 1000
 * @throws {Error} if intervalMs < 1000
 */
function startLoop(intervalMs = 10_000) {
  if (typeof intervalMs !== 'number' || intervalMs < 1000) {
    throw new Error(`[governance-kernel] intervalMs must be >= 1000, got ${intervalMs}`);
  }
  if (_loopInterval) {
    console.warn('[governance-kernel] Watchdog loop already running — ignoring duplicate start');
    return;
  }
  _loopInterval = setInterval(tick, intervalMs);
  _loopInterval.unref();
  console.log(`[governance-kernel] Watchdog loop started — tick every ${intervalMs}ms`);
}

/**
 * Stop the internal watchdog loop.
 * Idempotent — safe to call when not running.
 */
function stopLoop() {
  if (_loopInterval) {
    clearInterval(_loopInterval);
    _loopInterval = null;
    console.log('[governance-kernel] Watchdog loop stopped');
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Public API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Dispatch a runtime event upward into the governance kernel.
 *
 * The event is evaluated against the transition map. If a matching transition
 * exists and all guards (per-transition + general) pass, the state transitions,
 * lineage is recorded, and governance actions are emitted.
 *
 * @param {{ type: string, [key: string]: any }} event — runtime signal event
 * @returns {{ allowed: boolean, from?: string, to?: string, lineageId?: string, actionsEmitted?: number, reason?: string }}
 *   Deterministic — same event in same state always produces the same result.
 */
function dispatch(event) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    // Unknown event — not an error, just not a state-changing event
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const from = _currentState;

  // Resolve target — may be a function (event-driven target) or a static string
  const rawTarget = txn.target;
  const target = typeof rawTarget === 'function' ? rawTarget(event, { state: from }) : rawTarget;

  // null target means no state change — record lineage only, skip transition
  if (target === null) {
    _recordLineage(from, from, event.type, [{ name: `transition:${event.type}`, passed: true }], [], {
      accountId: event.accountId || null,
      eventCount: event.eventCount || null,
      intentCount: event.intentCount || null,
    });
    return {
      allowed: true,
      from,
      to: from,
      lineageId: null,
      actionsEmitted: 0,
      reason: 'no-transition: event recorded',
    };
  }

  // 1. Run per-transition guard (if defined)
  let perGuardResults = [];
  if (txn.guard) {
    const result = txn.guard(event, { state: from });
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
    perGuardResults.push({
      name: `transition:${event.type}`,
      passed: true,
    });
  }

  // 2. Run general guards
  const generalResults = runGeneralGuards(from, target);
  const failedGeneral = generalResults.find(g => !g.passed);
  if (failedGeneral) {
    return { allowed: false, reason: failedGeneral.reason };
  }

  // 3. Execute transition
  _currentState = target;
  _stateEnteredAt = Date.now();
  const allGuardResults = [...perGuardResults, ...generalResults];

  // 4. Build actions
  const actions = txn.buildActions(event, { state: from });

  // 5. Record lineage
  const lineageEntry = _recordLineage(from, target, event.type, allGuardResults, actions, {
    accountId: event.accountId || null,
    eventCount: event.eventCount || null,
    intentCount: event.intentCount || null,
  });

  // 6. Emit actions
  _emitActions(actions);

  console.log(`[governance-kernel] ${from} → ${target}  (${event.type})  [${lineageEntry.id.slice(0, 8)}]`);

  return {
    allowed: true,
    from,
    to: target,
    lineageId: lineageEntry.id,
    actionsEmitted: actions.length,
  };
}

/**
 * Subscribe to governance actions of a specific type.
 * Multiple subscribers per action type are supported (constitutional orchestrators).
 * Each constitutional orchestrator subscribes only to its own action types.
 *
 * @param {string} actionType — the action type to subscribe to (e.g. 'EVALUATE', 'EXECUTE_ACQUISITION')
 * @param {Function} fn — (action: object) => void
 * @throws {Error} if actionType is not a string or fn is not a function
 */
function subscribeAction(actionType, fn) {
  if (typeof actionType !== 'string' || !actionType) {
    throw new Error(`[governance-kernel] subscribeAction requires a non-empty actionType string`);
  }
  if (typeof fn !== 'function') {
    throw new Error(`[governance-kernel] subscribeAction handler must be a function, got ${typeof fn}`);
  }
  if (!_actionSubscribers.has(actionType)) {
    _actionSubscribers.set(actionType, new Set());
  }
  _actionSubscribers.get(actionType).add(fn);
}

/**
 * Legacy: register a catch-all action subscriber that receives ALL actions.
 * Maintained for backward compatibility during constitutional migration.
 * Prefer subscribeAction() for new code.
 *
 * @param {Function} fn — (action: object) => void
 * @throws {Error} if fn is not a function
 */
function onAction(fn) {
  if (typeof fn !== 'function') {
    throw new Error(`[governance-kernel] onAction handler must be a function, got ${typeof fn}`);
  }
  _legacyActionSubscriber = fn;
}

/**
 * Returns live governance state. Deterministic, no side effects.
 * @returns {{ state: string, parentState: string|null, lineageSize: number, uptimeMs: number,
 *             stateDurationMs: number, authFailureAccounts: number, activeCircuitBreakers: number,
 *             pendingRetries: number }}
 */
function status() {
  const now = Date.now();
  return {
    state: _currentState,
    parentState: parentOf(_currentState),
    lineageSize: _lineage.length,
    uptimeMs: now - STARTED_AT,
    stateDurationMs: now - _stateEnteredAt,
    authFailureAccounts: _authFailureStrikes.size,
    activeCircuitBreakers: Array.from(_circuitBreakers.values()).filter(c => c.until > now).length,
    pendingRetries: _executionRetries.size,
  };
}

/**
 * Returns the last N lineage records (or all if n is omitted).
 * Records are append-only and never mutated.
 * @param {number} [n] — number of recent records to return
 * @returns {Array<object>}
 */
function getLineage(n) {
  if (typeof n === 'number' && n > 0) {
    return _lineage.slice(-n);
  }
  return [..._lineage];
}

/**
 * Returns the current full state string (e.g. 'HEALTHY.IDLE').
 * @returns {string}
 */
function getState() {
  return _currentState;
}

/**
 * Returns active account IDs (set via UPDATE_ACCOUNTS action from LIFECYCLE_REFRESHED).
 * Used by sync substrate to know which accounts to poll.
 * @returns {Array<string>}
 */
function getAccountIds() {
  return _accountIds;
}

/**
 * Returns the number of auth failure strikes for an account.
 * Governance-owned tracking — migrated from retry substrate.
 * @param {string} accountId
 * @returns {number}
 */
function getAuthStrikes(accountId) {
  return _authFailureStrikes.get(accountId) || 0;
}

/**
 * Returns whether the circuit breaker is currently engaged for an account.
 * Governance-owned state — migrated from retry substrate.
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
 * Returns the current retry count for an acquisition intent.
 * Governance-owned tracking — migrated from execution bridge.
 * @param {string} intentId
 * @returns {number}
 */
function getRetryCount(intentId) {
  return _executionRetries.get(intentId) || 0;
}

/**
 * Resets auth failure strikes for an account (e.g. after manual token refresh).
 * @param {string} accountId
 */
function resetAuthStrikes(accountId) {
  _authFailureStrikes.delete(accountId);
}

/**
 * Clears circuit breaker for an account (e.g. after manual intervention).
 * @param {string} accountId
 */
function clearCircuitBreaker(accountId) {
  _circuitBreakers.delete(accountId);
}

module.exports = { dispatch, subscribeAction, onAction, tick, startLoop, stopLoop,
  getAccountIds, getAuthStrikes, isCircuitBreakerActive, getRetryCount,
  resetAuthStrikes, clearCircuitBreaker,
  status, getLineage, getState };
