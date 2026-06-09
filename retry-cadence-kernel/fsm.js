// retry-cadence-kernel/fsm.js
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
//                  rate-limiter substrate tracks per-domain rate limit state
//                  FSM governs lifecycle meaning, substrates perform mechanics
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
    try { _observability = require('../control-plane/observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

const rateLimiter = require('../substrates/rate-limiter');

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
      const { accountId, cooldownMs = CIRCUIT_BREAKER_COOLDOWN_MS, substrate, affectedDomains } = event;

      // Record circuit breaker state (account-level)
      const existing = _circuitBreakers.get(accountId);
      _circuitBreakers.set(accountId, {
        until: Date.now() + cooldownMs,
        cooldownMs,
        openedAt: existing ? existing.openedAt : Date.now(),
        reopenedAt: existing ? Date.now() : null,
      });

      // Query substrate state for informed degradation
      let escalationReason = `Circuit breaker OPEN for ${accountId}, cooldown ${cooldownMs / 1000}s`;
      if (substrate && affectedDomains) {
        const substrateState = rateLimiter.getSubstrateState(substrate);
        const degradedDomains = Object.keys(substrateState.domains)
          .filter(d => substrateState.domains[d].until > Date.now());
        if (degradedDomains.length === affectedDomains.length) {
          escalationReason = `Circuit breaker OPEN for ${accountId} — entire ${substrate} substrate degraded (${degradedDomains.join(', ')})`;
        }
      }

      return [
        {
          type: 'ENGAGE_CIRCUIT_BREAKER',
          accountId,
          cooldownMs,
          substrate,
          affectedDomains: affectedDomains || [],
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
          reason: escalationReason,
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
      const { accountId } = event;
      _authFailureStrikes.delete(accountId);
      _circuitBreakers.delete(accountId);
      // Clear any pending retries for this account
      for (const [intentId, ctx] of _executionContexts.entries()) {
        if (ctx.accountId === accountId) {
          _cancelRetry(intentId);
        }
      }
      return [];
    },
  },

  // ── Retry exhausted → retry budget consumed ─────────────────────────────
  RETRY_EXHAUSTED: {
    target: 'RETRY_EXHAUST',
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, domain, intentId, error } = event;
      // FSM owns the cancellation. Clear the held context.
      _cancelRetry(intentId);

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

  // ── Retry count incremented — DEPRECATED in Step 5. The FSM owns
  //    the canonical count via _executionContexts. retry-cadence
  //    workers no longer emit this event. Kept as a no-op for
  //    backward compatibility with any external emitter.
  RETRY_COUNT_INCREMENTED: {
    target: 'IDLE',
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      // No-op: the FSM increments its own count via _scheduleRetry.
      return [];
    },
  },

  // ── Rate limit cleared → check if substrate is fully clear, test circuit ──
  RATE_LIMIT_CLEARED: {
    target: () => _localState,  // no state change — informational
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, substrate } = event;
      if (_localState !== 'CIRCUIT_OPEN' && _localState !== 'CIRCUIT_COOLING') return [];
      const state = rateLimiter.getSubstrateState(substrate);
      if (!state.anyLimited) {
        return [{ type: 'CIRCUIT_COOLDOWN_ELAPSED', accountId }];
      }
      return [];
    },
  },

  // ── RETRY_CADENCE_REQUEST — telemetry-coordination-fsm requests retry ───────
  // Entry point for ctx.dispatchGlobal({ type: 'RETRY_CADENCE_REQUEST' })
  // from telemetry-coordination-fsm (via CK GLOBAL_TRANSITION_MAP).
  // CK already validated HALTED/DEAD. The engagement-fsm owns the
  // scheduling decision here — classification, budget check, timer.
  RETRY_CADENCE_REQUEST: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const { source, lag, escalationState, namespace, projectionId, projectionType, signalsHash, errorMessage, errorName, failedAt, consecutiveFailures } = event;

      // Telemetry retry uses 'telemetry-coordination' as the domain.
      // intentId is synthesised — no specific intent being retried.
      // For partition_write_failure source, include namespace+projectionId
      // in the intentId so concurrent failures from different namespaces
      // don't collapse into a single retry context.
      const domain = 'telemetry-coordination';
      const intentId = source === 'partition_write_failure'
        ? `telemetry-failure-${namespace}-${projectionId || Date.now()}`
        : `telemetry-ingress-${Date.now()}`;
      const accountId = '*'; // system-wide, not per-account

      const paired = retryCadenceStore.dispatch(domain, accountId, intentId, {
        source,
        lag,
        escalationState,
        namespace,
        projectionId,
        projectionType,
        signalsHash,
        errorMessage,
        errorName,
        failedAt,
        consecutiveFailures,
      });
      const existing = _executionContexts.get(intentId);
      const newCount = existing ? existing.count + 1 : 0;
      const maxRetries = paired.maxRetries || 0;

      if (newCount > maxRetries) {
        _cancelRetry(intentId);
        return _buildExhaustedActions({
          accountId, domain, intentId,
          error: 'telemetry_max_retries_exceeded',
          retryCount: newCount,
        });
      }

      const context = {
        domain, accountId, intentId,
        params: {
          source, lag, escalationState,
          namespace, projectionId, projectionType, signalsHash,
          errorMessage, errorName, failedAt, consecutiveFailures,
        },
        count: newCount,
        maxRetries,
        timeoutId: null,
        retryWorker: paired.retryWorker,
        classificationWorker: paired.classificationWorker,
        policy: paired.policy,
        lastError: source === 'partition_write_failure'
          ? { type: 'partition_write_failure', namespace, projectionId, errorMessage }
          : { type: 'ingress_lag', lag, source },
        scheduledAt: null,
        governance: _governance,
        invokeWorker: ctx.invokeWorker,
        workerName: _resolveWorkerName(domain, { params: { namespace } }),
      };

      const scheduleResult = await _scheduleRetry(context, { type: 'TRANSIENT_RETRY' }, ctx);
      if (!scheduleResult.scheduled) {
        return [{
          type: 'SANITY_CHECK_REJECTED',
          operation: 'telemetry_schedule_retry',
          accountId, domain, intentId,
          retryCount: newCount,
          reason: scheduleResult.sanityCheck.reason,
        }];
      }

      // Emit TELEMETRY_RETRY_IN_PROGRESS for state sync with telemetry-fsm
      return [{
        type: 'TELEMETRY_RETRY_IN_PROGRESS',
        accountId,
        domain,
        intentId,
        retryCount: newCount,
        delayMs: scheduleResult.delayMs,
        source,
        lag,
        escalationState,
        namespace,
        projectionId,
      }];
    },
  },

  // ── RETRY_CADENCE_CLEAR — lag resolved, wind down retry budget ──────────────
  RETRY_CADENCE_CLEAR: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const { source } = event;
      for (const [intentId, ec] of _executionContexts) {
        if (ec.domain === 'telemetry-coordination') {
          _cancelRetry(intentId);
        }
      }
      return [{
        type: 'TELEMETRY_RETRY_CLEARED',
        source,
      }];
    },
  },

  // ── Retry requested — external entry point for non-worker emissions ──
  // The FSM is now the intelligence layer. The WORKER_OUTCOME_REPORTED
  // handler is the primary path; this handler exists for any external
  // emitter (e.g. a future orchestration source) that wants to
  // request a retry without going through the worker report path.
  //
  // The handler applies the same logic:
  //   - check retry budget
  //   - call _scheduleRetry (which awaits sanity check)
  //   - on rejection: emit SANITY_CHECK_REJECTED
  //   - on success: no external emission (timer fires internally)
  RETRY_REQUESTED: {
    target: () => _localState,  // no state change
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const { accountId, domain, intentId, params, retryAfterMs } = event;

      const paired = retryCadenceStore.dispatch(
        domain, accountId, intentId, params || {});
      const existing = _executionContexts.get(intentId);
      // First attempt (no existing context) → count=0
      // Retry (existing context) → count = previous count + 1
      const newCount = existing ? existing.count + 1 : 0;
      const maxRetries = paired.maxRetries || 0;

      if (newCount > maxRetries) {
        _cancelRetry(intentId);
        return [{
          type: 'RETRY_EXHAUSTED',
          accountId, domain, intentId,
          error: 'max_retries_exceeded',
          retryCount: newCount,
        }];
      }

      const context = {
        domain, accountId, intentId,
        params: params || {},
        count: newCount,
        maxRetries,
        timeoutId: null,
        retryWorker: paired.retryWorker,
        classificationWorker: paired.classificationWorker,
        policy: paired.policy,
        lastError: null,
        scheduledAt: null,
        governance: _governance, // passed to worker on invocation
        invokeWorker: ctx.invokeWorker, // CTX gate — ownership, contract, sanity
        workerName: _resolveWorkerName(domain, { params: params || {} }),
      };

      // If the caller provided a retryAfterMs (override), use it
      const actionTag = retryAfterMs ? { retryAfterMs } : null;
      const scheduleResult = await _scheduleRetry(context, actionTag, ctx);
      if (!scheduleResult.scheduled) {
        return [{
          type: 'SANITY_CHECK_REJECTED',
          operation: 'schedule_retry',
          accountId, domain, intentId,
          retryCount: newCount,
          reason: scheduleResult.sanityCheck.reason,
          alternatives: scheduleResult.sanityCheck.alternatives,
        }];
      }
      return [];
    },
  },

  // ACQUISITION_INTENT_RECEIVED entry REMOVED in Step 7.
  // The event is routed by DOMAIN_EVENT_MAP → 'acquisition' domain.
  // engagement-fsm should NEVER receive this event directly.
  // If we ever need to clear retry state on a new acquisition
  // intent, route a different event (e.g. ACQUISITION_INTENT_CLEARED
  // → 'engagement') rather than reusing this event.

  // ── Circuit breaker query — pre-flight check routed through FSM via CK ─
  // This replaces the direct isCircuitBreakerActive() call in execution-bridge
  // The FSM is the sole execution authority; all execution flows through RETRY_REQUESTED
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

  // ── WORKER_OUTCOME_REPORTED — central classification entry point ─
  // This is the primary path for worker-reported execution outcomes.
  // Workers are operationally complete but semantically blind: they
  // emit WORKER_OUTCOME_REPORTED with raw errorShape (response-shape
  // categorisation from the IG transport). The engagement-fsm is
  // the intelligence membrane — it calls the classification-worker
  // to get a deterministic action tag, then EMITS the appropriate
  // downstream signal. It does NOT mutate state and does NOT
  // schedule — the downstream handlers (RETRY_REQUESTED,
  // AUTH_FAILURE_STRIKE, RATE_LIMIT_DETECTED, RETRY_EXHAUSTED,
  // AUTH_SUCCESS) are the state mutators. They do NOT call the
  // classifier — the classifier runs ONCE here.
  //
  // The classification-worker is looked up per-call via
  // substrateRegistry.getClassificationWorker(domain). This is the
  // paired-dispatch property (refinement 1 of Step 4): the worker
  // and the classifier are bound at the same domain scope, so the
  // FSM has the classifier available the moment the worker reports.
  //
  // ASYNC HANDLER (Step 5): the dispatch function returns a Promise
  // for this event type. CK's polymorphic await handles it.
  //
  // Loop closure: WORKER_OUTCOME_REPORTED is only emitted by workers.
  // The downstream actions emitted in response flow to OTHER handlers
  // in the same FSM, or to OTHER FSMs. They do NOT re-enter this
  // handler. The classifier runs once per worker report. The chain
  // terminates when the FSM emits RETRY_EXHAUSTED, AUTH_SUCCESS, or
  // a strike threshold that triggers DISCONNECT_ACCOUNT.
  WORKER_OUTCOME_REPORTED: {
    target: () => _localState,  // observation does not change FSM state
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const { accountId, domain, intentId, status, errorShape } = event;

      // Success — no engagement action needed; the next phase
      // (PARSING_COMPLETE) handles the result write.
      //
      // Step 7 DRIFT-1 FIX: clean up the execution context on
      // success. Without this, the context lingers in
      // _executionContexts until AUTH_SUCCESS or RETRY_EXHAUSTED,
      // which is a memory drift. The intent is done — cancel
      // the retry chain.
      if (status === 'completed' || status === 'skipped') {
        _cancelRetry(intentId);
        return [];
      }

      // Failure path — classify the raw errorShape.
      // The classifier is the FSM's tool, not an event. Called here,
      // synchronously, during the FSM's evaluation. The result is
      // consumed and emitted as a downstream action — the classifier
      // itself does not propagate.
      if (status === 'failed' && errorShape) {
        const classificationWorker =
          require('../acquisition-kernel/substrate-registry')
            .getClassificationWorker(domain);
        const actionTag = classificationWorker.classify(errorShape);

        switch (actionTag.type) {
          case 'TRANSIENT_RETRY': {
            // FSM owns the scheduling decision. Build the
            // execution context (or update an existing one),
            // ask CK for permission, compute the delay,
            // set the timer.
            const paired = retryCadenceStore.dispatch(
              domain, accountId, intentId, event.params || {});
            const existing = _executionContexts.get(intentId);
            // First attempt (no existing context) → count=0
            // Retry (existing context) → count = previous count + 1
            const newCount = existing ? existing.count + 1 : 0;
            const maxRetries = paired.maxRetries || 0;

            if (newCount > maxRetries) {
              // Budget exhausted — terminal
              _cancelRetry(intentId);
              return _buildExhaustedActions({
                accountId, domain, intentId,
                error: 'max_retries_exceeded',
                retryCount: newCount,
              });
            }

            const context = {
              domain, accountId, intentId,
              params: event.params || {},
              count: newCount,
              maxRetries,
              timeoutId: null,
              retryWorker: paired.retryWorker,
              classificationWorker: paired.classificationWorker,
              policy: paired.policy,
              lastError: errorShape,
              scheduledAt: null,
              governance: _governance, // passed to worker on invocation
              invokeWorker: ctx.invokeWorker, // CTX gate — ownership, contract, sanity
              workerName: _resolveWorkerName(domain, { params: event.params || {} }),
            };

            const scheduleResult = await _scheduleRetry(context, actionTag, ctx);
            // ORDERING ASSUMPTION: sanity gate fires BEFORE the timer is set
            // and BEFORE any worker is invoked. The rejected event below is
            // consumed by the FSM's own SANITY_CHECK_REJECTED handler (which
            // cancels the held context). If a future refactor splits the
            // sanity gate into pre-schedule AND pre-invoke, the rejected
            // event must NOT be emitted on the pre-invoke path — the worker
            // would already be in flight.
            if (!scheduleResult.scheduled) {
              // Sanity check rejected the schedule. Emit
              // SANITY_CHECK_REJECTED for the FSM's own handler
              // to process (cancellation, state update).
              return [{
                type: 'SANITY_CHECK_REJECTED',
                operation: 'schedule_retry',
                accountId, domain, intentId,
                retryCount: newCount,
                reason: scheduleResult.sanityCheck.reason,
                alternatives: scheduleResult.sanityCheck.alternatives,
              }];
            }

            // Scheduled successfully. For publish:* domains, emit
            // RETRY_IN_PROGRESS. For dedup domain, emit
            // DEDUP_RETRY_IN_PROGRESS. The domain FSM holds its
            // state while the retry chain is in flight
            // (observability fidelity).
            if (domain && domain.startsWith('publish:')) {
              return [{
                type: 'RETRY_IN_PROGRESS',
                accountId,
                domain,
                intentId,
                retryCount: newCount,
                delayMs: scheduleResult.delayMs,
              }];
            }
            if (domain && domain.startsWith('dedup:')) {
              return [{
                type: 'DEDUP_RETRY_IN_PROGRESS',
                accountId,
                domain,
                intentId,
                retryCount: newCount,
                delayMs: scheduleResult.delayMs,
              }];
            }
            if (domain === 'reconciliation') {
              return [{
                type: 'RECON_RETRY_IN_PROGRESS',
                accountId,
                domain,
                intentId,
                retryCount: newCount,
                delayMs: scheduleResult.delayMs,
              }];
            }
            return [];
          }

          case 'AUTH_FAILURE':
            // Any pending retry for this intent is now moot
            _cancelRetry(intentId);
            return [{
              type: 'AUTH_FAILURE_STRIKE',
              accountId,
              error: event.error,
              igCode: actionTag.igCode,
            }];

          case 'RATE_LIMIT':
            // Do NOT cancel pending retry — the gate will block
            // it on its next attempt. The existing
            // RATE_LIMIT_DETECTED handler opens the breaker.
            return [{
              type: 'RATE_LIMIT_DETECTED',
              accountId,
              cooldownMs: actionTag.retryAfterMs,
              domain,
              substrate: require('../substrates/rate-limiter')
                .getSubstrate(domain),
              affectedDomains: [domain],
              igCode: actionTag.igCode,
            }];

          case 'PERMANENT_FAILURE':
            _cancelRetry(intentId);
            return _buildExhaustedActions({
              accountId, domain, intentId,
              error: event.error || 'permanent_failure',
              igCode: actionTag.igCode,
            });

          default:
            _cancelRetry(intentId);
            return [{
              type: 'RETRY_EXHAUSTED',
              accountId, domain, intentId,
              error: `unrecognised_classification: ${actionTag.type}`,
            }];
        }
      }

      // Failed with no errorShape (defensive)
      _cancelRetry(intentId);
      return _buildExhaustedActions({
        accountId, domain, intentId,
        error: event.error || 'no_error_shape',
      });
    },
  },

  // ── SANITY_CHECK_REJECTED — the FSM's own reaction to its own rejections ─
  // When CK rejects a scheduling or invocation decision, the FSM
  // receives SANITY_CHECK_REJECTED. The FSM:
  //   - cancels any held execution context for the intent
  //     (no point keeping it if CK won't allow the schedule)
  //   - tracks the rejection count for the account
  //   - emits the appropriate terminal event:
  //     * schedule_retry rejection → RETRY_EXHAUSTED with reason
  //     * invoke_worker rejection → RETRY_EXHAUSTED with reason
  //   - logs degraded state for observability
  // The FSM does NOT retry the sanity check — CK's decision is final.
  SANITY_CHECK_REJECTED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, intentId, operation, reason } = event;

      // Cancel the held context (the decision was rejected, the
      // intent is not going to retry)
      _cancelRetry(intentId);

      // Log the rejection for observability
      return [
        {
          type: 'LOG_DEGRADED',
          substate: 'SANITY_REJECTED',
          reason: `${operation} rejected: ${reason}`,
        },
        ..._buildExhaustedActions({
          accountId,
          domain: event.domain,
          intentId,
          error: `sanity_check_rejected: ${reason}`,
          operation,
        }),
      ];
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

// ── Execution context state: intentId → ExecutionContext ─────────────────
// The CANONICAL retry state. Promoted from _executionRetries in Step 5.
// Each context holds the full operational state for an in-flight retry:
//   { domain, accountId, intentId, params, count, maxRetries, timeoutId,
//     retryWorker, classificationWorker, policy, lastError,
//     scheduledAt }
// The FSM owns the counter, the timer, the delay computation, the
// dispatch decision, the cancellation. Single source of truth.
const _executionContexts = new Map();

// ── Sanity check reference (canonical: ctx.sanityCheck) ────────────────
// The ctx.sanityCheck is the universal gate. The FSM calls
// ctx.sanityCheck(action) during evaluation. No module-level
// reference is needed — the ctx is the canonical interface.
//
// Default fallback (for tests calling dispatch without CK):
// a no-op sanity check that always allows. This is fail-open
// to preserve operational cadence in non-CK contexts.
const _defaultSanityCheck = async () => ({ allowed: true });

/**
 * Resolve the sanity check function from the ctx.
 * If ctx is null/undefined (test mode), returns the
 * default fail-open check.
 */
function _resolveSanityCheck(ctx) {
  if (ctx && typeof ctx.sanityCheck === 'function') {
    return ctx.sanityCheck;
  }
  return _defaultSanityCheck;
}

// ── Governance reference (set by orchastrator at boot) ────────────────
// The FSM holds a governance ref (set via setGovernance) for
// its own use during evaluation. The FSM also passes the
// governance ref through the execution context to the
// retry worker (so the worker can emit WORKER_OUTCOME_REPORTED).
//
// The orchastrator calls engagementFsm.setGovernance(governance)
// at boot. Default: null (the FSM must not be used without
// a governance ref in production — fail-loud in that case).
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


// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch — process event, ask constitutional for validation, transition
//
// Domain FSMs emit through observability plane (not lineage ledger).
// ═══════════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════════
// 3.5. Retry orchestration — FSM-owned scheduling, timer, cancellation
// ═══════════════════════════════════════════════════════════════════════════════
// The FSM is the LOCAL intelligence layer. It owns:
//   - the retry counter (canonical)
//   - the timer (setTimeout, tracked by timeoutId)
//   - the delay computation (policy + classifier override)
//   - the dispatch decision (call worker directly or schedule)
//   - the cancellation (clear timer, clear state)
//
// CK is the CENTRAL authority vector. Before scheduling or invoking,
// the FSM calls ctx.sanityCheck (the universal gate).
// CK may reject the operation. On rejection, the FSM emits
// SANITY_CHECK_REJECTED and falls through to a safe default.

const { computeRetryDelay } = require('./policy');
const retryCadenceStore = require('./index');

/**
 * Schedule a retry attempt for an execution context.
 * Computes the delay (policy + classifier override), sets a
 * setTimeout, and tracks the timeoutId in the context.
 *
 * The timer fires _executeRetry which invokes the worker. The
 * worker emits WORKER_OUTCOME_REPORTED when done, which re-enters
 * the FSM's main handler. The chain continues until the FSM
 * decides to exhaust, cancel, or succeed.
 *
 * @param {object} context — ExecutionContext
 * @param {object|null} actionTag — classification output (for delay override)
 * @param {object|null} fsmCtx — the dispatch ctx (for ctx.sanityCheck)
 * @returns {{ scheduled: boolean, timeoutId: number|null, delayMs: number,
 *            sanityCheck: object }}
 */
async function _scheduleRetry(context, actionTag, fsmCtx) {
  // Central authority vector gate (universal — every FSM)
  const sanityCheck = _resolveSanityCheck(fsmCtx);
  const sanity = await sanityCheck({
    operation: 'schedule_retry',
    accountId: context.accountId,
    domain: context.domain,
    intentId: context.intentId,
    retryCount: context.count,
    maxRetries: context.maxRetries,
  });

  if (!sanity.allowed) {
    return {
      scheduled: false,
      timeoutId: null,
      delayMs: 0,
      sanityCheck: sanity,
    };
  }

  // Compute delay — FSM owns this
  const delayMs = computeRetryDelay(context.policy, context.count, actionTag);

  // Set the timer. _executeRetry is the callback.
  const timeoutId = setTimeout(() => {
    // Clear timeoutId from context — it's firing now
    const ctx = _executionContexts.get(context.intentId);
    if (ctx) ctx.timeoutId = null;
    _executeRetry(context, fsmCtx).catch((err) => {
      console.error(`[engagement-fsm] _executeRetry failed for ${context.intentId}:`, err.message);
    });
  }, delayMs);

  // Update context with the new timeoutId
  context.timeoutId = timeoutId;
  context.scheduledAt = Date.now();
  _executionContexts.set(context.intentId, context);

  // Record failure in retry-cadence store
  retryCadenceStore.recordFailure(context.intentId, context.lastError);

  return {
    scheduled: true,
    timeoutId,
    delayMs,
    sanityCheck: sanity,
  };
}

/**
 * Resolve the registered worker name for a domain.
 * Used to pass workerName through execution contexts so
 * _executeRetry can call ctx.invokeWorker(workerName, params).
 *
 * For telemetry namespace retries, the domain in the execution context
 * is 'telemetry-coordination' (the canonical domain for the retry cadence).
 * The actual worker is namespace-specific — read from context.params.namespace
 * which is carried through from the RETRY_CADENCE_REQUEST event.
 *
 * @param {string} domain — the retry domain (e.g. 'telemetry-coordination')
 * @param {object} [context] — execution context with params.namespace
 * @returns {string} worker name registered in CK
 */
function _resolveWorkerName(domain, context) {
  // Telemetry namespace resolution — check params.namespace first
  if (domain === 'telemetry-coordination' && context && context.params && context.params.namespace) {
    return `telemetry-retry-${context.params.namespace}-worker`;
  }

  const MAP = {
    comments: 'engagement-retry',
    messages: 'engagement-retry',
    ugc: 'ugc-retry',
    insights: 'insights-retry',
    media: 'content-retry',
    'publish:post': 'publish-content-retry',
    'publish:story': 'publish-content-retry',
    'publish:comment': 'publish-engagement-retry',
    'publish:message': 'publish-engagement-retry',
    'dedup:redis':     'dedup-redis-retry',
    'dedup:repair':    'dedup-repair-retry',
    reconciliation:    'reconciliation-retry',
    'telemetry-coordination': 'telemetry-retry',
    // Direct namespace domain keys (for cases where domain itself is namespaced)
    'telemetry:runtime':   'telemetry-retry-runtime-worker',
    'telemetry:integrity': 'telemetry-retry-integrity-worker',
    'telemetry:authority': 'telemetry-retry-authority-worker',
    'telemetry:health':    'telemetry-retry-health-worker',
    'telemetry:systemic':  'telemetry-retry-systemic-worker',
  };
  return MAP[domain] || 'unknown';
}

/**
 * Execute a retry attempt. Called by the setTimeout in _scheduleRetry.
 * Routes through the CTX gate (ctx.invokeWorker) when available —
 * validates ownership, contract, and sanity before invocation.
 * Falls back to direct retryWorker invocation if the gate is unset
 * (legacy path — should not happen in production after CK gate wiring).
 *
 * @param {object} context — ExecutionContext (must include governance)
 * @param {object|null} fsmCtx — the dispatch ctx (for ctx.sanityCheck)
 */
async function _executeRetry(context, fsmCtx) {
  // ── CTX gate path — ownership, contract, sanity validated by CK ──────
  if (context.invokeWorker && context.workerName) {
    try {
      await context.invokeWorker(context.workerName, {
        domain: context.domain,
        accountId: context.accountId,
        intentId: context.intentId,
        params: context.params,
        retryCount: context.count,
        maxRetries: context.maxRetries,
        governance: context.governance,
      });
      return;
    } catch (err) {
      console.error(`[engagement-fsm] CTX gate blocked worker '${context.workerName}' for ${context.intentId}:`, err.message);
      return;
    }
  }

  // ── Fallback: direct invocation (legacy, no gate) ─────────────────────
  // Sanity check before invocation (universal gate)
  const sanityCheck = _resolveSanityCheck(fsmCtx);
  const sanity = await sanityCheck({
    operation: 'invoke_worker',
    accountId: context.accountId,
    domain: context.domain,
    intentId: context.intentId,
    worker: context.retryWorker?.name || 'unknown',
  });

  if (!sanity.allowed) {
    return;
  }

  if (!context.retryWorker || typeof context.retryWorker.execute !== 'function') {
    console.error(`[engagement-fsm] No retryWorker in context for ${context.intentId}`);
    return;
  }

  // Invoke. The worker is responsible for emitting
  // WORKER_OUTCOME_REPORTED. The worker receives the
  // governance ref via the context — the FSM is the
  // only place that holds the ref. The worker does
  // NOT import it at module load (fail-loud if null).
  if (!context.governance) {
    console.error(`[engagement-fsm] No governance in context for ${context.intentId} — worker cannot dispatch`);
    return;
  }
  try {
    await context.retryWorker.execute(
      context.domain,
      context.accountId,
      context.intentId,
      context.params,
      context.count,
      context.maxRetries,
      // Governance reference — passed from FSM's context.
      // The worker uses this to emit WORKER_OUTCOME_REPORTED.
      context.governance,
    );
  } catch (err) {
    console.error(`[engagement-fsm] Retry worker threw for ${context.intentId}:`, err.message);
  }
}

/**
 * Cancel a pending retry. Clears the timer, removes the execution
 * context, and notifies the retry-cadence store.
 */
function _cancelRetry(intentId) {
  const context = _executionContexts.get(intentId);
  if (context && context.timeoutId) {
    clearTimeout(context.timeoutId);
  }
  _executionContexts.delete(intentId);
  retryCadenceStore.clearRetry(intentId);
}

/**
 * Track a sanity check rejection for an account.
 * Used to escalate to DEGRADED if rejections pile up.
 */
/**
 * Build the terminal RETRY_EXHAUSTED action bundle.
 *
 * Always emits RETRY_EXHAUSTED (routed to engagement domain).
 * For publish:* domains, ALSO emits PUBLISH_RETRY_EXHAUSTED
 * (routed to publishing domain) so publishing-fsm can
 * transition EXECUTING → IDLE on the canonical terminal
 * failure path.
 *
 * Without this, publishing-fsm would stay in EXECUTING
 * (held by RETRY_IN_PROGRESS) even after the retry chain
 * is exhausted. PUBLISH_RETRY_EXHAUSTED closes the loop.
 *
 * @param {object} params — { accountId, domain, intentId, error, ... }
 * @returns {Array} actions array (1 or 2 elements)
 */
function _buildExhaustedActions(params) {
  const actions = [{
    type: 'RETRY_EXHAUSTED',
    ...params,
  }];
  if (params.domain && params.domain.startsWith('publish:')) {
    actions.push({
      type: 'PUBLISH_RETRY_EXHAUSTED',
      accountId: params.accountId,
      domain: params.domain,
      intentId: params.intentId,
      error: params.error,
      igCode: params.igCode,
      retryCount: params.retryCount,
      operation: params.operation,
    });
  }
  if (params.domain && params.domain.startsWith('dedup:')) {
    actions.push({
      type: 'DEDUP_RETRY_EXHAUSTED',
      accountId: params.accountId,
      domain: params.domain,
      intentId: params.intentId,
      error: params.error,
      retryCount: params.retryCount,
      operation: params.operation,
    });
  }
  if (params.domain === 'reconciliation') {
    actions.push({
      type: 'RECON_RETRY_EXHAUSTED',
      accountId: params.accountId,
      domain: params.domain,
      intentId: params.intentId,
      error: params.error,
      retryCount: params.retryCount,
      operation: params.operation,
    });
  }
  if (params.domain === 'telemetry-coordination') {
    actions.push({
      type: 'TELEMETRY_RETRY_EXHAUSTED',
      accountId: params.accountId,
      domain: params.domain,
      intentId: params.intentId,
      error: params.error,
      retryCount: params.retryCount,
      source: params.params?.source,
      lag: params.params?.lag,
    });
  }
  return actions;
}

/**
 * Process a domain event within the engagement FSM.
 *
 * @param {{ type: string, [key: string]: any }} event — domain event
 * @param {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx — constitutional kernel context
 * @returns {{ allowed: boolean, from?: string, to?: string, lineageId?: string, actions?: Array, reason?: string } | Promise<...>}
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
  const actions = txn.buildActions ? txn.buildActions(event, ctx) : [];

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
    pendingRetries: _executionContexts.size,
  };
}

function getHealth() {
  const now = Date.now();
  const breakerCount = Array.from(_circuitBreakers.values()).filter(b => b.until > now).length;
  const strikeCount = _authFailureStrikes.size;
  const pendingContexts = _executionContexts.size;

  return {
    ok: strikeCount === 0 && breakerCount === 0 && pendingContexts < 10,
    signals: {
      activeBreakers: breakerCount,
      authFailureAccounts: strikeCount,
      pendingRetries: pendingContexts,
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
 * Returns the retry count for an intent (canonical, from
 * _executionContexts).
 *
 * @param {string} intentId
 * @returns {number}
 */
function getRetryCount(intentId) {
  const ctx = _executionContexts.get(intentId);
  return ctx ? ctx.count : 0;
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
  // Return Map< intentId, count > for backward compatibility.
  // The canonical state is _executionContexts (full contexts).
  const m = new Map();
  for (const [intentId, ctx] of _executionContexts.entries()) {
    m.set(intentId, ctx.count);
  }
  return m;
}

/**
 * Return the full execution contexts (the canonical state).
 * Used by observability, reconciliation, and the orchastrator.
 */
function getExecutionContexts() {
  return new Map(_executionContexts);
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
    executionRetries: Array.from(_executionContexts.entries()).map(([intentId, ctx]) => ({
      intentId, count: ctx.count, maxRetries: ctx.maxRetries,
    })),
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
  setGovernance,
  getGovernance,
  registerWorker,
  getWorker,
  getWorkers,
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
  getExecutionContexts,
  getEngagementSnapshot,
  getLastTransitionedAt,
};
