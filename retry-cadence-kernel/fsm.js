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
// Transition writers consume from the observability plane and write to the
// canonical lineage ledger via lineageLedger.recordWorkerEntry().
// FSMs do NOT write to the lineage ledger directly.
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

// ── Deferred retry policy (R7) ─────────────────────────────────────────────
// When CK rejects a schedule (HALTED/DEAD/etc.), the intent is pushed
// to _deferredIntents. On SANITY_CHECK_RESUMED the deferred intents
// drain — each with exponential backoff, capped at DEFERRED_MAX_DELAY_MS.
// After DEFERRED_MAX_ATTEMPTS rejections the intent goes terminal.
const DEFERRED_BASE_DELAY_MS = 30000;   // 30s first re-try
const DEFERRED_MAX_DELAY_MS  = 300000;  // 5min cap
const DEFERRED_BACKOFF_MULTIPLIER = 2;
const DEFERRED_MAX_ATTEMPTS = 5;        // after 5 deferrals, terminal

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
  // DEFERRED is an internal substate, not a transition target. The
  // local state stays IDLE/CIRCUIT_OPEN/etc. while intents are deferred.
  // The state registry entry exists for observability/dashboards.
  DEFERRED: {
    description: 'CK is HALTED/DEAD; one or more intents are deferred awaiting CK_RESUMED',
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
      // R3: accept event.domain if present, fall back to telemetry-coordination
      const domain = event.domain || 'telemetry-coordination';
      const intentId = source === 'partition_write_failure'
        ? `telemetry-failure-${namespace}-${projectionId || Date.now()}`
        : `telemetry-ingress-${Date.now()}`;
      const accountId = '*'; // system-wide, not per-account

  // ── Build and schedule — R1 + R12 + R3 all funnelled through helper ───────
      const helperParams = {
        source, lag, escalationState,
        namespace, projectionId, projectionType, signalsHash,
        errorMessage, errorName, failedAt, consecutiveFailures,
      };
      const lastError = source === 'partition_write_failure'
        ? { type: 'partition_write_failure', namespace, projectionId, errorMessage }
        : { type: 'ingress_lag', lag, source };

      const result = await _buildRetrySchedule({
        domain, accountId, intentId,
        params: helperParams,
        lastError,
        actionTag: { type: 'TRANSIENT_RETRY' },
        retryAfterMs: null,
        ctx,
      });

      if (result.kind === 'exhausted') {
        // R12: pass source/lag as top-level fields. _buildExhaustedActions
        // reads them directly (not params.params).
        return result.actions.map(a => {
          if (a.type === 'TELEMETRY_RETRY_EXHAUSTED') {
            return { ...a, source, lag };
          }
          return a;
        });
      }
      if (result.kind === 'rejected') {
        return [{
          type: 'SANITY_CHECK_REJECTED',
          operation: 'telemetry_schedule_retry',
          accountId, domain, intentId,
          retryCount: result.newCount,
          reason: result.sanityCheck.reason,
        }];
      }

      // Emit TELEMETRY_RETRY_IN_PROGRESS for state sync with telemetry-fsm
      return [{
        type: 'TELEMETRY_RETRY_IN_PROGRESS',
        accountId,
        domain,
        intentId,
        retryCount: result.newCount,
        delayMs: result.delayMs,
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

      const result = await _buildRetrySchedule({
        domain, accountId, intentId, params,
        lastError: null,
        actionTag: null,
        retryAfterMs: retryAfterMs || null,
        ctx,
      });

      if (result.kind === 'exhausted') {
        return result.actions;
      }
      if (result.kind === 'rejected') {
        return [{
          type: 'SANITY_CHECK_REJECTED',
          operation: 'schedule_retry',
          accountId, domain, intentId,
          retryCount: result.newCount,
          reason: result.sanityCheck.reason,
          alternatives: result.sanityCheck.alternatives,
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
            // FSM owns the scheduling decision. Funnel through the
            // canonical _buildRetrySchedule helper — same path as
            // RETRY_REQUESTED. The classification ran once here, the
            // actionTag is the classifier's verdict, the helper does
            // paired-dispatch + budget + sanity + timer.
            const result = await _buildRetrySchedule({
              domain, accountId, intentId,
              params: event.params || {},
              lastError: errorShape,
              actionTag,
              retryAfterMs: null,
              ctx,
            });

            if (result.kind === 'exhausted') {
              return result.actions;
            }
            if (result.kind === 'rejected') {
              // Sanity check rejected the schedule. Emit
              // SANITY_CHECK_REJECTED for the FSM's own handler
              // to process (cancellation, state update).
              return [{
                type: 'SANITY_CHECK_REJECTED',
                operation: 'schedule_retry',
                accountId, domain, intentId,
                retryCount: result.newCount,
                reason: result.sanityCheck.reason,
                alternatives: result.sanityCheck.alternatives,
              }];
            }

            // Publish retries no longer flow through the retry-cadence
            // path. The publishing FSM emits RETRY_IN_PROGRESS (or
            // its own publish-specific signal) directly. The retry-
            // cadence FSM has no publish:* branch here.
            if (domain && domain.startsWith('dedup:')) {
              return [{
                type: 'DEDUP_RETRY_IN_PROGRESS',
                accountId,
                domain,
                intentId,
                retryCount: result.newCount,
                delayMs: result.delayMs,
              }];
            }
            if (domain === 'reconciliation') {
              return [{
                type: 'RECON_RETRY_IN_PROGRESS',
                accountId,
                domain,
                intentId,
                retryCount: result.newCount,
                delayMs: result.delayMs,
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

  // ── SANITY_CHECK_REJECTED — R7 deferred queue ────────────────────────────
  // When CK rejects a scheduling or invocation decision, the FSM
  // receives SANITY_CHECK_REJECTED. The FSM:
  //   - schedule_retry rejection → push to _deferredIntents, emit
  //     DEFERRED_RETRY_SCHEDULED. The intent waits for SANITY_CHECK_RESUMED
  //     (CK_RESUMED trigger from the constitutional kernel) before retrying.
  //   - invoke_worker rejection → still terminal. The worker is already
  //     in flight; the gate is downstream of the timer. Map to
  //     RETRY_EXHAUSTED so the chain closes.
  //   - after DEFERRED_MAX_ATTEMPTS rejections → terminal. Map to
  //     RETRY_EXHAUSTED with the deferral history.
  //   - logs degraded state for observability
  SANITY_CHECK_REJECTED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      const { accountId, intentId, operation, reason } = event;

      const isScheduleRejection = operation === 'schedule_retry';

      if (isScheduleRejection) {
        // Look up existing deferral — if present, increment count
        const existing = _deferredIntents.get(intentId);
        const deferralCount = existing ? existing.deferralCount + 1 : 1;

        if (deferralCount > DEFERRED_MAX_ATTEMPTS) {
          // Too many deferrals — terminal
          _cancelRetry(intentId);
          _deferredIntents.delete(intentId);
          return [
            {
              type: 'LOG_DEGRADED',
              substate: 'DEFERRED_EXHAUSTED',
              reason: `Deferred ${deferralCount - 1} times then rejected: ${reason}`,
            },
            ..._buildExhaustedActions({
              accountId,
              domain: event.domain,
              intentId,
              error: `sanity_check_rejected_after_deferral: ${reason}`,
              operation,
            }),
          ];
        }

        // Defer — keep the held context but mark it deferred
        _deferredIntents.set(intentId, {
          domain: event.domain,
          accountId,
          intentId,
          params: existing?.params || null,
          lastError: existing?.lastError || null,
          actionTag: existing?.actionTag || null,
          retryAfterMs: existing?.retryAfterMs || null,
          ctx: existing?.ctx || null,
          deferredAt: Date.now(),
          deferralCount,
          reason,
        });

        const rawDelay = DEFERRED_BASE_DELAY_MS * Math.pow(DEFERRED_BACKOFF_MULTIPLIER, deferralCount - 1);
        const delayMs = Math.min(rawDelay, DEFERRED_MAX_DELAY_MS);

        return [
          {
            type: 'LOG_DEGRADED',
            substate: 'SANITY_REJECTED_DEFERRED',
            reason: `${operation} rejected (attempt ${deferralCount}/${DEFERRED_MAX_ATTEMPTS}): ${reason}`,
          },
          {
            type: 'DEFERRED_RETRY_SCHEDULED',
            accountId,
            domain: event.domain,
            intentId,
            deferralCount,
            delayMs,
            reason,
          },
        ];
      }

      // Non-schedule rejection (invoke_worker) — terminal
      _cancelRetry(intentId);
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

  // ── SANITY_CHECK_RESUMED — CK is back, drain deferred intents ───────────
  // CK emits CK_RESUMED (HALTED/DEAD → NORMAL) and routes it here as
  // SANITY_CHECK_RESUMED. The FSM drains _deferredIntents by re-running
  // _buildRetrySchedule on each. Deferred intent that succeeds
  // re-schedules its timer. Deferred intent still rejected either
  // increments deferralCount (via the SANITY_CHECK_REJECTED handler) or
  // goes terminal (DEFERRED_MAX_ATTEMPTS exceeded).
  SANITY_CHECK_RESUMED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const drained = [];
      const now = Date.now();

      for (const [intentId, deferred] of _deferredIntents) {
        // Compute per-intent backoff from deferralCount
        const rawDelay = DEFERRED_BASE_DELAY_MS * Math.pow(DEFERRED_BACKOFF_MULTIPLIER, deferred.deferralCount - 1);
        const delayMs = Math.min(rawDelay, DEFERRED_MAX_DELAY_MS);
        const elapsed = now - deferred.deferredAt;

        if (elapsed < delayMs) {
          // Not yet time to retry this one
          continue;
        }

        // Pop from deferred and re-schedule
        _deferredIntents.delete(intentId);

        const result = await _buildRetrySchedule({
          domain: deferred.domain,
          accountId: deferred.accountId,
          intentId: deferred.intentId,
          params: deferred.params || {},
          lastError: deferred.lastError,
          actionTag: deferred.actionTag,
          retryAfterMs: deferred.retryAfterMs,
          ctx: deferred.ctx || ctx,
        });

        if (result.kind === 'scheduled') {
          drained.push({
            type: 'RETRY_RESUMED',
            accountId: deferred.accountId,
            domain: deferred.domain,
            intentId: deferred.intentId,
            delayMs: result.delayMs,
            deferralCount: deferred.deferralCount,
          });
        } else if (result.kind === 'rejected') {
          // Re-rejected — emit SANITY_CHECK_REJECTED to re-enter the
          // deferral path (which will check DEFERRED_MAX_ATTEMPTS)
          drained.push({
            type: 'SANITY_CHECK_REJECTED',
            operation: 'schedule_retry',
            accountId: deferred.accountId,
            domain: deferred.domain,
            intentId: deferred.intentId,
            retryCount: result.newCount,
            reason: result.sanityCheck.reason,
            alternatives: result.sanityCheck.alternatives,
          });
        } else if (result.kind === 'exhausted') {
          // Budget exhausted during drain
          drained.push(...result.actions);
        }
      }

      return drained;
    },
  },

  // ── DB_PERSIST_FAILURE — postgres-telemetry FSM forwarded a failed write ─
  // Phase 2: the writer has classified through the full reliability
  // substrate and emitted a full analysis object. The FSM (acting as the
  // authority vector per Q1) reads the analysis, evaluates all
  // recommendations, and authorizes all flagged ones. For each
  // authorized recommendation, the FSM emits an *_AUTHORIZED action.
  // The candidate is moved to _decidedDbFailures for audit.
  DB_PERSIST_FAILURE: {
    target: () => _localState,
    guard: (event) => {
      if (!event || (!event.analysis && !event.errorShape)) {
        return { allowed: false, reason: 'DB_PERSIST_FAILURE requires analysis or errorShape' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { domain, accountId, intentId, table, analysis, errorShape, error, idempotencyKey, rows } = event;
      // analysis is canonical; errorShape is fallback
      const effectiveCategory = analysis?.category || errorShape?.category || 'UNKNOWN';
      const effectiveSubtype = analysis?.subtype || errorShape?.subtype || 'unknown';
      const effectiveRetryable = analysis?.retryable ?? errorShape?.retryable ?? false;
      const effectiveSeverity = analysis?.severity || 'MEDIUM';
      const effectiveRecommendations = analysis?.recommendations || [];
      const effectiveBackoff = analysis?.backoff || null;
      const effectiveSeverityScore = analysis?.severityScore ?? 50;
      const effectiveIdempotencyKey = idempotencyKey || analysis?.idempotencyKey || null;
      const candidateIntentId = `db-failure-${table}-${intentId || Date.now()}-${accountId || '*'}`;

      // Move to decided map — full analysis retained for audit
      _decidedDbFailures.set(candidateIntentId, {
        domain: domain || 'persist-telemetry',
        accountId: accountId || '*',
        intentId: candidateIntentId,
        table,
        rows: rows || [],
        analysis,
        errorShape,
        error,
        queuedAt: Date.now(),
        decidedAt: Date.now(),
        source: 'db_persist_failure',
        authorizedRecommendations: [...effectiveRecommendations],
        idempotencyKey: effectiveIdempotencyKey,
      });

      const actionList = [{
        type: 'LOG_DEGRADED',
        substate: 'DB_FAILURE_ANALYZED',
        reason: `DB failure analyzed: ${domain}/${table} category=${effectiveCategory} subtype=${effectiveSubtype} retryable=${effectiveRetryable} severity=${effectiveSeverity} recommendations=[${effectiveRecommendations.join(',')}]`,
        severity: effectiveSeverity,
        severityScore: effectiveSeverityScore,
      }];

      // FSM is the authority vector (Q1): authorize ALL flagged recommendations
      for (const rec of effectiveRecommendations) {
        switch (rec) {
          case 'RETRY_OPERATION':
            actionList.push({
              type: 'RETRY_OPERATION_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              table,
              rows: rows || [],
              backoff: effectiveBackoff,
              idempotencyKey: effectiveIdempotencyKey,
              analysis,
            });
            break;
          case 'THROTTLE_WORKLOAD':
            actionList.push({
              type: 'THROTTLE_WORKLOAD_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              recommendedConcurrencyDelta: analysis?.resourceExhaustion?.recommendedConcurrencyDelta ?? -1,
              analysis,
            });
            break;
          case 'REFRESH_AUTHENTICATION':
            actionList.push({
              type: 'REFRESH_AUTHENTICATION_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              analysis,
            });
            break;
          case 'RECONCILE_STATE':
            actionList.push({
              type: 'RECONCILE_STATE_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              table,
              analysis,
            });
            break;
          case 'REPAIR_SCHEMA':
            actionList.push({
              type: 'REPAIR_SCHEMA_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              analysis,
            });
            break;
          case 'REBUILD_CACHE':
            actionList.push({
              type: 'REBUILD_CACHE_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              analysis,
            });
            break;
          case 'ESCALATE_TO_OPERATOR':
            actionList.push({
              type: 'ESCALATE_TO_OPERATOR_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              category: effectiveCategory,
              subtype: effectiveSubtype,
              severity: effectiveSeverity,
              table,
              analysis,
            });
            break;
          case 'DEFER_EXECUTION':
            actionList.push({
              type: 'DEFER_EXECUTION_AUTHORIZED',
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
              intentId: candidateIntentId,
              analysis,
            });
            break;
          default:
            // Unknown recommendation — log but don't block
            actionList.push({
              type: 'RECOMMENDATION_UNKNOWN',
              recommendation: rec,
              domain: domain || 'persist-telemetry',
              accountId: accountId || '*',
            });
        }
      }

      return actionList;
    },
  },

  // ── DB_PERSIST_FAILURE_READ — postgres-telemetry FSM forwarded a failed read ─
  DB_PERSIST_FAILURE_READ: {
    target: () => _localState,
    guard: (event) => {
      if (!event || (!event.analysis && !event.errorShape)) {
        return { allowed: false, reason: 'DB_PERSIST_FAILURE_READ requires analysis or errorShape' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { readDomain, accountId, readId, analysis, errorShape, error, readParams } = event;
      const effectiveCategory = analysis?.category || errorShape?.category || 'UNKNOWN';
      const effectiveSubtype = analysis?.subtype || errorShape?.subtype || 'unknown';
      const effectiveRetryable = analysis?.retryable ?? errorShape?.retryable ?? false;
      const effectiveSeverity = analysis?.severity || 'MEDIUM';
      const effectiveRecommendations = analysis?.recommendations || [];
      const candidateIntentId = `db-read-failure-${readDomain}-${readId}-${accountId || '*'}`;

      _decidedDbReadFailures.set(candidateIntentId, {
        readDomain,
        accountId: accountId || '*',
        intentId: candidateIntentId,
        readId,
        analysis,
        errorShape,
        error,
        queuedAt: Date.now(),
        decidedAt: Date.now(),
        source: 'db_persist_failure_read',
        authorizedRecommendations: [...effectiveRecommendations],
      });

      const actionList = [{
        type: 'LOG_DEGRADED',
        substate: 'DB_READ_FAILURE_ANALYZED',
        reason: `DB read failure analyzed: ${readDomain}/${accountId || '*'} category=${effectiveCategory} subtype=${effectiveSubtype} retryable=${effectiveRetryable} severity=${effectiveSeverity} recommendations=[${effectiveRecommendations.join(',')}]`,
      }];

      // Authorize all flagged recommendations for reads too
      for (const rec of effectiveRecommendations) {
        if (rec === 'RETRY_OPERATION') {
          actionList.push({
            type: 'RETRY_OPERATION_AUTHORIZED',
            readDomain, accountId: accountId || '*',
            intentId: candidateIntentId,
            readId,
            readParams: readParams || null,
            analysis,
          });
        } else if (rec === 'RECONCILE_STATE') {
          actionList.push({
            type: 'RECONCILE_STATE_AUTHORIZED',
            readDomain, accountId: accountId || '*',
            intentId: candidateIntentId,
            analysis,
          });
        } else if (rec === 'ESCALATE_TO_OPERATOR') {
          actionList.push({
            type: 'ESCALATE_TO_OPERATOR_AUTHORIZED',
            readDomain, accountId: accountId || '*',
            intentId: candidateIntentId,
            category: effectiveCategory,
            subtype: effectiveSubtype,
            severity: effectiveSeverity,
            analysis,
          });
        }
      }

      return actionList;
    },
  },

  // ── RETRY_OPERATION_AUTHORIZED — delegate to retry-execution-substrate ──
  // Phase 3: the FSM delegates operation logic to the recovery substrate.
  // The substrate owns the dispatch decision (connection, backoff, timeout).
  // The FSM only authorizes; the substrate executes.
  //
  // Phase 4: domain-aware dispatch. For publish:* domains, the IG
  // recovery substrate is consulted first; for non-IG domains, the
  // persistence retry-execution-substrate handles it. This is the
  // pattern that lets one transition serve both substrates.
  RETRY_OPERATION_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const governance = ctx || event._governance || null;
      const domain = event?.domain || event?.params?.domain || null;
      // Route IG domains to the IG recovery substrate (Phase 4)
      const isIgDomain = typeof domain === 'string' && domain.startsWith('publish:');
      const substrate = isIgDomain
        ? require('./substrates/ig-recovery-substrate')
        : require('./substrates/retry-execution-substrate');
      const result = await substrate.execute({ ...event, type: 'RETRY_OPERATION_AUTHORIZED' }, governance);
      return [{
        type: isIgDomain ? 'IG_RECOVERY_DELEGATED' : 'RETRY_EXECUTION_DELEGATED',
        success: result.success,
        workerName: result.workerName,
        durationMs: result.durationMs,
        error: result.error || null,
        recommendation: 'REQUEUE_OPERATION',
      }];
    },
  },

  // ── THROTTLE_WORKLOAD_AUTHORIZED — delegate to throttle-substrate ────────
  THROTTLE_WORKLOAD_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/throttle-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'THROTTLE_WORKLOAD_AUTHORIZED' }, governance);
      return [{
        type: 'THROTTLE_DELEGATED',
        success: result.success,
        persistent: result.persistent || false,
        durationMs: result.durationMs,
      }];
    },
  },

  // ── REFRESH_AUTHENTICATION_AUTHORIZED — delegate to auth-recovery-substrate
  REFRESH_AUTHENTICATION_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/auth-recovery-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'REFRESH_AUTHENTICATION_AUTHORIZED' }, governance);
      return [{
        type: 'AUTH_RECOVERY_DELEGATED',
        success: result.success,
        durationMs: result.durationMs,
        error: result.error || null,
      }];
    },
  },

  // ── RECONCILE_STATE_AUTHORIZED — delegate to reconciliation-substrate ─────
  RECONCILE_STATE_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/reconciliation-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'RECONCILE_STATE_AUTHORIZED' }, governance);
      return [{
        type: 'RECONCILIATION_DELEGATED',
        success: result.success,
        resolution: result.resolution || null,
        durationMs: result.durationMs,
        error: result.error || null,
      }];
    },
  },

  // ── REPAIR_SCHEMA_AUTHORIZED — delegate to maintenance-substrate ──────────
  REPAIR_SCHEMA_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/maintenance-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'REPAIR_SCHEMA_AUTHORIZED' }, governance);
      return [{
        type: 'SCHEMA_REPAIR_DELEGATED',
        success: result.success,
        durationMs: result.durationMs,
        error: result.error || null,
      }];
    },
  },

  // ── REBUILD_CACHE_AUTHORIZED — delegate to maintenance-substrate ──────────
  REBUILD_CACHE_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/maintenance-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'REBUILD_CACHE_AUTHORIZED' }, governance);
      return [{
        type: 'CACHE_REPAIR_DELEGATED',
        success: result.success,
        durationMs: result.durationMs,
        error: result.error || null,
      }];
    },
  },

  // ── ESCALATE_TO_OPERATOR_AUTHORIZED — delegate to escalation-substrate ────
  ESCALATE_TO_OPERATOR_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/escalation-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'ESCALATE_TO_OPERATOR_AUTHORIZED' }, governance);
      return [{
        type: 'ESCALATION_DELEGATED',
        success: result.success,
        durationMs: result.durationMs,
        error: result.error || null,
      }];
    },
  },

  // ── IG-specific *_AUTHORIZED transitions ──────────────────────────────────
  // The IG reliability substrate emits 12 IG-specific recommendations
  // (REFRESH_TOKEN, VALIDATE_PERMISSIONS, REBUILD_WEBHOOK_STATE,
  // VERIFY_PUBLICATION, RECOVER_MEDIA_CONTAINER, etc.). These
  // transition handlers delegate to the ig-recovery-substrate façade,
  // which dispatches to the appropriate operationally bounded worker.

  REFRESH_TOKEN_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/ig-recovery-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'REFRESH_TOKEN_AUTHORIZED' }, governance);
      return [{
        type: 'IG_RECOVERY_DELEGATED',
        success: result.success,
        workerName: result.workerName,
        durationMs: result.durationMs,
        error: result.error || null,
        recommendation: 'REFRESH_TOKEN',
      }];
    },
  },

  VERIFY_PUBLICATION_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/ig-recovery-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'VERIFY_PUBLICATION_AUTHORIZED' }, governance);
      return [{
        type: 'IG_RECOVERY_DELEGATED',
        success: result.success,
        workerName: result.workerName,
        durationMs: result.durationMs,
        error: result.error || null,
        recommendation: 'VERIFY_PUBLICATION',
      }];
    },
  },

  RECOVER_MEDIA_CONTAINER_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/ig-recovery-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'RECOVER_MEDIA_CONTAINER_AUTHORIZED' }, governance);
      return [{
        type: 'IG_RECOVERY_DELEGATED',
        success: result.success,
        workerName: result.workerName,
        durationMs: result.durationMs,
        error: result.error || null,
        recommendation: 'RECOVER_MEDIA_CONTAINER',
      }];
    },
  },

  REBUILD_WEBHOOK_STATE_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/ig-recovery-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'REBUILD_WEBHOOK_STATE_AUTHORIZED' }, governance);
      return [{
        type: 'IG_RECOVERY_DELEGATED',
        success: result.success,
        workerName: result.workerName,
        durationMs: result.durationMs,
        error: result.error || null,
        recommendation: 'REBUILD_WEBHOOK_STATE',
      }];
    },
  },

  VALIDATE_PERMISSIONS_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/ig-recovery-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'VALIDATE_PERMISSIONS_AUTHORIZED' }, governance);
      return [{
        type: 'IG_RECOVERY_DELEGATED',
        success: result.success,
        workerName: result.workerName,
        durationMs: result.durationMs,
        error: result.error || null,
        recommendation: 'VALIDATE_PERMISSIONS',
      }];
    },
  },

  PROACTIVE_REFRESH_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/ig-recovery-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'PROACTIVE_REFRESH_AUTHORIZED' }, governance);
      return [{
        type: 'IG_RECOVERY_DELEGATED',
        success: result.success,
        workerName: result.workerName,
        durationMs: result.durationMs,
        error: result.error || null,
        recommendation: 'PROACTIVE_REFRESH',
      }];
    },
  },

  // ── DEFER_EXECUTION_AUTHORIZED — delegate to deferral-substrate ───────────
  DEFER_EXECUTION_AUTHORIZED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const substrate = require('./substrates/deferral-substrate');
      const governance = ctx || event._governance || null;
      const result = await substrate.execute({ ...event, type: 'DEFER_EXECUTION_AUTHORIZED' }, governance);
      return [{
        type: 'DEFER_DELEGATED',
        success: result.success,
        queueSize: result.queueSize || 0,
        durationMs: result.durationMs,
      }];
    },
  },

  // ── CRITICAL_FAILURE_OBSERVED (retry-cadence domain) ────────────────────
  // The persist-telemetry FSM emitted CRITICAL_FAILURE_OBSERVED; the
  // retry-cadence FSM receives it here for tracking.
  CRITICAL_FAILURE_OBSERVED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'LOG_CRITICAL',
      substate: 'RETRY_CADENCE_CRITICAL_OBSERVED',
      reason: `CRITICAL failure routed through retry-cadence: ${event.category}/${event.subtype}`,
    }],
  },

  // ── HIGH_FAILURE_OBSERVED (retry-cadence domain) ────────────────────────
  HIGH_FAILURE_OBSERVED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'RETRY_CADENCE_HIGH_OBSERVED',
      reason: `HIGH failure routed through retry-cadence: ${event.category}/${event.subtype}`,
    }],
  },

  // ── IG_FAILURE_OBSERVED — constitutional IG failure intake ────────────
  // Emitted by IG workers (publishing-kernel substrates, transport
  // boundary) carrying the raw error and a CHEAP suspected_category
  // hint. The FSM is the routing membrane — it does NOT classify.
  // The FSM calls the IG reliability substrate via the registered
  // classification worker (CLASSIFICATION_WORKER_MAP maps publish:*
  // domains to ig-reliability-substrate). The substrate returns
  // the canonical analysis (§1-§17). The FSM emits *_AUTHORIZED
  // actions per analysis.recommendations.
  //
  // For publish:* domains, the FSM ALSO emits PUBLISH_RETRY_EXHAUSTED
  // when the retry budget is exhausted, so the publishing FSM's
  // EXECUTING → IDLE transition fires. This re-arms the terminal
  // path that was removed in the publish-retry-worker purge.
  //
  // Kernels MUST NOT call the substrate directly. This transition
  // is the canonical seam.
  IG_FAILURE_OBSERVED: {
    target: () => _localState,
    guard: (event) => {
      if (!event || (!event.rawError && !event.error)) {
        return { allowed: false, reason: 'IG_FAILURE_OBSERVED requires rawError or error' };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { accountId, intentId, domain, rawError, error, suspectedCategory,
              endpoint, workerName, tokenMetadata, publicationState,
              containerId, publicationId, webhookState, dependencyHealth,
              correlationIds, attemptN = 1, lineageId = null } = event || {};

      // Resolve the canonical IG classification worker (the substrate)
      const substrateRegistry = require('../acquisition-kernel/substrate-registry');
      const igSubstrate = substrateRegistry.getClassificationWorker(domain);

      if (!igSubstrate || typeof igSubstrate.analyzeFailure !== 'function') {
        // No substrate registered for this domain — fall through to
        // log and skip. The worker should be registered in the
        // CLASSIFICATION_WORKER_MAP; if it's missing, that's a
        // registration bug.
        return [{
          type: 'LOG_DEGRADED',
          substate: 'IG_SUBSTRATE_MISSING',
          reason: `No IG reliability substrate registered for domain: ${domain}`,
          domain, accountId, intentId,
        }];
      }

      // §1-§17: canonical analysis. The substrate is the only
      // entity that classifies the raw error.
      const analysis = igSubstrate.analyzeFailure(
        rawError || error,
        domain,
        'ig-graph',
        {
          accountId, intentId, attemptN, lineageId,
          lineageDomain: 'ig-domain', workerName,
          endpoint, tokenMetadata, publicationState,
          containerId, publicationId, webhookState,
          dependencyHealth, correlationIds,
          // The worker's cheap hint is passed through context
          // for the substrate's priority/severity heuristics.
          suspectedCategory: suspectedCategory || null,
        }
      );

      // Queue the decided failure (mirrors _decidedDbFailures).
      // Phase 2 will add real workers; for now the queue is
      // observable but does not trigger execution.
      const decided = {
        domain, accountId, intentId, rawError: rawError || error,
        analysis, error: error || null, queuedAt: Date.now(),
        decidedAt: Date.now(),
        authorizedRecommendations: analysis.recommendations.slice(),
        idempotencyKey: analysis.idempotencyKey,
      };
      _decidedIgFailures.set(intentId || `${accountId}:${Date.now()}`, decided);

      // Emit *_AUTHORIZED actions per the substrate's recommendations.
      // The FSM authorizes ALL flagged ones (per spec — the substrate
      // does NOT pick one). The *_AUTHORIZED handlers then delegate
      // to the appropriate recovery substrate (mirrors the persistence
      // path's REFRESH_AUTHENTICATION_AUTHORIZED etc.).
      const actions = [];
      const recs = analysis.recommendations || [];
      for (const rec of recs) {
        const actionType = `${rec}_AUTHORIZED`;
        actions.push({
          type: actionType,
          accountId, intentId, domain,
          analysis,  // full canonical analysis for the recovery substrate
          retryCount: attemptN,
        });
      }

      // Severity-graded observability
      if (analysis.severity === 'CRITICAL') {
        actions.push({
          type: 'CRITICAL_FAILURE_OBSERVED',
          category: analysis.category,
          subtype: analysis.subtype,
          domain, accountId, intentId,
          severityScore: analysis.severityScore,
        });
      } else if (analysis.severity === 'HIGH') {
        actions.push({
          type: 'HIGH_FAILURE_OBSERVED',
          category: analysis.category,
          subtype: analysis.subtype,
          domain, accountId, intentId,
          severityScore: analysis.severityScore,
        });
      }

      // Re-arm PUBLISH_RETRY_EXHAUSTED for publish domains on
      // retry-budget exhaustion. This closes the publishing FSM's
      // EXECUTING → IDLE transition that was orphaned in the
      // publish-retry-worker purge.
      const isPublishDomain = typeof domain === 'string' && domain.startsWith('publish:');
      const budgetExhausted = attemptN >= (analysis.retryabilityReason?.includes('non_retryable') ? 1 : 3);
      const wantsRequeue = recs.includes('REQUEUE_OPERATION');
      if (isPublishDomain && budgetExhausted && !wantsRequeue) {
        actions.push({
          type: 'PUBLISH_RETRY_EXHAUSTED',
          accountId, intentId, domain,
          error: rawError?.message || error?.message || 'publish_retry_exhausted',
          igCode: analysis.subtype,
          retryCount: attemptN,
          operation: domain,
          analysis,
        });
      }

      return actions;
    },
  },

  // ── WORKER_RESULT — record every CK-invoked worker outcome ──────────────
  // Emitted by CK.invokeWorker. The engagement FSM tracks worker outcomes
  // per business account for failure escalation and circuit breaker state.
  WORKER_RESULT: {
    target: null,
    guard: (event) => {
      if (!event.accountId || !event.workerName) {
        return { allowed: false, reason: 'WORKER_RESULT requires accountId, workerName' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const cred = _credRecords.get(event.accountId) || _createCredRecord(event.accountId);
      if (cred) {
        if (!cred.workerLog) cred.workerLog = [];
        cred.workerLog.push({
          workerName: event.workerName,
          outcome: event.outcome,
          error: event.error || null,
          at: Date.now(),
        });
        if (cred.workerLog.length > 100) cred.workerLog.shift();
        if (event.outcome === 'failed') {
          cred.consecutiveFailures = (cred.consecutiveFailures || 0) + 1;
        } else if (event.outcome === 'completed') {
          cred.consecutiveFailures = 0;
        }
      }
      return [];
    },
  },
};

function _emitWorkerResult(workerName, result) {
  const obs = _obs();
  if (!obs) return;
  const outcome = (result && result.status) || (result && !result.error ? 'completed' : 'failed');
  obs.transition({
    domain: 'engagement',
    entity: 'worker_result',
    entityId: `engagement:${workerName}`,
    previousState: null,
    nextState: 'WORKER_RESULT',
    authority: 'engagement-fsm',
    raw: { workerName, domain: 'engagement', accountId: null, outcome, data: (result && result.data) || null, error: (result && result.error) || null, invokedAt: Date.now() },
  });
}

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

// ── Deferred intents (R7): intentId → DeferredIntent ─────────────────────
// When CK rejects a schedule (HALTED/DEAD), the intent lands here instead
// of going terminal. The intent waits for SANITY_CHECK_RESUMED (emitted
// when CK transitions HALTED/DEAD → NORMAL). Drain logic in the
// SANITY_CHECK_RESUMED handler re-calls _buildRetrySchedule per intent.
//
// DeferredIntent:
//   { domain, accountId, intentId, params, lastError, actionTag,
//     retryAfterMs, ctx, deferredAt, deferralCount, reason }
const _deferredIntents = new Map();

// ── DB failure candidates (base phase): intentId → CandidateDbFailure ────
// When a postgres-telemetry write fails, the writer classifies through
// persistence-failure-substrate, the persist-telemetry FSM forwards via
// DB_PERSIST_FAILURE, and the candidate lands here. In the base phase
// we only queue and emit telemetry; the real retry attempt comes in
// phase 2 when the persist-telemetry retry worker is instantiated.
//
// CandidateDbFailure (write):
//   { domain, accountId, intentId, table, errorShape, error,
//     queuedAt, source }
// CandidateDbFailure (read):
//   { readDomain, accountId, intentId, readId, errorShape, error,
//     queuedAt, source }
const _candidateDbFailures = new Map();
const _candidateDbReadFailures = new Map();

// ── Decided DB failures (phase 2): intentId → DecidedDbFailure ───────────
// After the FSM (authority vector) authorizes recommendations, the
// candidate moves from _candidateDbFailures to _decidedDbFailures.
// Phase 3's real workers consume these to execute the actual retry,
// throttle, reconcile, or escalate actions.
//
// DecidedDbFailure:
//   { domain, accountId, intentId, table, analysis, errorShape, error,
//     queuedAt, decidedAt, authorizedRecommendations, idempotencyKey }
const _decidedDbFailures = new Map();
const _decidedDbReadFailures = new Map();

// ── Decided IG failures (parallel to _decidedDbFailures) ───────────
// The constitutional IG failure path: workers emit IG_FAILURE_OBSERVED
// with raw error + cheap hint. The engagement-fsm (this FSM) calls
// ig-reliability-substrate.analyzeFailure() and queues the result here.
// Phase 2 will instantiate the actual recovery workers that consume
// from this map.
//
// DecidedIgFailure:
//   { domain, accountId, intentId, rawError, analysis, error,
//     queuedAt, decidedAt, authorizedRecommendations, idempotencyKey }
const _decidedIgFailures = new Map();

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
    // R5: startup assertion — the CTX gate must be wired at install time.
    // The legacy direct-invocation fallback in _executeRetry has been
    // removed; without ctx.invokeWorker, every retry would fail with
    // "gate not wired" at fire time. Fail-loud here so a misconfigured
    // boot is caught immediately, not on the first failure.
    if (typeof governance.invokeWorker !== 'function') {
      throw new Error('[engagement-fsm] R5: setGovernance() called with a governance that does not expose invokeWorker(). The legacy fallback has been removed; the CTX gate is required.');
    }
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
 * Build and schedule a retry attempt — the canonical retry-schedule path.
 *
 * Both RETRY_REQUESTED (external entry) and WORKER_OUTCOME_REPORTED →
 * TRANSIENT_RETRY (worker outcome) funnel through this helper. The
 * canonical budget + sanity + timer logic lives here; the two callers
 * just supply their entry-specific input shape.
 *
 * Steps:
 *   1. paired-dispatch (budget lookup, paired workers, policy)
 *   2. budget check — if exceeded, return exhausted result
 *   3. build ExecutionContext
 *   4. _scheduleRetry (sanity gate + timer)
 *   5. on rejection: caller emits SANITY_CHECK_REJECTED
 *   6. on success: caller emits domain-specific RETRY_IN_PROGRESS (or none)
 *
 * The caller decides what to do with the three outcomes via the return shape:
 *   { kind: 'exhausted', actions, newCount }   — budget gone
 *   { kind: 'rejected', sanityCheck, newCount } — sanity said no
 *   { kind: 'scheduled', delayMs, newCount }   — timer is set
 *
 * @param {object} input
 * @param {string} input.domain
 * @param {string} input.accountId
 * @param {string} input.intentId
 * @param {object} input.params — opaque worker params
 * @param {object|null} input.lastError — errorShape or null
 * @param {object|null} input.actionTag — classification output (delay override)
 * @param {number|null} input.retryAfterMs — explicit delay override
 * @param {object} input.ctx — dispatch ctx (for ctx.sanityCheck)
 * @returns {Promise<
 *   | { kind: 'exhausted', actions: Array, newCount: number }
 *   | { kind: 'rejected', sanityCheck: object, newCount: number }
 *   | { kind: 'scheduled', delayMs: number, newCount: number }
 * >}
 */
async function _buildRetrySchedule(input) {
  const { domain, accountId, intentId, params, lastError, actionTag, retryAfterMs, ctx } = input;

  // 1. paired-dispatch
  const paired = retryCadenceStore.dispatch(
    domain, accountId, intentId, params || {});
  const existing = _executionContexts.get(intentId);
  // R11: count advances ONLY when sanity approves. The budget check
  // below compares existing.count against maxRetries; the count is
  // committed to the context AFTER sanity.allowed returns true. This
  // means a sanity-rejected attempt does not consume the retry budget.
  const maxRetries = paired.maxRetries || 0;

  // 2. budget check (read-only — does not advance count)
  if (existing && existing.count >= maxRetries) {
    _cancelRetry(intentId);
    return {
      kind: 'exhausted',
      actions: _buildExhaustedActions({
        accountId, domain, intentId,
        error: 'max_retries_exceeded',
        retryCount: existing.count,
      }),
      newCount: existing.count,
    };
  }

  // 3. ExecutionContext — count is the prospective count, committed later
  const prospectiveCount = existing ? existing.count + 1 : 0;
  const context = {
    domain, accountId, intentId,
    params: params || {},
    count: existing ? existing.count : 0,  // current count, not yet advanced
    maxRetries,
    timeoutId: null,
    retryWorker: paired.retryWorker,
    classificationWorker: paired.classificationWorker,
    policy: paired.policy,
    lastError: lastError || null,
    scheduledAt: null,
    governance: _governance, // passed to worker on invocation
    invokeWorker: ctx.invokeWorker, // CTX gate — ownership, contract, sanity
    workerName: _resolveWorkerName(domain, { params: params || {} }),
    // internal — passed to _scheduleRetry, NOT in worker-facing shape
    _prospectiveCount: prospectiveCount,
  };

  // 4. actionTag resolution — explicit override wins, then classification
  const effectiveActionTag = retryAfterMs ? { retryAfterMs } : actionTag;

  // 5. schedule (sanity gate + timer)
  const scheduleResult = await _scheduleRetry(context, effectiveActionTag, ctx);
  if (!scheduleResult.scheduled) {
    // R11: count NOT advanced. Rejection does not burn budget.
    return {
      kind: 'rejected',
      sanityCheck: scheduleResult.sanityCheck,
      newCount: existing ? existing.count : 0,
    };
  }

  // R11: count advanced only now, post-sanity
  const finalCount = existing ? existing.count + 1 : 1;
  context.count = finalCount;
  _executionContexts.set(intentId, context);

  return {
    kind: 'scheduled',
    delayMs: scheduleResult.delayMs,
    newCount: finalCount,
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
    // Publish retry workers — REMOVED. Publishing FSM owns publish
    // failure handling via the publish substrate's classified
    // errorShape. No retry-cadence worker lookup for publish:*.
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
 * R5: routes ONLY through the CTX gate (ctx.invokeWorker) — validates
 * ownership, contract, and sanity before invocation. The legacy direct
 * retryWorker.execute() path has been removed; production callers MUST
 * wire ctx.invokeWorker at install time. The startup assertion in
 * setGovernance() verifies the gate is present at boot.
 *
 * @param {object} context — ExecutionContext (must include invokeWorker, workerName, governance)
 * @param {object|null} fsmCtx — the dispatch ctx (for ctx.sanityCheck)
 */
async function _executeRetry(context, fsmCtx) {
  // R5: CTX gate path is the ONLY path. No fallback.
  if (!context.invokeWorker || !context.workerName) {
    console.error(`[engagement-fsm] R5: ctx.invokeWorker or workerName missing for ${context.intentId} — gate not wired. The legacy direct-invocation fallback has been removed.`);
    return;
  }
  if (!context.governance) {
    console.error(`[engagement-fsm] No governance in context for ${context.intentId} — worker cannot dispatch`);
    return;
  }
  try {
    const result = await context.invokeWorker(context.workerName, {
      domain: context.domain,
      accountId: context.accountId,
      intentId: context.intentId,
      params: context.params,
      retryCount: context.count,
      maxRetries: context.maxRetries,
      governance: context.governance,
    });
    _emitWorkerResult(context.workerName, result);
  } catch (err) {
    console.error(`[engagement-fsm] CTX gate blocked worker '${context.workerName}' for ${context.intentId}:`, err.message);
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
  // Publish retries no longer flow through the retry-cadence path.
  // The publishing FSM owns its own terminal failure signal.
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
    // R12: read source/lag as top-level fields on the params object.
    // Callers of _buildExhaustedActions for telemetry-coordination
    // pass { accountId, domain, intentId, error, retryCount, source, lag }.
    // The previous params.params?.source path always returned undefined
    // because no caller passed a nested `params` field.
    actions.push({
      type: 'TELEMETRY_RETRY_EXHAUSTED',
      accountId: params.accountId,
      domain: params.domain,
      intentId: params.intentId,
      error: params.error,
      retryCount: params.retryCount,
      source: params.source,
      lag: params.lag,
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
async function _syncProjectionState() {
  try {
    const { getRedisClient } = require('../config/redis');
    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      const raw = await redis.get('lineage:projection:domain:engagement');
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && parsed.projection && parsed.projection.state) {
          if (typeof _localState !== 'undefined') {
            _localState = parsed.projection.state;
          }
        }
      }
    }
  } catch (_) {}
}

async function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  await _syncProjectionState();

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

  // 6. Build actions — await async handlers (Phase 3 substrates and workers
  // return Promises; the dispatch must resolve them before returning)
  const actions = txn.buildActions
    ? await txn.buildActions(event, ctx)
    : [];

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

function getDeferredIntents() {
  return new Map(_deferredIntents);
}

// ── DB failure candidate accessors (base phase observability) ─────────────
// Phase 2's real retry worker will consume these. Exposed now so the
// wiring can be inspected without waiting for phase 2.
function getCandidateDbFailures() {
  return new Map(_candidateDbFailures);
}

function getCandidateDbReadFailures() {
  return new Map(_candidateDbReadFailures);
}

// ── Decided DB failure accessors (phase 2 audit) ──────────────────────────
function getDecidedDbFailures() {
  return new Map(_decidedDbFailures);
}

function getDecidedDbReadFailures() {
  return new Map(_decidedDbReadFailures);
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
  getDeferredIntents,
  getCandidateDbFailures,
  getCandidateDbReadFailures,
  getDecidedDbFailures,
  getDecidedDbReadFailures,
};
