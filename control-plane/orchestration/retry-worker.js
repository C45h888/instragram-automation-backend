// control-plane/orchestration/retry-worker.js
// Bounded single-attempt execution worker.
//
// Owns: one execution attempt — fetch + persist + observation emission,
//        retry count tracking per intentId.
// Does NOT own: retry policy (governed by engagement-fsm), auth/cb state,
//               execution mechanics beyond single bounded attempt.
//
// This worker is invoked by the acquisition orchestrator for EACH attempt.
// It tracks retry count per intentId and emits engagement signals directly
// to CK (DOMAIN_EVENT_MAP routes them to engagement-fsm).
//
// Constitutional invariant:
//   One attempt → one EXECUTION_OBSERVATION upward
//   Engagement signals (AUTH_SUCCESS, AUTH_FAILURE_STRIKE, RATE_LIMIT_DETECTED,
//   RETRY_COUNT_INCREMENTED, RETRY_EXHAUSTED) emitted directly to CK by this worker
//   CK routes via DOMAIN_EVENT_MAP to engagement-fsm — no acquisition-fsm involvement

const retry = require('../../substrates/retry');
const quota = require('../../substrates/quota');
const telemetry = require('../../substrates/telemetry');
const metricsSubstrate = require('../../substrates/metrics-substrate');

// ── Retry count tracking (per intentId, for engagement signal emission) ──────
const _retryCounts = new Map(); // intentId → retry count

const MAX_ACQUISITION_RETRIES = 1;

/**
 * Execute a single bounded attempt for one acquisition intent.
 *
 * Performs one fetch + persist cycle, records telemetry/quota/metrics,
 * then emits EXECUTION_OBSERVATION upward to governance for evaluation.
 * Governance alone decides retry/escalation/failure.
 *
 * @param {string} accountId - business account UUID
 * @param {string} domain - 'comments'|'messages'|'publish:media'|etc.
 * @param {object} params - intent parameters passed to routing.fetch()
 * @param {string} intentId - acquisition intent ID (for observability)
 * @param {object} governance - governance kernel module (for observation emission)
 * @param {{ fetch: Function, persist: Function }} routing - domain registry entry
 * @returns {Promise<{ status: 'completed'|'failed', count: number, error: string|null, instagram_id: string|null }>}
 */
async function executeSingle(accountId, domain, params, intentId, governance, routing) {
  const startTime = Date.now();

  // ── Pre-flight: circuit breaker check — routed through CK to engagement FSM ─
  // Constitutional hierarchy: the engagement FSM is the SOLE authority on circuit breaker state.
  // Execution layers must dispatch through CK, not query state directly.
  const breakerResult = governance.dispatch({
    type: 'CIRCUIT_BREAKER_CHECK',
    accountId,
    domain,
    intentId,
  });
  const isActive = breakerResult && breakerResult.actions && breakerResult.actions.some(
    a => a.type === 'CIRCUIT_BREAKER_ACTIVE'
  );
  if (isActive) {
    console.log(`[retry-worker] ${domain}/${accountId} circuit-breaker active, skipping intent ${intentId}`);
    await _recordFailure(domain, accountId, intentId, 'rate_limited', 0);
    _emitTransition(intentId, 'PENDING', 'SKIPPED', { accountId, domain, reason: 'circuit_breaker' });
    _emitObservation(governance, accountId, intentId, domain, 'failed', {
      error_category: 'rate_limit',
      retryable: false,
      count: 0,
      latencyMs: 0,
      error: 'circuit_breaker_active',
    });
    return { status: 'failed', count: 0, error: 'circuit_breaker_active', instagram_id: null };
  }

  // Observability: attempt start transition
  _emitTransition(intentId, 'PENDING', 'ATTEMPTING', { accountId, domain });

  // ── Single bounded attempt ───────────────────────────────────────────────
  let result;
  try {
    result = await routing.fetch(accountId, params);
  } catch (err) {
    result = { success: false, count: 0, error: err.message };
  }

  const latencyMs = Date.now() - startTime;

  // ── Quota tracking ───────────────────────────────────────────────────────
  if (result._usagePct != null) {
    quota.updateQuotaUsage(accountId, result._usagePct);
  }

  // ── Persist on success ───────────────────────────────────────────────────
  if (result.success) {
    try {
      const persistResult = await routing.persist(accountId, result);
      result.count = persistResult?.count || result.count || 0;
      result.instagram_id = result.instagram_id || null;
    } catch (persistErr) {
      result = { success: false, count: 0, error: persistErr.message };
    }
  }

  // ── Error classification (mechanical — retry substrate only) ────────────
  const { skip, break: brk, retryable, retryAfterMs } = retry.handleFetchError(result, accountId);

  // ── Emit engagement signals directly to CK ─────────────────────────────
  // Constitutional hierarchy: engagement-fsm is the SOLE authority on engagement state.
  // retry-worker emits signals directly to CK; DOMAIN_EVENT_MAP routes them to
  // engagement-fsm. No acquisition-fsm involvement in engagement signal origination.
  if (result.success) {
    _emitEngagementSignal(governance, 'AUTH_SUCCESS', { accountId, intentId });
  } else if (brk) {
    // Rate limit → engagement-fsm manages circuit breaker via CK routing
    _emitEngagementSignal(governance, 'RATE_LIMIT_DETECTED', {
      accountId, cooldownMs: (retryAfterMs || 3600000),
    });
  } else if (skip) {
    // Auth failure → engagement-fsm manages auth strikes via CK routing
    _emitEngagementSignal(governance, 'AUTH_FAILURE_STRIKE', {
      accountId, error: result.error,
    });
  } else if (retryable) {
    const retryCount = (_retryCounts.get(intentId) || 0) + 1;
    _retryCounts.set(intentId, retryCount);
    if (retryCount > MAX_ACQUISITION_RETRIES) {
      // Retries exhausted → engagement-fsm receives RETRY_EXHAUSTED via CK routing
      _emitEngagementSignal(governance, 'RETRY_EXHAUSTED', {
        accountId, domain, intentId, error: 'max_retries_exceeded',
      });
      _retryCounts.delete(intentId);
    } else {
      _emitEngagementSignal(governance, 'RETRY_COUNT_INCREMENTED', { intentId, retryCount });
    }
  }

  // ── Record outcome to telemetry + metrics ───────────────────────────────
  if (result.success) {
    await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
    metricsSubstrate.record(domain, 'completed', latencyMs, accountId);
  } else {
    await _recordFailure(domain, accountId, intentId, _errorTag(result, skip, brk, retryable), latencyMs);
  }

  // Observability: attempt result transition (COMPLETED or FAILED)
  _emitTransition(intentId, 'ATTEMPTING', result.success ? 'COMPLETED' : 'FAILED', {
    accountId, domain,
    error_category: result.success ? null : (skip ? 'auth_failure' : brk ? 'rate_limit' : retryable ? 'transient' : 'permanent'),
  });

  // ── Emit observation upward to governance ───────────────────────────────
  _emitObservation(governance, accountId, intentId, domain, result.success ? 'completed' : 'failed', {
    error_category: skip ? 'auth_failure' : brk ? 'rate_limit' : retryable ? 'transient' : (result.success ? null : 'permanent'),
    retryable: !result.success && !skip && !brk && retryable,
    count: result.success ? result.count : 0,
    latencyMs,
    error: result.success ? null : (result.error || null),
    retryAfterMs: retryable ? (retryAfterMs || null) : null,
  });

  return {
    status: result.success ? 'completed' : 'failed',
    count: result.success ? (result.count || 0) : 0,
    error: result.success ? null : (result.error || null),
    instagram_id: result.instagram_id || null,
  };
}

/**
 * Emit observability transition for attempt state changes.
 */
function _emitTransition(intentId, previousState, nextState, extraRaw = {}) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition({
      domain: 'execution',
      entity: 'attempt',
      entityId: intentId,
      previousState,
      nextState,
      authority: 'retry-worker',
      raw: extraRaw,
    });
  } catch (err) {
    console.warn('[retry-worker] Observability transition error:', err.message);
  }
}

/**
 * Emit EXECUTION_OBSERVATION upward to governance.
 * Pure observation — no policy interpretation.
 */
function _emitObservation(governance, accountId, intentId, domain, status, meta) {
  if (!governance) return;
  governance.dispatch({
    type: 'EXECUTION_OBSERVATION',
    accountId,
    intentId,
    domain,
    status,
    ...meta,
  });
}

/**
 * Emit engagement signals directly to CK.
 * DOMAIN_EVENT_MAP routes these to engagement-fsm — no acquisition-fsm involvement.
 * Pure signal emission — no policy interpretation.
 */
function _emitEngagementSignal(governance, eventType, payload) {
  if (!governance) return;
  governance.dispatch({ type: eventType, ...payload });
}

/**
 * Record a failed acquisition to telemetry + metrics substrate.
 */
async function _recordFailure(domain, accountId, intentId, errorTag, latencyMs) {
  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, errorTag);
  metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
}

/**
 * Map error classification to error tag string.
 */
function _errorTag(result, skip, brk, retryable) {
  if (result.success) return null;
  if (skip) return 'auth_failure';
  if (brk) return 'rate_limited';
  if (retryable) return 'transient';
  return result.error || 'unknown';
}

module.exports = { executeSingle };
