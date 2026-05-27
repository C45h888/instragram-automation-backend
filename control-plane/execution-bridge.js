// control-plane/execution-bridge.js
// Execution Bridge: thin single-attempt execution wrapper.
//
// Owns: one bounded attempt — quota tracking, telemetry, metrics recording,
//        observation emission upward, retry count tracking per intentId.
// Does NOT own: retry policy (governed by engagement-fsm), auth/cb state.
//
// Constitutional purity:
//   Engagement signals (AUTH_SUCCESS, AUTH_FAILURE_STRIKE, RATE_LIMIT_DETECTED,
//   RETRY_COUNT_INCREMENTED, RETRY_EXHAUSTED) are emitted by this bridge directly
//   to CK. DOMAIN_EVENT_MAP routes them to engagement-fsm — no acquisition-fsm
//   involvement. Engagement-fsm is the SOLE authority on engagement state.
//
// Note:
//   The retry loop has been extracted into retry-worker.js (orchestration layer).
//   This bridge is now a thin passthrough that composes a single attempt with
//   the bounded substrates (retry classification, quota, telemetry, metrics).
//   For a full retry-aware execution path, use retry-worker.js instead
//   (wired via acquisition orchestrator's RETRY_ACQUISITION routing).

const retry = require('../substrates/retry');
const quota = require('../substrates/quota');
const telemetry = require('../substrates/telemetry');
const metricsSubstrate = require('../substrates/metrics-substrate');

// ── Retry count tracking (per intentId, for engagement signal emission) ──────
const _retryCounts = new Map(); // intentId → retry count

const MAX_ACQUISITION_RETRIES = 1;

// Lazy governance reference — set by caller to avoid circular deps.
let _governance = null;

function setGovernance(governance) {
  _governance = governance;
}

function _emitObservation(accountId, intentId, domain, status, meta = {}) {
  if (!_governance) return;
  _governance.dispatch({ type: 'EXECUTION_OBSERVATION', accountId, intentId, domain, status, ...meta });
}

/**
 * Emit engagement signals directly to CK.
 * DOMAIN_EVENT_MAP routes these to engagement-fsm — no acquisition-fsm involvement.
 * Pure signal emission — no policy interpretation.
 */
function _emitEngagementSignal(eventType, payload) {
  if (!_governance) return;
  _governance.dispatch({ type: eventType, ...payload });
}

/**
 * Execute a single bounded attempt.
 * Emits EXECUTION_OBSERVATION upward after the attempt.
 *
 * @param {string} accountId
 * @param {string} intentId
 * @param {string} domain
 * @param {Function} executeFn — async (accountId, params) => { success, count, error?, _usagePct?, instagram_id? }
 * @param {object} [params={}]
 * @returns {Promise<{status: 'completed'|'failed', count: number, error: string|null, instagram_id: string|null}>}
 */
async function executeSingle(accountId, intentId, domain, executeFn, params = {}) {
  const startTime = Date.now();

  // ── Pre-flight: circuit breaker check — routed through CK to engagement FSM ─
  // Constitutional hierarchy: the engagement FSM is the SOLE authority on circuit breaker state.
  // Execution layers must dispatch through CK, not query state directly.
  const breakerResult = _governance ? _governance.dispatch({
    type: 'CIRCUIT_BREAKER_CHECK',
    accountId,
    domain,
    intentId,
  }) : null;
  const isActive = breakerResult && breakerResult.actions && breakerResult.actions.some(
    a => a.type === 'CIRCUIT_BREAKER_ACTIVE'
  );
  if (isActive) {
    console.log(`[execution-bridge] ${domain}/${accountId} circuit-breaker active, skipping intent ${intentId}`);
    await _recordFailure(domain, accountId, intentId, 'rate_limited', 0);
    _emitObservation(accountId, intentId, domain, 'failed', { error_category: 'rate_limit', retryable: false, count: 0, latencyMs: 0, error: 'circuit_breaker_active' });
    _emitTransition(intentId, 'PENDING', 'SKIPPED', { accountId, domain, reason: 'circuit_breaker' });
    return { status: 'failed', count: 0, error: 'circuit_breaker_active', instagram_id: null };
  }

  // Observability: attempt start
  _emitTransition(intentId, 'PENDING', 'STARTED', { accountId, domain });

  let result;
  try {
    result = await executeFn(accountId, params);
  } catch (err) {
    result = { success: false, count: 0, error: err.message };
  }

  const latencyMs = Date.now() - startTime;

  if (result._usagePct != null) {
    quota.updateQuotaUsage(accountId, result._usagePct);
  }

  const { skip, break: brk, retryable, retryAfterMs } = retry.handleFetchError(result, accountId);

  // ── Emit engagement signals directly to CK ─────────────────────────────
  // Constitutional hierarchy: engagement-fsm is the SOLE authority on engagement state.
  // execution-bridge emits signals directly to CK; DOMAIN_EVENT_MAP routes them to
  // engagement-fsm. No acquisition-fsm involvement in engagement signal origination.
  if (result.success) {
    _emitEngagementSignal('AUTH_SUCCESS', { accountId, intentId });
  } else if (brk) {
    // Rate limit → engagement-fsm manages circuit breaker via CK routing
    _emitEngagementSignal('RATE_LIMIT_DETECTED', {
      accountId, cooldownMs: (retryAfterMs || 3600000),
    });
  } else if (skip) {
    // Auth failure → engagement-fsm manages auth strikes via CK routing
    _emitEngagementSignal('AUTH_FAILURE_STRIKE', {
      accountId, error: result.error,
    });
  } else if (retryable) {
    const retryCount = (_retryCounts.get(intentId) || 0) + 1;
    _retryCounts.set(intentId, retryCount);
    if (retryCount > MAX_ACQUISITION_RETRIES) {
      // Retries exhausted → engagement-fsm receives RETRY_EXHAUSTED via CK routing
      _emitEngagementSignal('RETRY_EXHAUSTED', {
        accountId, domain, intentId, error: 'max_retries_exceeded',
      });
      _retryCounts.delete(intentId);
    } else {
      _emitEngagementSignal('RETRY_COUNT_INCREMENTED', { intentId, retryCount });
    }
  }

  if (result.success) {
    await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
    metricsSubstrate.record(domain, 'completed', latencyMs, accountId);
    // Observability: attempt completed
    _emitTransition(intentId, 'STARTED', 'COMPLETED', { accountId, domain, count: result.count });
    _emitObservation(accountId, intentId, domain, 'completed', { error_category: null, retryable: false, count: result.count, latencyMs });
    return { status: 'completed', count: result.count || 0, error: null, instagram_id: result.instagram_id || null };
  }

  // Observability: attempt failed
  _emitTransition(intentId, 'STARTED', 'FAILED', {
    accountId, domain,
    error_category: skip ? 'auth_failure' : brk ? 'rate_limit' : retryable ? 'transient' : 'permanent',
  });

  await _recordFailure(domain, accountId, intentId, _errorTag(result, skip, brk, retryable), latencyMs);
  _emitObservation(accountId, intentId, domain, 'failed', {
    error_category: skip ? 'auth_failure' : brk ? 'rate_limit' : retryable ? 'transient' : 'permanent',
    retryable: !skip && !brk && retryable,
    count: 0,
    latencyMs,
    error: result.error || null,
    retryAfterMs: retryable ? (retryAfterMs || null) : null,
  });

  return { status: 'failed', count: 0, error: result.error || null, instagram_id: null };
}

async function _recordFailure(domain, accountId, intentId, errorTag, latencyMs) {
  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, errorTag);
  metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
}

function _errorTag(result, skip, brk, retryable) {
  if (result.success) return null;
  if (skip) return 'auth_failure';
  if (brk) return 'rate_limited';
  if (retryable) return 'transient';
  return result.error || 'unknown';
}

function _emitTransition(intentId, previousState, nextState, extraRaw = {}) {
  try {
    const observability = require('./observability/emitters/transition-emitter');
    observability.transition({
      domain: 'execution',
      entity: 'attempt',
      entityId: intentId,
      previousState,
      nextState,
      authority: 'execution-bridge',
      raw: extraRaw,
    });
  } catch (err) {
    console.warn('[execution-bridge] Observability transition error:', err.message);
  }
}

function getMetrics() {
  return metricsSubstrate.getHealthSignals();
}

// Backward compat alias
const executeWithRetry = executeSingle;

module.exports = { executeSingle, executeWithRetry, getMetrics, setGovernance };
