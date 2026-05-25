// control-plane/execution-bridge.js
// Execution Bridge: thin single-attempt execution wrapper.
//
// Owns: one bounded attempt — quota tracking, telemetry, metrics recording,
//        observation emission upward.
// Does NOT own: retry policy, retry counting, retry scheduling, governance.
//
// Constitutional boundary:
//   This bridge performs ONE attempt and emits ONE EXECUTION_OBSERVATION upward.
//   Governance evaluates the observation and decides retry/escalation/complete.
//   Retry authority lives entirely in HSM governance.
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

  if (_governance && _governance.isCircuitBreakerActive && _governance.isCircuitBreakerActive(accountId)) {
    console.log(`[execution-bridge] ${domain}/${accountId} circuit-breaker active, skipping intent ${intentId}`);
    await _recordFailure(domain, accountId, intentId, 'rate_limited', 0);
    _emitObservation(accountId, intentId, domain, 'failed', { error_category: 'rate_limit', retryable: false, count: 0, latencyMs: 0, error: 'circuit_breaker_active' });
    return { status: 'failed', count: 0, error: 'circuit_breaker_active', instagram_id: null };
  }

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

  if (result.success) {
    await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
    metricsSubstrate.record(domain, 'completed', latencyMs, accountId);
    _emitObservation(accountId, intentId, domain, 'completed', { error_category: null, retryable: false, count: result.count, latencyMs });
    return { status: 'completed', count: result.count || 0, error: null, instagram_id: result.instagram_id || null };
  }

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

function getMetrics() {
  return metricsSubstrate.getHealthSignals();
}

// Backward compat alias
const executeWithRetry = executeSingle;

module.exports = { executeSingle, executeWithRetry, getMetrics, setGovernance };
