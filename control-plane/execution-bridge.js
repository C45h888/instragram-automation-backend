// control-plane/execution-bridge.js
// Execution Bridge: legacy thin single-attempt execution wrapper.
//
// CONSTITUTIONAL CONTRACT (Step 4 of authority centralisation):
//   - This is a LEGACY path. New execution goes through
//     acquisition-kernel/retry-worker.js. This bridge is kept for
//     backward compatibility with any code that imports it.
//   - Owns: one bounded attempt — quota tracking, telemetry, metrics
//     recording, raw outcome emission upward.
//   - Does NOT classify errors.
//   - Does NOT emit engagement lifecycle signals (AUTH_SUCCESS,
//     RATE_LIMIT_DETECTED, AUTH_FAILURE_STRIKE, RETRY_EXHAUSTED).
//   - Does NOT mutate engagement state.
//   - Emits WORKER_OUTCOME_REPORTED with raw errorShape. engagement-fsm
//     classifies and decides.
//
// If a future caller wires this bridge, it will produce raw
// outcome events that flow through the new authority model. The
// legacy direct-engagement-signal emissions are removed.

const quota = require('../substrates/quota');
const telemetry = require('../substrates/telemetry');
const metricsSubstrate = require('../substrates/metrics-substrate');

let _governance = null;

function setGovernance(governance) {
  _governance = governance;
}

function _emitOutcome(accountId, intentId, domain, status, payload) {
  if (!_governance) return;
  _governance.dispatch({
    type: 'WORKER_OUTCOME_REPORTED',
    accountId, intentId, domain,
    status, ...payload,
  });
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

/**
 * Execute a single bounded attempt.
 * Emits WORKER_OUTCOME_REPORTED with raw outcome. engagement-fsm
 * classifies and decides.
 */
async function executeSingle(accountId, intentId, domain, executeFn, params = {}) {
  const startTime = Date.now();

  // Pre-flight: ask governance (option A from Step 3)
  const breakerResult = _governance ? _governance.dispatch({
    type: 'CIRCUIT_BREAKER_CHECK',
    accountId, domain, intentId,
  }) : null;
  const isActive = breakerResult && breakerResult.actions &&
    breakerResult.actions.some(a => a.type === 'CIRCUIT_BREAKER_ACTIVE');
  if (isActive) {
    _emitTransition(intentId, 'PENDING', 'SKIPPED', { accountId, domain, reason: 'gate_active' });
    _emitOutcome(accountId, intentId, domain, 'skipped', {
      reason: 'circuit_breaker_gate_active', latencyMs: 0,
    });
    return { status: 'failed', count: 0, error: 'circuit_breaker_gate_active', instagram_id: null };
  }

  _emitTransition(intentId, 'PENDING', 'STARTED', { accountId, domain });

  let result;
  try {
    result = await executeFn(accountId, params);
  } catch (err) {
    result = {
      success: false, count: 0, error: err.message, code: null,
      retryable: null, error_category: null, retry_after_seconds: null,
    };
  }

  const latencyMs = Date.now() - startTime;

  if (result._usagePct != null) {
    quota.updateQuotaUsage(accountId, result._usagePct);
  }

  if (result.success) {
    await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
    metricsSubstrate.record(domain, 'completed', latencyMs, accountId);
    _emitTransition(intentId, 'STARTED', 'COMPLETED', { accountId, domain, count: result.count });
    _emitOutcome(accountId, intentId, domain, 'completed', {
      result: { count: result.count || 0 },
      latencyMs,
      transportMeta: { success: true, instagramId: result.instagram_id || null },
    });
    return { status: 'completed', count: result.count || 0, error: null, instagram_id: result.instagram_id || null };
  }

  const errorTag = result.error_category || result.error || 'unknown';
  await _recordFailure(domain, accountId, intentId, errorTag, latencyMs);
  _emitTransition(intentId, 'STARTED', 'FAILED', { accountId, domain });

  _emitOutcome(accountId, intentId, domain, 'failed', {
    error: result.error || null,
    errorShape: {
      category: result.error_category || null,
      code: result.code || null,
      retryable: result.retryable ?? null,
      retryAfterSeconds: result.retry_after_seconds || null,
    },
    latencyMs,
    transportMeta: { success: false, instagramId: result.instagram_id || null },
  });

  return { status: 'failed', count: 0, error: result.error || null, instagram_id: null };
}

async function _recordFailure(domain, accountId, intentId, errorTag, latencyMs) {
  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, errorTag);
  metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
}

function getMetrics() {
  return metricsSubstrate.getHealthSignals();
}

module.exports = { executeSingle, getMetrics, setGovernance };
