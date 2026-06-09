// acquisition-kernel/retry-worker.js
// Execution Bridge: bounded retry-aware acquisition loop.
// Kernelized from: control-plane/runtime/execution-bridge.js
// Refactored in Phase 6a: governance now mediates retry behaviour.
//
// Owns: executing a single bounded attempt for one acquisition intent,
//        recording telemetry/quota/metrics, emitting categorised signals.
// Does NOT own: governance policy, retry decisions, classification,
//               state mutation — those are engagement-fsm responsibilities.
//
// Architectural invariants:
//   Signals UP    → governance.dispatch() reports outcome to CK
//   Authority ↓   → governance.dispatch(CIRCUIT_BREAKER_CHECK) asks FSM for permission
//   Substrates ↓  → telemetry/quota/metrics are recorded here (mechanical)
//                   but metrics now flow through governance.recordMetric()
//                   for constitutional routing (CK → scheduling FSM → worker → substrate)
//
// The worker is semantically blind. It does not classify errors, does not
// decide retry policy, does not schedule. It executes, records, and reports.

const quota = require('../substrates/quota');
const telemetry = require('../substrates/telemetry');
const parsing = require('./parsing');

/**
 * Execute a single bounded attempt for one acquisition intent.
 *
 * Performs one fetch + parsing-dispatch cycle, records telemetry/quota/
 * metrics, then emits a categorised signal bundle upward. Governance
 * routes the signals to the correct FSM. The FSM classifies (via
 * classification-worker) and decides the next action.
 *
 * @param {string} accountId - business account UUID
 * @param {string} domain - 'comments'|'messages'|'publish:media'|etc.
 * @param {object} params - intent parameters passed to routing.fetch()
 * @param {string} intentId - acquisition intent ID (for observability)
 * @param {object} governance - governance kernel module
 * @param {{ fetch: Function }} routing - domain registry entry
 * @returns {Promise<{ status: string, count: number, error: string|null,
 *            instagram_id: string|null, latencyMs: number,
 *            transportMeta: object, errorCategory: string|null,
 *            errorCode: number|null }>}
 */
async function executeSingle(accountId, domain, params, intentId, governance, routing) {
  const startTime = Date.now();

  // ── Pre-flight: ask governance "may I attempt this?" (option A) ─────────
  // Worker is semantically blind. It does NOT read rateLimiter state
  // directly. It dispatches a query event; engagement-fsm answers.
  const gateResult = governance.dispatch({
    type: 'CIRCUIT_BREAKER_CHECK',
    accountId,
    domain,
    intentId,
  });
  const gateActive = gateResult && gateResult.actions &&
    gateResult.actions.some(a => a.type === 'CIRCUIT_BREAKER_ACTIVE');

  if (gateActive) {
    const latencyMs = Date.now() - startTime;
    console.log(`[retry-worker] ${domain}/${accountId} circuit-breaker gate active, skipping intent ${intentId}`);
    await _recordFailure(domain, accountId, intentId, 'circuit_breaker_active', 0, governance);
    _emitTransition(intentId, 'PENDING', 'SKIPPED', { accountId, domain, reason: 'gate_active' });
    // Report the gate-block to governance. engagement-fsm will see this.
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'skipped',
      error: 'circuit_breaker_active',
      params,
    });
    return {
      status: 'skipped',
      count: 0,
      error: 'circuit_breaker_active',
      instagram_id: null,
      latencyMs,
      transportMeta: null,
      errorCategory: 'circuit_breaker',
      errorCode: null,
    };
  }

  // ── Parse intent parameters (declarative) ───────────────────────────────
  const parsed = parsing.parse(intentId, params);
  const jobId = parsed ? parsed.job_id : null;

  // ── Fetch from IG transport ─────────────────────────────────────────────
  let result = null;
  try {
    result = await routing.fetch(accountId, intentId, parsed || { ...params, job_id: jobId });
  } catch (err) {
    const latencyMs = Date.now() - startTime;
    console.error(`[retry-worker] ${domain}/${accountId} fetch error:`, err.message);
    await _recordFailure(domain, accountId, intentId, 'fetch_error', latencyMs, governance);
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      error: err.message,
      errorShape: { category: 'transient', code: null, retryable: true, retryAfterSeconds: null },
      params,
    });
    return {
      status: 'failed',
      count: 0,
      error: err.message,
      instagram_id: null,
      latencyMs,
      transportMeta: null,
      errorCategory: 'fetch_error',
      errorCode: null,
    };
  }

  const latencyMs = Date.now() - startTime;

  // ── Post-process result ─────────────────────────────────────────────────
  if (result) {
    // Record post-acquisition to telemetry
    // (telemetry.recordAcquisition is called in success/failure paths below)

    // Record quota consumption per-scope
    const quotas = result.quota || {};
    for (const [scope, consumed] of Object.entries(quotas)) {
      quota.consume(domain, accountId, scope, consumed);
    }

    // Quota depleted check — emit LOG_DEGRADED only (quota substrate
    // tracks remaining; the FSM is blind but governance can interrogate
    // quota substrate on WORKER_OUTCOME_REPORTED with QUOTA_EXHAUSTED shape)
    if (result.quota_exhausted) {
      const depleted = Array.isArray(result.quota_exhausted)
        ? result.quota_exhausted : [result.quota_exhausted];
      governance.dispatch({
        type: 'WORKER_OUTCOME_REPORTED',
        accountId, domain, intentId,
        status: 'completed',
        error: `quota_exhausted: ${depleted.join(', ')}`,
        errorShape: { category: 'quota_exhausted', code: 4, retryable: false, retryAfterSeconds: null },
        params,
        result,
      });
      return {
        status: 'completed',
        count: result.count || 0,
        error: `quota_exhausted: ${depleted.join(', ')}`,
        instagram_id: result.instagram_id || null,
        latencyMs,
        transportMeta: result.meta || null,
        errorCategory: 'quota_exhausted',
        errorCode: 4,
      };
    }

    // Recording identity to acquisition map (so dedup can match IG IDs to intent IDs)
    if (result.instagram_id) {
      const { getRedisClient } = require('../config/redis');
      const redis = getRedisClient();
      if (redis && redis.status === 'ready') {
        redis.set(`acquisition:${domain}:${result.instagram_id}`, intentId, 'EX', 86400).catch(() => {});
      }
    }

    result.count = 0;
    result.instagram_id = result.instagram_id || null;
  }

  // ── Record outcome to telemetry + metrics ───────────────────────────────
  if (result.success) {
    await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
    await governance.recordMetric(domain, 'completed', latencyMs, accountId);
  } else {
    const errorTag = result.error_category || result.error || 'unknown';
    await _recordFailure(domain, accountId, intentId, errorTag, latencyMs, governance);
  }

  // Observability: attempt result transition (COMPLETED or FAILED)
  _emitTransition(intentId, 'ATTEMPTING', result.success ? 'COMPLETED' : 'FAILED', {
    accountId, domain,
  });

  // ── Emit categorised signal bundle upward to governance ─────────────────
  // The worker reports the RESPONSE-SHAPE categorisation from the IG
  // transport (rate_limit, auth_failure, transient, permanent, etc.).
  // It does NOT decide what to do. It does NOT call rate-limiter.
  // It does NOT call retry-cadence. It does NOT mutate state.
  //
  // Governance routes this observation to the correct FSM. The FSM
  // (engagement-fsm) classifies the errorShape via its paired
  // classification-worker and emits the appropriate downstream signal
  // (TRANSIENT_RETRY, AUTH_FAILURE, RATE_LIMIT, PERMANENT_FAILURE).
  governance.dispatch({
    type: 'WORKER_OUTCOME_REPORTED',
    accountId,
    domain,
    intentId,
    jobId,
    status: result.success ? 'completed' : 'failed',
    result,
    error: result.error || null,
    errorShape: result.errorShape || null,
    errorCategory: result.error_category || null,
    errorCode: result.error_code || null,
    params,
  });

  return {
    status: result.success ? 'completed' : 'failed',
    count: result.count || 0,
    error: result.error || null,
    instagram_id: result.instagram_id || null,
    latencyMs,
    transportMeta: result.meta || null,
    errorCategory: result.error_category || null,
    errorCode: result.error_code || null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Private helpers
// ═══════════════════════════════════════════════════════════════════════════════

function _emitTransition(intentId, previousState, nextState, extra = {}) {
  try {
    const observability = require('../control-plane/observability/emitters/transition-emitter');
    const extraRaw = extra || {};
    observability.transition({
      domain: 'acquisition',
      entity: 'intent',
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
 * Record a failed acquisition to telemetry + metrics substrate.
 */
async function _recordFailure(domain, accountId, intentId, errorTag, latencyMs, governance) {
  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, errorTag);
  await governance.recordMetric(domain, 'failed', latencyMs, accountId);
}

module.exports = { executeSingle };
