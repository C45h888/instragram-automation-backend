// control-plane/orchestration/retry-worker.js
// Bounded single-attempt execution worker.
//
// Owns: one execution attempt — fetch + parsing dispatch + signal
//        emission. One bounded I/O call.
// Does NOT own: state mutation, scheduling retries, calling other
//               workers, acting on signals it emits.
//
// CONSTITUTIONAL CONTRACT (Step 3 — corrected per refinement):
//   - The worker is operationally complete. It parses the IG
//     response, identifies the response shape (success, rate_limit,
//     auth_failure, transient, permanent), and REPORTS the
//     categorised signal to governance.
//   - The worker is semantically blind in the sense that it does
//     NOT decide what to do. It does NOT schedule retries, does
//     NOT call rate-limiter, does NOT mutate engagement state,
//     does NOT call other workers.
//   - Pre-flight: asks governance "may I attempt this?" via
//     CIRCUIT_BREAKER_CHECK dispatch. engagement-fsm answers via
//     actions. The worker reads actions, never state directly.
//   - Post-attempt: emits categorised signals upward. Each signal
//     is a report of what happened. Governance routes to the
//     correct FSM. The FSM consumes the classification-worker
//     output (deterministic pure function) and decides the next
//     action. The FSM emits the next worker invocation if needed.
//   - The orchestrator receives the return value (Step 5) and CK
//     enriches the observation with executor-plane data.
//
// One attempt → one categorised signal bundle upward → done.

const quota = require('../substrates/quota');
const telemetry = require('../substrates/telemetry');
const metricsSubstrate = require('../substrates/metrics-substrate');
const parsing = require('./substrates/parsing-substrate');

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
    await _recordFailure(domain, accountId, intentId, 'circuit_breaker_active', 0);
    _emitTransition(intentId, 'PENDING', 'SKIPPED', { accountId, domain, reason: 'gate_active' });
    // Report the gate-block to governance. engagement-fsm will see this.
    governance.dispatch({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'skipped',
      reason: 'circuit_breaker_gate_active',
      latencyMs,
    });
    return {
      status: 'failed', count: 0, error: 'circuit_breaker_gate_active',
      instagram_id: null, latencyMs, transportMeta: { gate: 'closed_active' },
      errorCategory: 'rate_limit', errorCode: null,
    };
  }

  // Observability: attempt start transition
  _emitTransition(intentId, 'PENDING', 'ATTEMPTING', { accountId, domain });

  // ── Single bounded attempt ───────────────────────────────────────────────
  let result;
  try {
    result = await routing.fetch(accountId, params);
  } catch (err) {
    // Catch-block error — transport threw before buildErrorResponse ran.
    // Wrap as raw error with no categorisation. The classification-worker
    // (called by engagement-fsm) will determine the category.
    result = {
      success: false, count: 0,
      error: err.message, code: null,
      retryable: null, error_category: null, retry_after_seconds: null,
      _usagePct: null, instagram_id: null, igUserId: null, pageId: null,
    };
  }

  const latencyMs = Date.now() - startTime;

  // ── Quota tracking ───────────────────────────────────────────────────────
  if (result._usagePct != null) {
    quota.updateQuotaUsage(accountId, result._usagePct);
  }

  // ── Persist on success → dispatch to parsing substrate (async) ──────────
  if (result.success) {
    const { jobId } = parsing.dispatch(
      domain, result, accountId, intentId,
      { igUserId: result.igUserId, pageId: result.pageId, pageToken: result.pageToken }
    );

    governance.dispatch({
      type: 'PARSING_DISPATCHED',
      accountId, domain, intentId,
      jobId,
      rawCount: result.count || 0,
    });

    result.count = 0;
    result.instagram_id = result.instagram_id || null;
  }

  // ── Record outcome to telemetry + metrics ───────────────────────────────
  if (result.success) {
    await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
    metricsSubstrate.record(domain, 'completed', latencyMs, accountId);
  } else {
    const errorTag = result.error_category || result.error || 'unknown';
    await _recordFailure(domain, accountId, intentId, errorTag, latencyMs);
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
  // consults the classification-worker (deterministic pure function
  // — raw error → classified action tag) and decides the next action.
  //
  // The categories below are RESPONSE-SHAPE categories (what the IG
  // API told us), not action categories (what we should do about it).
  // The classification-worker is what bridges between the two.
  governance.dispatch({
    type: 'WORKER_OUTCOME_REPORTED',
    accountId, intentId, domain,
    status: result.success ? 'completed' : 'failed',
    // Raw outcome + response-shape categorisation from the IG transport
    result: result.success ? { count: result.count || 0 } : null,
    error: result.success ? null : (result.error || null),
    // Response-shape categorisation from the IG transport. This is
    // INFORMATION, not a DECISION. The classification-worker
    // interprets it.
    errorShape: result.success ? null : {
      category: result.error_category || null,
      code: result.code || null,
      retryable: result.retryable ?? null,
      retryAfterSeconds: result.retry_after_seconds || null,
    },
    latencyMs,
    transportMeta: {
      success: result.success,
      instagramId: result.instagram_id || null,
      igUserId: result.igUserId || null,
      pageId: result.pageId || null,
    },
  });

  return {
    status: result.success ? 'completed' : 'failed',
    count: result.success ? (result.count || 0) : 0,
    error: result.success ? null : (result.error || null),
    instagram_id: result.instagram_id || null,
    latencyMs,
    transportMeta: {
      success: result.success,
      igUserId: result.igUserId || null,
      pageId: result.pageId || null,
    },
    errorCategory: result.error_category || null,
    errorCode: result.code || null,
  };
}

/**
 * Emit observability transition for attempt state changes.
 */
function _emitTransition(intentId, previousState, nextState, extraRaw = {}) {
  try {
    const observability = require('../control-plane/observability/emitters/transition-emitter');
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
 * Record a failed acquisition to telemetry + metrics substrate.
 */
async function _recordFailure(domain, accountId, intentId, errorTag, latencyMs) {
  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, errorTag);
  metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
}

module.exports = { executeSingle };
