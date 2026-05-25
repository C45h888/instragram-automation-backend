// control-plane/execution-bridge.js
// Execution Bridge: deterministic execution wrapper with mechanical retry.
//
// Owns: coordinating retry + quota + telemetry substrates during a single
//        acquisition execution. Stateless — state lives in substrates.
// Does NOT own: governance policy, retry decisions, auth escalation,
//               circuit breaker engagement.
//
// Constitutional boundary:
//   This bridge mechanically executes and emits raw observations upward.
//   Governance alone evaluates observations and decides retry policy,
//   auth escalation, circuit breakers, and permanent failure.
//   The bridge NEVER governs — it only executes + observes.
//
// Architecture invariant:
//   After each execution attempt → emit EXECUTION_OBSERVATION upward
//   Governance evaluates → decides retry/escalation/complete
//   Bridge returns final mechanical result to caller

const retry = require('../substrates/retry');
const quota = require('../substrates/quota');
const telemetry = require('../substrates/telemetry');
const metricsSubstrate = require('../substrates/metrics-substrate');

// Lazy governance reference — set by caller to avoid circular deps.
// The acquisition orchestrator wires this during boot.
let _governance = null;

/**
 * Set the governance kernel reference for observation emission.
 * Called once during boot by the orchestrator composition root.
 * @param {object} governance — governance kernel module
 */
function setGovernance(governance) {
  _governance = governance;
}

/**
 * Emit an execution observation upward to governance.
 * Pure observation — no policy interpretation by the bridge.
 */
function _emitObservation(accountId, intentId, domain, status, meta = {}) {
  if (!_governance) return;
  _governance.dispatch({
    type: 'EXECUTION_OBSERVATION',
    accountId,
    intentId,
    domain,
    status,
    ...meta,
  });
}

/**
 * Wraps a single acquisition execution with mechanical retry,
 * quota tracking, and telemetry recording.
 *
 * After each attempt, emits EXECUTION_OBSERVATION upward so governance
 * can track auth strikes, circuit breakers, and retry counts.
 * The bridge mechanically retries based on error classification,
 * but governance is informed of every outcome.
 *
 * @param {string} accountId - business account UUID
 * @param {string} intentId - acquisition intent ID
 * @param {string} domain - 'comments'|'messages'|'media'|'insights'|'ugc'
 * @param {Function} executeFn - async (accountId, params) => { success, count, error? }
 * @param {object} [params={}] - intent parameters
 * @param {object} [options={}] - retry options
 * @param {number} [options.maxRetries=1] - total attempts before permanent failure
 * @returns {Promise<{status: 'completed'|'failed', count: number, error: string|null}>}
 */
async function executeWithRetry(accountId, intentId, domain, executeFn, params = {}, options = {}) {
  const { maxRetries = 1 } = options || {};

  // ── Pre-flight: rate limit guard (mechanical check only) ────────────────
  if (retry.isAccountRateLimited(accountId)) {
    console.log(`[execution-bridge] ${domain}/${accountId} rate-limited, skipping intent ${intentId}`);
    await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, 0, 'rate_limited');
    metricsSubstrate.record(domain, 'failed', 0, accountId);
    _emitObservation(accountId, intentId, domain, 'failed', {
      error_category: 'rate_limit', retryable: false, count: 0, latencyMs: 0,
    });
    return { status: 'failed', count: 0, error: 'rate_limited', instagram_id: null };
  }

  // ── Retry loop: attemptCount starts at 1 ────────────────────────────────
  let attemptCount = 1;

  while (attemptCount <= maxRetries) {
    const startTime = Date.now();
    let result;
    try {
      result = await executeFn(accountId, params);
    } catch (err) {
      result = { success: false, count: 0, error: err.message };
    }

    const latencyMs = Date.now() - startTime;

    // ── Track quota ───────────────────────────────────────────────────────
    if (result._usagePct != null) {
      quota.updateQuotaUsage(accountId, result._usagePct);
    }

    // ── Error classification (mechanical — retry substrate only classifies) ─
    const { skip, break: brk, retryable, retryAfterMs } = retry.handleFetchError(result, accountId);

    if (skip) {
      // Auth failure — emit observation upward, do not retry
      await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, 'auth_failure');
      metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
      _emitObservation(accountId, intentId, domain, 'failed', {
        error_category: 'auth_failure', retryable: false, count: 0, latencyMs, error: result.error,
      });
      return { status: 'failed', count: 0, error: 'auth_failure', instagram_id: null };
    }

    if (brk) {
      // Rate limit — emit observation upward, do not retry
      await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, 'rate_limited');
      metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
      _emitObservation(accountId, intentId, domain, 'failed', {
        error_category: 'rate_limit', retryable: false, count: 0, latencyMs, error: result.error,
      });
      return { status: 'failed', count: 0, error: 'rate_limited', instagram_id: null };
    }

    if (result.success) {
      // Success — emit observation upward with completed status
      await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
      metricsSubstrate.record(domain, 'completed', latencyMs, accountId);
      _emitObservation(accountId, intentId, domain, 'completed', {
        error_category: null, retryable: false, count: result.count, latencyMs,
      });
      return { status: 'completed', count: result.count, error: null, instagram_id: result.instagram_id || null };
    }

    if (retryable) {
      // Transient error — emit observation upward, retry if attempts remain
      _emitObservation(accountId, intentId, domain, 'failed', {
        error_category: 'transient', retryable: true, count: 0, latencyMs,
        error: result.error, retryAfterMs: retryAfterMs || null,
        retryCount: attemptCount,
      });

      if (attemptCount < maxRetries) {
        const waitMs = retryAfterMs || 30000;
        console.log(`[execution-bridge] ${domain}/${accountId} attempt ${attemptCount} failed (retryable), waiting ${waitMs}ms before retry ${attemptCount + 1}/${maxRetries}`);
        await new Promise(resolve => setTimeout(resolve, waitMs));
        attemptCount++;
      } else {
        // Exhausted all attempts
        const totalLatencyMs = Date.now() - startTime;
        await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, totalLatencyMs, 'max_retries_exceeded');
        metricsSubstrate.record(domain, 'failed', totalLatencyMs, accountId);
        _emitObservation(accountId, intentId, domain, 'failed', {
          error_category: 'exhausted', retryable: false, count: 0, latencyMs: totalLatencyMs,
          error: 'max_retries_exceeded', retryCount: attemptCount,
        });
        return { status: 'failed', count: 0, error: 'max_retries_exceeded', instagram_id: null };
      }
    } else {
      // Permanent failure (non-retryable, non-skip, non-break, not success)
      await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, result.error || 'unknown');
      metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
      _emitObservation(accountId, intentId, domain, 'failed', {
        error_category: 'permanent', retryable: false, count: 0, latencyMs, error: result.error,
      });
      return { status: 'failed', count: 0, error: result.error || 'unknown', instagram_id: null };
    }
  }

  // Loop exhausted without success (fallback)
  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, 0, 'max_retries_exceeded');
  metricsSubstrate.record(domain, 'failed', 0, accountId);
  _emitObservation(accountId, intentId, domain, 'failed', {
    error_category: 'exhausted', retryable: false, count: 0, latencyMs: 0, error: 'max_retries_exceeded',
  });
  return { status: 'failed', count: 0, error: 'max_retries_exceeded', instagram_id: null };
}

/**
 * Returns aggregate execution metrics for the rolling window.
 * Delegates to metrics-substrate for raw telemetry — no policy embedded here.
 * @returns {{ total: number, completed: number, failed: number, failureRate: number, windowMs: number }}
 */
function getMetrics() {
  return metricsSubstrate.getHealthSignals();
}

module.exports = { executeWithRetry, getMetrics, setGovernance };
