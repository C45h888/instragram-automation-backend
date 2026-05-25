// control-plane/execution-bridge.js
// Execution Bridge: deterministic retry-coordinating execution wrapper.
//
// Owns: coordinating retry + quota + telemetry substrates during a single
//        acquisition execution. Stateless — state lives in substrates.
// Does NOT own: governance, orchestration, wake authority, Instagram API,
//               persistence, domain-specific execution logic.
//
// This is NOT the governance plane. It is a mechanical retry wrapper
// called by workers inside BRPOP loops. The real governance authority
// (FSM, wake, coherence, cooldown, signal ingestion) lives in the
// agent repo's supervisor_service.py.
//
// Used by every domain worker as the wrapper around their domain-specific
// execution pipeline.

const retry = require('../substrates/retry');
const quota = require('../substrates/quota');
const telemetry = require('../substrates/telemetry');
const metricsSubstrate = require('../substrates/metrics-substrate');

/**
 * Wraps a single acquisition execution with retry governance, quota tracking,
 * and telemetry recording.
 *
 * @param {string} accountId - business account UUID
 * @param {string} intentId - acquisition intent ID
 * @param {string} domain - 'comments'|'messages'|'media'|'insights'|'ugc'
 * @param {Function} executeFn - async (accountId, params) => { success, count, error? }
 * @param {object} [params={}] - intent parameters
 * @param {object} [options={}] - retry options
 * @param {number} [options.maxRetries=1] - total attempts before permanent DLQ failure
 * @returns {Promise<{status: 'completed'|'failed', count: number, error: string|null}>}
 */
async function executeWithRetry(accountId, intentId, domain, executeFn, params = {}, options = {}) {
  const { maxRetries = 1 } = options || {};

  // ── Pre-flight: rate limit guard ────────────────────────────────────────
  if (retry.isAccountRateLimited(accountId)) {
    console.log(`[execution-bridge] ${domain}/${accountId} rate-limited, skipping intent ${intentId}`);
    await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, 0, 'rate_limited');
    metricsSubstrate.record(domain, 'failed', 0, accountId);
    return { status: 'failed', count: 0, error: 'rate_limited', instagram_id: null };
  }

  // ── Retry loop: attemptCount starts at 1 ─────────────────────────────────
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

    // ── Track quota ─────────────────────────────────────────────────────────
    if (result._usagePct != null) {
      quota.updateQuotaUsage(accountId, result._usagePct);
    }

    // ── Error classification ────────────────────────────────────────────────
    const { skip, break: brk, retryable, retryAfterMs } = retry.handleFetchError(result, accountId);

    if (skip) {
      // Auth failure — do not retry
      await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, 'auth_failure');
      metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
      return { status: 'failed', count: 0, error: 'auth_failure', instagram_id: null };
    }

    if (brk) {
      // Rate limit — circuit breaker engaged, do not retry
      await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, 'rate_limited');
      metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
      return { status: 'failed', count: 0, error: 'rate_limited', instagram_id: null };
    }

    if (result.success) {
      // Success
      await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
      metricsSubstrate.record(domain, 'completed', latencyMs, accountId);
      return { status: 'completed', count: result.count, error: null, instagram_id: result.instagram_id || null };
    }

    if (retryable) {
      // Transient error — retry if attempts remain
      if (attemptCount < maxRetries) {
        const waitMs = retryAfterMs || 30000;
        console.log(`[execution-bridge] ${domain}/${accountId} attempt ${attemptCount} failed (retryable), waiting ${waitMs}ms before retry ${attemptCount + 1}/${maxRetries}`);
        await new Promise(resolve => setTimeout(resolve, waitMs));
        attemptCount++;
        // Loop continues
      } else {
        // Exhausted all attempts
        const totalLatencyMs = Date.now() - startTime;
        await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, totalLatencyMs, 'max_retries_exceeded');
        metricsSubstrate.record(domain, 'failed', totalLatencyMs, accountId);
        return { status: 'failed', count: 0, error: 'max_retries_exceeded', instagram_id: null };
      }
    } else {
      // Permanent failure (non-retryable, non-skip, non-break, not success)
      await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, result.error || 'unknown');
      metricsSubstrate.record(domain, 'failed', latencyMs, accountId);
      return { status: 'failed', count: 0, error: result.error || 'unknown', instagram_id: null };
    }
  }

  // Loop exhausted without success (fallback return — should not reach here normally)
  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, 0, 'max_retries_exceeded');
  metricsSubstrate.record(domain, 'failed', 0, accountId);
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

module.exports = { executeWithRetry, getMetrics };
