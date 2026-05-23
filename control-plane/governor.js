// control-plane/governor.js
// Control Plane: per-worker execution governance.
//
// Owns: coordinating retry + quota + telemetry substrates during a single
//        acquisition execution. Stateless — state lives in substrates.
// Does NOT own: Instagram API, persistence, domain-specific execution logic.
//
// Used by every domain worker as the wrapper around their domain-specific
// execution pipeline.

const retry = require('../substrates/retry');
const quota = require('../substrates/quota');
const telemetry = require('../substrates/telemetry');

/**
 * Wraps a single acquisition execution with retry governance, quota tracking,
 * and telemetry recording.
 *
 * @param {string} accountId - business account UUID
 * @param {string} intentId - acquisition intent ID
 * @param {string} domain - 'comments'|'messages'|'media'|'insights'|'ugc'
 * @param {Function} executeFn - async (accountId, params) => { success, count, error? }
 * @param {object} [params={}] - intent parameters
 * @returns {Promise<{status: 'completed'|'failed', count: number, error: string|null}>}
 */
async function executeWithRetry(accountId, intentId, domain, executeFn, params = {}) {
  // ── Pre-flight: rate limit guard ────────────────────────────────────────
  if (retry.isAccountRateLimited(accountId)) {
    console.log(`[Governor] ${domain}/${accountId} rate-limited, skipping intent ${intentId}`);
    await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, 0, 'rate_limited');
    return { status: 'failed', count: 0, error: 'rate_limited' };
  }

  // ── First attempt ───────────────────────────────────────────────────────
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

  if (skip || brk) {
    // Auth failure or rate limit — do not retry
    const error = result.error || (skip ? 'auth_failure' : 'rate_limit');
    const status = skip ? 'failed' : 'failed';
    await telemetry.recordAcquisition(domain, accountId, intentId, status, 0, latencyMs, error);
    return { status, count: 0, error };
  }

  if (retryable) {
    // ── Retry once ────────────────────────────────────────────────────────
    const waitMs = retryAfterMs || 30000;
    console.log(`[Governor] ${domain}/${accountId} transient error, retrying in ${waitMs}ms: ${result.error}`);

    // Track quota from first attempt before retry
    if (result._usagePct != null) {
      quota.updateQuotaUsage(accountId, result._usagePct);
    }

    await new Promise(resolve => setTimeout(resolve, waitMs));

    let retryResult;
    try {
      retryResult = await executeFn(accountId, params);
    } catch (err) {
      retryResult = { success: false, count: 0, error: err.message };
    }

    const totalLatencyMs = Date.now() - startTime;

    if (retryResult._usagePct != null) {
      quota.updateQuotaUsage(accountId, retryResult._usagePct);
    }

    const retryError = retry.handleFetchError(retryResult, accountId);

    if (retryResult.success && !retryError.skip && !retryError.break) {
      await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', retryResult.count, totalLatencyMs, null);
      return { status: 'completed', count: retryResult.count, error: null };
    }

    const finalError = retryResult.error || 'retry_failed';
    await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, totalLatencyMs, finalError);
    return { status: 'failed', count: 0, error: finalError };
  }

  // ── Success or permanent failure ────────────────────────────────────────
  if (result.success) {
    await telemetry.recordAcquisition(domain, accountId, intentId, 'completed', result.count, latencyMs, null);
    return { status: 'completed', count: result.count, error: null };
  }

  await telemetry.recordAcquisition(domain, accountId, intentId, 'failed', 0, latencyMs, result.error || 'unknown');
  return { status: 'failed', count: 0, error: result.error || 'unknown' };
}

module.exports = { executeWithRetry };
