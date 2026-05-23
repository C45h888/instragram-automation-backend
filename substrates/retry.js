// substrates/retry.js
// Bounded substrate: retry governance.
//
// Owns: circuit breaker state, error classification, backoff computation.
// Does NOT own: Instagram API knowledge, orchestration, persistence.
//
// Shared state (_rateLimitedAccounts, _authFailureStrikes) is module-level
// singleton — all domain workers and post-fallback.js share the same circuit.
// An IG-level rate limit on one domain blocks all domains for that account
// (correct behavior — Meta rate limits are app-level, not endpoint-level).

const { getSupabaseAdmin, logAudit } = require('../config/supabase');
const { clearCredentialCache, logDataBusEvent } = require('../helpers/agent-helpers');

// ── In-memory circuit breaker state ─────────────────────────────────────────

const _rateLimitedAccounts = new Map(); // accountId → unblocked_at ms
const _authFailureStrikes  = new Map(); // accountId → strike count
const AUTH_FAILURE_MAX_STRIKES = 3;

// ── Rate limit guard ─────────────────────────────────────────────────────────

function isAccountRateLimited(accountId) {
  const unblocked = _rateLimitedAccounts.get(accountId);
  if (!unblocked) return false;
  if (Date.now() >= unblocked) {
    _rateLimitedAccounts.delete(accountId);
    return false;
  }
  return true;
}

function markAccountRateLimited(accountId, retryAfterSeconds) {
  const cooldown = (retryAfterSeconds || 3600) * 1000;
  _rateLimitedAccounts.set(accountId, Date.now() + cooldown);
  console.warn(`[retry] Account ${accountId} rate-limited for ${retryAfterSeconds || 3600}s`);
  logAudit({
    event_type: 'rate_limit_triggered',
    action: 'circuit_breaker',
    resource_type: 'instagram_business_account',
    resource_id: null,
    details: { account_id: accountId, retry_after_seconds: retryAfterSeconds || 3600, source: 'acquisition_worker' },
    success: false,
  }).catch(() => {});
}

// ── Auth failure escalation ──────────────────────────────────────────────────

/**
 * Marks an account disconnected after auth failure strikes exceed threshold.
 * Async fire-and-forget — callers should not await.
 */
async function markAccountDisconnectedOnAuthFailure(accountId, errorMessage) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;
  try {
    await supabase
      .from('instagram_business_accounts')
      .update({ is_connected: false, connection_status: 'disconnected' })
      .eq('id', accountId);

    // Dedup: only insert if no unresolved auth_failure alert already exists
    const { data: existingAlert } = await supabase
      .from('system_alerts')
      .select('id')
      .eq('business_account_id', accountId)
      .eq('alert_type', 'auth_failure')
      .eq('resolved', false)
      .maybeSingle();

    if (!existingAlert) {
      await supabase
        .from('system_alerts')
        .insert({
          alert_type: 'auth_failure',
          business_account_id: accountId,
          message: `Acquisition auth failure: ${errorMessage}`,
          details: { source: 'acquisition_worker', error: errorMessage, occurred_at: new Date().toISOString() },
          resolved: false,
        });
    }

    clearCredentialCache(accountId);
  } catch (err) {
    console.warn(`[retry] Failed to mark account ${accountId} disconnected:`, err.message);
  }
}

// ── Fetch error classifier ───────────────────────────────────────────────────

/**
 * Classifies a fetch result and returns flow-control signals for the caller.
 *
 * @param {object} result - Fetch result from transport substrate
 * @param {string} accountId
 * @returns {{ skip: boolean, break: boolean, retryable: boolean, retryAfterMs: number|null }}
 *   skip      → caller should skip this account (auth failure)
 *   break     → caller should stop (rate limit — circuit breaker engaged)
 *   retryable → transient error that can be retried once
 *   retryAfterMs → server-suggested wait before retry (null = use default)
 */
function handleFetchError(result, accountId) {
  if (!result || result.success) {
    _authFailureStrikes.delete(accountId);
    return { skip: false, break: false, retryable: false, retryAfterMs: null };
  }

  if (result.error_category === 'auth_failure') {
    const strikes = (_authFailureStrikes.get(accountId) || 0) + 1;
    _authFailureStrikes.set(accountId, strikes);
    console.warn(`[retry] Account ${accountId} auth_failure strike ${strikes}/${AUTH_FAILURE_MAX_STRIKES}`);

    logAudit({
      event_type: 'auth_failure_strike',
      action: 'circuit_breaker',
      resource_type: 'instagram_business_account',
      resource_id: null,
      details: { account_id: accountId, strike: strikes, max: AUTH_FAILURE_MAX_STRIKES },
      success: false,
    }).catch(() => {});

    if (strikes >= AUTH_FAILURE_MAX_STRIKES) {
      _authFailureStrikes.delete(accountId);
      markAccountDisconnectedOnAuthFailure(accountId, result.error || 'auth_failure').catch(() => {});
      logDataBusEvent('sync', 'token_expired_mid_run', {
        account_id: accountId,
        error_code: result.code || null,
        success: false,
      }).catch(() => {});
    }
    return { skip: true, break: false, retryable: false, retryAfterMs: null };
  }

  if (result.error_category === 'rate_limit') {
    markAccountRateLimited(accountId, result.retry_after_seconds);
    return { skip: false, break: true, retryable: false, retryAfterMs: null };
  }

  // Transient error (5xx, timeout) — retryable with server-suggested or default delay
  if (result.error_category === 'transient') {
    const retryAfterSec = result.retry_after_seconds || 30;
    const cappedSec = Math.min(retryAfterSec, 300); // cap at 5 min
    return { skip: false, break: false, retryable: true, retryAfterMs: cappedSec * 1000 };
  }

  return { skip: false, break: false, retryable: false, retryAfterMs: null };
}

// ── Exponential backoff ──────────────────────────────────────────────────────

/**
 * Exponential backoff in ms: 2^n minutes, capped at 60 minutes.
 * retry_count=1 → 2 min, =2 → 4 min, =3 → 8 min, =4 → 16 min, =5 → 32 min
 */
function backoffMs(retryCount) {
  return Math.min(Math.pow(2, retryCount) * 60 * 1000, 60 * 60 * 1000);
}

// ── Cache invalidation ───────────────────────────────────────────────────────

/**
 * Clears the shared accounts cache (used by persistence substrate).
 * Called when an account is disconnected so it's excluded from the next
 * account refresh immediately.
 */
let _clearAccountsCacheFn = null;
function _setClearAccountsCache(fn) { _clearAccountsCacheFn = fn; }
function clearAccountsCacheAndQuota() {
  if (_clearAccountsCacheFn) _clearAccountsCacheFn();
}

module.exports = {
  _rateLimitedAccounts,
  _authFailureStrikes,
  AUTH_FAILURE_MAX_STRIKES,
  isAccountRateLimited,
  markAccountRateLimited,
  handleFetchError,
  markAccountDisconnectedOnAuthFailure,
  backoffMs,
  _setClearAccountsCache,
};
