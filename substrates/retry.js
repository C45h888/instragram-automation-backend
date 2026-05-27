// substrates/retry.js
// Bounded substrate: mechanical retry classification.
//
// Owns: circuit breaker state, error classification, backoff computation.
// Does NOT own: governance policy, auth escalation, system alerts,
//               account disconnection, strike tracking.
//
// Constitutional boundary:
//   This substrate mechanically classifies errors and maintains circuit
//   breaker state. It NEVER governs — no account disconnection, no system
//   alerts, no auth strike escalation. All governance decisions belong
//   to the HSM governance kernel.
//
// Architecture invariant:
//   Mechanical classification only → execution bridge consumes classification
//   Execution bridge emits EXECUTION_OBSERVATION upward → governance decides
//   Governance emits DISCONNECT_ACCOUNT, ENGAGE_CIRCUIT_BREAKER downward
//   Credential cache clearing flows through engagement-telemetry-interpreter → governance

// ── In-memory circuit breaker state (mechanical only) ───────────────────────

const _rateLimitedAccounts = new Map(); // accountId → unblocked_at ms

/**
 * Returns the current circuit breaker state for an account.
 * Used by observability plane to get previous state before rate limiting.
 *
 * @param {string} accountId
 * @returns {string} 'IDLE' | 'RATE_LIMITED'
 */
function getCircuitBreakerState(accountId) {
  const unblocked = _rateLimitedAccounts.get(accountId);
  if (!unblocked) return 'IDLE';
  if (Date.now() >= unblocked) return 'IDLE';
  return 'RATE_LIMITED';
}

// ── Rate limit guard ─────────────────────────────────────────────────────────

function isAccountRateLimited(accountId) {
  const unblocked = _rateLimitedAccounts.get(accountId);
  if (!unblocked) return false;
  if (Date.now() >= unblocked) {
    return false;
  }
  return true;
}

/**
 * Check and auto-clear expired circuit breaker. Returns previous state before clear.
 * Called by observability plane or retry logic when checking if a breaker is active.
 *
 * @param {string} accountId
 * @returns {{ cleared: boolean, previousState: string }}
 */
function checkAndClearIfExpired(accountId) {
  const previousState = getCircuitBreakerState(accountId);
  const unblocked = _rateLimitedAccounts.get(accountId);
  if (unblocked && Date.now() >= unblocked) {
    _rateLimitedAccounts.delete(accountId);
    if (previousState === 'RATE_LIMITED') {
      try {
        const observability = require('../control-plane/observability/emitters/transition-emitter');
        observability.transition({
          domain: 'quota',
          entity: 'circuit_breaker',
          entityId: accountId,
          previousState: 'RATE_LIMITED',
          nextState: 'IDLE',
          authority: 'retry-substrate',
          raw: { autoCleared: true },
        });
      } catch (err) {
        console.warn('[retry] Observability transition error:', err.message);
      }
    }
    return { cleared: true, previousState };
  }
  return { cleared: false, previousState };
}

/**
 * Mark an account as rate-limited. Pure mechanical state — no governance.
 * Governance is informed via EXECUTION_OBSERVATION from execution-bridge.
 */
function markAccountRateLimited(accountId, retryAfterSeconds) {
  const previousState = getCircuitBreakerState(accountId);
  const cooldown = (retryAfterSeconds || 3600) * 1000;
  _rateLimitedAccounts.set(accountId, Date.now() + cooldown);
  console.warn(`[retry] Account ${accountId} rate-limited for ${retryAfterSeconds || 3600}s (mechanical state only)`);

  // Observability transition — circuit breaker state change
  try {
    const observability = require('../control-plane/observability/emitters/transition-emitter');
    observability.transition({
      domain: 'quota',
      entity: 'circuit_breaker',
      entityId: accountId,
      previousState,
      nextState: 'RATE_LIMITED',
      authority: 'retry-substrate',
      raw: { retryAfterSeconds, cooldownMs: cooldown },
    });
  } catch (err) {
    // Never let observability failures affect retry logic
    console.warn('[retry] Observability transition error:', err.message);
  }
}

// ── Fetch error classifier ───────────────────────────────────────────────────

/**
 * Classifies a fetch result and returns flow-control signals for the caller.
 * Pure mechanical classification — no governance decisions.
 *
 * Governance owns auth strike tracking, escalation, and disconnect decisions.
 * This function only classifies: auth_failure → skip, rate_limit → break,
 * transient → retryable, permanent → non-retryable.
 *
 * @param {object} result - Fetch result from transport substrate
 * @param {string} accountId
 * @returns {{ skip: boolean, break: boolean, retryable: boolean, retryAfterMs: number|null }}
 *   skip      → caller should skip this account (auth failure)
 *   break     → caller should stop (rate limit — circuit breaker engaged)
 *   retryable → transient error that can be retried
 *   retryAfterMs → server-suggested wait before retry (null = use default)
 */
function handleFetchError(result, accountId) {
  if (!result || result.success) {
    return { skip: false, break: false, retryable: false, retryAfterMs: null };
  }

  if (result.error_category === 'auth_failure') {
    return { skip: true, break: false, retryable: false, retryAfterMs: null };
  }

  if (result.error_category === 'rate_limit') {
    markAccountRateLimited(accountId, result.retry_after_seconds);
    // clearCredentialCache is no longer called here — the engagement-telemetry-interpreter
    // detects rate limit pressure via retry._rateLimitedAccounts polling and dispatches
    // CLEAR_CREDENTIAL_CACHE upward through governance. This ensures credential cache
    // invalidation is a governance decision, not a direct substrate mutation.
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

let _clearAccountsCacheFn = null;
function _setClearAccountsCache(fn) { _clearAccountsCacheFn = fn; }
function clearAccountsCacheAndQuota() {
  if (_clearAccountsCacheFn) _clearAccountsCacheFn();
}

module.exports = {
  _rateLimitedAccounts,
  isAccountRateLimited,
  markAccountRateLimited,
  checkAndClearIfExpired,
  getCircuitBreakerState,
  handleFetchError,
  backoffMs,
  _setClearAccountsCache,
};
