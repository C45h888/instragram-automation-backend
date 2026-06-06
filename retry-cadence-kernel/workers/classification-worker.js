// retry-cadence-kernel/workers/classification-worker.js
// Classification worker: semantically blind, deterministic, pure.
//
// CONSTITUTIONAL CONTRACT:
//   - Receives: errorShape = { category, code, retryable, retryAfterSeconds }
//                from a worker via governance.
//   - Returns:  classified action tag:
//                { type, [retryAfterMs], [retryAfterSeconds], [igCode] }
//   - Type values:
//       'TRANSIENT_RETRY'  — temporary failure, schedule a retry
//       'AUTH_FAILURE'     — token/permission issue, escalate via strike
//       'RATE_LIMIT'       — IG rate limit hit, engage circuit breaker
//       'PERMANENT_FAILURE' — non-retryable, mark intent as failed
//   - Does NOT decide whether to actually retry (engagement-fsm does,
//     considering circuit breaker state).
//   - Does NOT schedule.
//   - Does NOT mutate state.
//   - Does NOT emit events.
//   - Is a pure mapping function — same input → same output, no side effects.
//
// IG-CODE DELAY OVERRIDES (moved from substrates/retry.js handleFetchError):
//   The IG transport returns a response-shape error with an error code.
//   Some IG codes warrant longer cooldowns than the default 30s transient
//   delay. The classifier applies these overrides so the FSM gets back
//   a retryAfterMs that already accounts for the IG-code-specific cooldown.
//
//   IG code 4   (app-level throttle)    → 60s base
//   IG code 17  (user request limit)    → 60s
//   IG code 32  (page-level)            → 15s (faster recovery)
//   IG code 613 (rate limit exceeded)   → 60s
//   IG code 190 (OAuth)                 → no retry (auth_failure)
//   IG code 102 (session expired)       → no retry (auth_failure)
//   IG code 104 (bad token)             → no retry (auth_failure)
//
// The IG error category values come from
// substrates/transport/error-classifier.js (categorizeIgError):
//   - 'auth_failure' (IG codes 190, 102, 104)
//   - 'permanent'    (HTTP 400 with non-rate-limit IG code)
//   - 'rate_limit'   (IG codes 4, 17, 32, 613; HTTP 429)
//   - 'transient'    (HTTP 5xx; ETIMEDOUT; ECONNABORTED)
//   - 'unknown'      (default — retry once then permanent)

const ACTION_TRANSIENT_RETRY  = 'TRANSIENT_RETRY';
const ACTION_AUTH_FAILURE     = 'AUTH_FAILURE';
const ACTION_RATE_LIMIT       = 'RATE_LIMIT';
const ACTION_PERMANENT_FAILURE = 'PERMANENT_FAILURE';

// IG-code-specific retry delay overrides (in seconds).
// Source: moved from substrates/retry.js handleFetchError.
const IG_CODE_DELAY_OVERRIDES_SEC = {
  4:    60,   // App-level throttle → 60s
  17:   60,   // User request limit → 60s
  32:   15,   // Page-level → 15s (faster recovery)
  613:  60,   // Rate limit exceeded → 60s
};

const DEFAULT_RATE_LIMIT_SEC = 3600;  // 1h
const DEFAULT_TRANSIENT_SEC  = 30;    // 30s
const DEFAULT_UNKNOWN_SEC    = 60;    // 60s

/**
 * Map an IG errorShape to a classified action tag.
 * Pure function. No side effects. No state read or write.
 *
 * @param {object|null} errorShape
 *   { category: string, code: number|null, retryable: boolean|null,
 *     retryAfterSeconds: number|null }
 * @returns {{ type: string, retryAfterMs?: number,
 *             retryAfterSeconds?: number, igCode?: number,
 *             reason?: string }}
 */
function classify(errorShape) {
  // No error — should not happen on the failure path, but defensive
  if (!errorShape || !errorShape.category) {
    return { type: ACTION_PERMANENT_FAILURE, reason: 'no_error_shape' };
  }

  const { category, code, retryable, retryAfterSeconds } = errorShape;

  // AUTH_FAILURE — escalate via strike
  if (category === 'auth_failure') {
    return { type: ACTION_AUTH_FAILURE, igCode: code };
  }

  // RATE_LIMIT — engage circuit breaker.
  // Apply IG-code-specific delay override if the transport did
  // not already populate retryAfterSeconds.
  if (category === 'rate_limit') {
    const overrideSec = code != null ? IG_CODE_DELAY_OVERRIDES_SEC[code] : null;
    const finalSec = retryAfterSeconds || overrideSec || DEFAULT_RATE_LIMIT_SEC;
    return {
      type: ACTION_RATE_LIMIT,
      igCode: code,
      retryAfterSeconds: finalSec,
      retryAfterMs: finalSec * 1000,
    };
  }

  // TRANSIENT — server error or network timeout, retry with backoff
  if (category === 'transient') {
    const delaySec = retryAfterSeconds || DEFAULT_TRANSIENT_SEC;
    return {
      type: ACTION_TRANSIENT_RETRY,
      igCode: code,
      retryAfterSeconds: delaySec,
      retryAfterMs: delaySec * 1000,
    };
  }

  // PERMANENT — bad params, permission denied, action blocked
  if (category === 'permanent') {
    return { type: ACTION_PERMANENT_FAILURE, igCode: code };
  }

  // UNKNOWN — default safe path: retry once with conservative delay,
  // then permanent. The engagement-fsm consumes the action tag and
  // decides whether the retry budget allows this. The classifier
  // only reports what category the error shape is.
  if (category === 'unknown') {
    if (retryable === false) {
      return { type: ACTION_PERMANENT_FAILURE, reason: 'unknown_not_retryable' };
    }
    return {
      type: ACTION_TRANSIENT_RETRY,
      retryAfterSeconds: retryAfterSeconds || DEFAULT_UNKNOWN_SEC,
      retryAfterMs: (retryAfterSeconds || DEFAULT_UNKNOWN_SEC) * 1000,
      reason: 'unknown_retryable',
    };
  }

  // Catch-all — treat as permanent
  return { type: ACTION_PERMANENT_FAILURE, reason: 'unrecognised_category' };
}

// Export action constants so the FSM can use them without typo risk
module.exports = {
  classify,
  ACTIONS: {
    TRANSIENT_RETRY: ACTION_TRANSIENT_RETRY,
    AUTH_FAILURE: ACTION_AUTH_FAILURE,
    RATE_LIMIT: ACTION_RATE_LIMIT,
    PERMANENT_FAILURE: ACTION_PERMANENT_FAILURE,
  },
  // Exposed for FSM transparency — the FSM may want to know the
  // delay override for a specific IG code without a full classify call.
  IG_CODE_DELAY_OVERRIDES_SEC,
};
