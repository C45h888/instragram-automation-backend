// substrates/transport/error-classifier.js
// IG API error classifier — pure function, no side effects.
//
// Owns: mapping raw axios/IG errors → structured {retryable, error_category, retry_after_seconds}.
// Does NOT own: retry decisions (retry-cadence-kernel), DB, orchestration.
//
// Extracted from helpers/agent-helpers.js (decomposed).

/**
 * Maps an Instagram Graph API error to structured retry metadata.
 * Called by every catch block that handles a Graph API axios failure.
 *
 * IG rate limits come as HTTP 400 with specific error codes (4, 17, 32, 613).
 * They do NOT reliably use HTTP 429, so we must inspect the IG error code directly.
 *
 * @param {Error} error - axios error from a Graph API call
 * @returns {{ retryable: boolean, error_category: string, retry_after_seconds: number|null }}
 */
function categorizeIgError(error) {
  const status = error.response?.status;
  const igCode = error.response?.data?.error?.code;
  const retryAfterHeader = error.response?.headers?.['retry-after'];

  // Auth failures — token expired/invalid, requires user re-auth, must not retry
  if ([190, 102, 104].includes(igCode)) {
    return { retryable: false, error_category: 'auth_failure', retry_after_seconds: null };
  }

  // Permanent errors — bad params, permission denied, action blocked
  // Checked BEFORE rate-limit block because they're also HTTP 400
  if (status === 400 && igCode && ![4, 17, 32, 613].includes(igCode)) {
    return { retryable: false, error_category: 'permanent', retry_after_seconds: null };
  }

  // Rate limits — IG sends these as HTTP 400, not 429, with specific codes
  if ([4, 17, 613].includes(igCode)) {
    const retryAfter = retryAfterHeader ? parseInt(retryAfterHeader, 10) : 3600;
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: retryAfter };
  }
  if (igCode === 32) {
    // Page-level throttling — shorter cooldown than app-level
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: 900 };
  }
  // Some IG endpoints do return HTTP 429 with Retry-After
  if (status === 429) {
    const retryAfter = retryAfterHeader ? parseInt(retryAfterHeader, 10) : 3600;
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: retryAfter };
  }

  // Transient — IG server errors
  if (status >= 500) {
    return { retryable: true, error_category: 'transient', retry_after_seconds: 30 };
  }

  // Network timeout — axios ETIMEDOUT / ECONNABORTED (no response object)
  if (!status && (error.code === 'ETIMEDOUT' || error.code === 'ECONNABORTED')) {
    return { retryable: true, error_category: 'transient', retry_after_seconds: 30 };
  }

  // Unknown — default safe to retry
  return { retryable: true, error_category: 'unknown', retry_after_seconds: 60 };
}

module.exports = { categorizeIgError };
