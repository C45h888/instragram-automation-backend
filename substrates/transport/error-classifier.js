// substrates/transport/error-classifier.js
// Cheap IG error category HINT. NOT a classifier.
//
// This module is intentionally MINIMAL. The Instagram Reliability
// Substrate (substrates/ig-reliability-substrate.js)
// is the canonical IG failure interpreter — it implements the 17-section
// spec (normalization, classification, token lifecycle, quota, rate-limit
// recovery, prioritization, media, publishing state, webhook, dependency
// health, retryability, idempotency, adaptive cadence, recommendations,
// severity, telemetry, canonical output).
//
// What this module does:
//   - Provides a CHEAP first-pass `suspectIgCategory(error)` that
//     returns a string hint based on the error shape. NO retryable
//     decision, NO error_category verdict, NO retry_after_seconds.
//   - Workers use this hint to populate the `suspectedCategory` field
//     when emitting IG_FAILURE_OBSERVED. The substrate consumes the
//     hint as a priority/severity signal — it does NOT trust it for
//     classification.
//
// What this module does NOT do:
//   - Classify errors. The substrate owns classification.
//   - Decide retryability. The substrate owns retryability analysis.
//   - Set backoff. The substrate owns adaptive cadence.
//
// The legacy `categorizeIgError(error)` shape is preserved as a
// transitional alias for callers that still consume the legacy
// {retryable, error_category, retry_after_seconds} tuple. New
// callers MUST use `suspectIgCategory(error)` and pass the hint
// to the substrate. The legacy alias is marked for removal once
// the 8 existing call sites migrate.

/**
 * Cheap IG error category HINT — a single string only.
 * Workers use this to populate suspectedCategory in IG_FAILURE_OBSERVED.
 *
 * @param {Error} error
 * @returns {string|null} — one of:
 *   'auth' — error has IG code 190/102/104/10/200 (auth failure)
 *   'rate_limit' — IG code 4/17/32/613 or HTTP 429
 *   'permission' — IG code 200/10/220 (permission failure)
 *   'media' — IG subcode 2207027/2207050 (media processing)
 *   'dependency' — HTTP 5xx (server-side, possibly platform)
 *   'network' — ETIMEDOUT/ECONNREFUSED/ENOTFOUND (transport)
 *   'timeout' — HTTP 408/504 (timeout)
 *   'unknown' — no signal
 */
function suspectIgCategory(error) {
  if (!error) return 'unknown';
  const status = error.response?.status ?? error.status ?? null;
  const code = error.response?.data?.error?.code ?? error.code ?? null;
  const subcode = error.response?.data?.error?.error_subcode ?? error.error_subcode ?? null;

  // Auth codes
  if ([190, 102, 104, 10, 200].includes(code)) return 'auth';
  // Rate-limit codes
  if ([4, 17, 32, 613].includes(code)) return 'rate_limit';
  // Permission codes
  if (code === 220) return 'permission';
  // Media subcodes
  if (['2207027', '2207050'].includes(String(subcode))) return 'media';
  // HTTP status
  if (status === 429) return 'rate_limit';
  if (status === 401) return 'auth';
  if (status === 403) return 'permission';
  if (status === 408 || status === 504) return 'timeout';
  if (status >= 500) return 'dependency';
  // Network
  if (error.code === 'ETIMEDOUT' || error.code === 'ECONNABORTED') return 'timeout';
  if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND' || error.code === 'ECONNRESET') return 'network';
  return 'unknown';
}

/**
 * LEGACY: Transitional alias. Returns the slim shape the existing
 * 8 call sites use ({retryable, error_category, retry_after_seconds}).
 * New callers MUST NOT use this — emit raw error + cheap hint and
 * route through the substrate.
 *
 * @deprecated Use suspectIgCategory(error) + substrate.analyzeFailure()
 * @param {Error} error
 * @returns {{ retryable: boolean|null, error_category: string|null, retry_after_seconds: number|null }}
 */
function categorizeIgError(error) {
  if (!error) {
    return { retryable: null, error_category: null, retry_after_seconds: null };
  }
  const status = error.response?.status ?? null;
  const code = error.response?.data?.error?.code ?? null;
  const retryAfterHeader = error.response?.headers?.['retry-after'];

  // Auth failures — must not retry
  if ([190, 102, 104, 10, 200].includes(code)) {
    return { retryable: false, error_category: 'auth_failure', retry_after_seconds: null };
  }

  // Permanent errors — HTTP 400 with non-rate-limit IG code
  if (status === 400 && code != null && ![4, 17, 32, 613].includes(code)) {
    return { retryable: false, error_category: 'permanent', retry_after_seconds: null };
  }

  // Rate limits — IG sends these as HTTP 400, not 429
  if ([4, 17, 613].includes(code)) {
    const retryAfter = retryAfterHeader ? parseInt(retryAfterHeader, 10) : 3600;
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: retryAfter };
  }
  if (code === 32) {
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: 900 };
  }
  if (status === 429) {
    const retryAfter = retryAfterHeader ? parseInt(retryAfterHeader, 10) : 3600;
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: retryAfter };
  }

  // Transient
  if (status >= 500) {
    return { retryable: true, error_category: 'transient', retry_after_seconds: 30 };
  }
  if (!status && (error.code === 'ETIMEDOUT' || error.code === 'ECONNABORTED')) {
    return { retryable: true, error_category: 'transient', retry_after_seconds: 30 };
  }

  // Unknown — default safe
  return { retryable: true, error_category: 'unknown', retry_after_seconds: 60 };
}

module.exports = {
  suspectIgCategory,
  categorizeIgError,  // legacy — see deprecation note
};
