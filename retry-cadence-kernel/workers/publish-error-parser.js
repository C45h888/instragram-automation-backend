// retry-cadence-kernel/workers/publish-error-parser.js
// Publish error parser: normalises publish substrate output into the
// shared errorShape contract used by classification-worker.
//
// CONSTITUTIONAL CONTRACT (Step 6 of authority centralisation):
//   - This is a PURE FUNCTION, not a worker, not in the registry.
//   - Receives: substrate result (thrown or returned) + domain
//   - Returns: errorShape = { category, code, retryable,
//     retryAfterSeconds, retryAfterMs }
//
// Used by:
//   - publish-content-retry-worker
//   - publish-engagement-retry-worker
//   - publishing-kernel/orchestrator (for the initial attempt's failure)
//
// The parser is a thin normaliser. It does NOT classify — the
// classification-worker does that. The parser's only job is to
// bridge the substrate's error format (whatever it is) to the
// shared errorShape contract.

// ── IG error code table (publish endpoints) ──────────────────────
// Source of truth for IG code semantics on publish operations.
// Same codes as the existing classification-worker (which handles
// read-side endpoints). The publish side uses the same IG API.
const IG_CODE_TABLE = {
  // Rate limits
  4:   { category: 'rate_limit', retryable: true,  retryAfterSeconds: 60 },
  613: { category: 'rate_limit', retryable: true,  retryAfterSeconds: 60 },
  17:  { category: 'rate_limit', retryable: true,  retryAfterSeconds: 30 },
  9:   { category: 'rate_limit', retryable: true,  retryAfterSeconds: 30 },

  // Auth failures
  190: { category: 'auth_failure', retryable: false, retryAfterSeconds: null },
  102: { category: 'auth_failure', retryable: false, retryAfterSeconds: null },
  10:  { category: 'auth_failure', retryable: false, retryAfterSeconds: null },

  // Transient (5xx, network, IG internal)
  1:   { category: 'transient',   retryable: true,  retryAfterSeconds: 30 },
  2:   { category: 'transient',   retryable: true,  retryAfterSeconds: 30 },
  32:  { category: 'transient',   retryable: true,  retryAfterSeconds: 15 },
  500: { category: 'transient',   retryable: true,  retryAfterSeconds: 30 },
  502: { category: 'transient',   retryable: true,  retryAfterSeconds: 30 },
  503: { category: 'transient',   retryable: true,  retryAfterSeconds: 30 },
  504: { category: 'transient',   retryable: true,  retryAfterSeconds: 30 },

  // Permanent
  100: { category: 'permanent',   retryable: false, retryAfterSeconds: null },
  220: { category: 'permanent',   retryable: false, retryAfterSeconds: null },
};

// ── Heuristic category detection (when no IG code) ──────────────
const TRANSIENT_MESSAGE_PATTERNS = [
  /timeout/i, /econnreset/i, /etimedout/i, /enotfound/i,
  /socket hang up/i, /connection reset/i, /network/i,
  /try again/i, /temporarily/i,
];

const RATE_LIMIT_MESSAGE_PATTERNS = [
  /rate limit/i, /too many requests/i, /429/,
];

const AUTH_FAILURE_MESSAGE_PATTERNS = [
  /unauthorized/i, /invalid token/i, /session expired/i,
  /auth.*fail/i, /401/,
];

/**
 * Parse a substrate RETURN value (not thrown) into an errorShape.
 * The publish substrate's execute() may return { success, error, code,
 * error_category, retry_after_seconds } on failure rather than
 * throwing.
 *
 * @param {object} result — substrate's return value
 * @param {string} domain — publish:post | publish:story |
 *   publish:comment | publish:message
 * @returns {object} errorShape
 */
function parse(result, domain) {
  if (!result) {
    return _unknownErrorShape('substrate_returned_null', domain);
  }
  if (result.success) {
    // Not an error. Caller should check success first.
    return null;
  }

  // Substrate provided structured error fields
  if (result.code != null || result.error_category) {
    return _fromStructuredFields(result, domain);
  }

  // Substrate provided only an error string
  if (result.error) {
    return _fromErrorString(result.error, domain);
  }

  return _unknownErrorShape('substrate_returned_empty_failure', domain);
}

/**
 * Parse a substrate THROWN error into an errorShape.
 * The publish substrate may throw on uncaught failures. The
 * emission-orchestrator's try/catch routes thrown errors here.
 *
 * @param {Error|string} err
 * @param {string} domain
 * @returns {object} errorShape
 */
function parseError(err, domain) {
  if (!err) {
    return _unknownErrorShape('null_error', domain);
  }

  // If the thrown error is an IG response with a code
  if (typeof err === 'object' && (err.code != null || err.response?.status)) {
    return _fromStructuredFields(err, domain);
  }

  const message = typeof err === 'string' ? err : (err.message || String(err));
  return _fromErrorString(message, domain);
}

// ── Private helpers ──────────────────────────────────────────────

function _fromStructuredFields(input, domain) {
  const code = input.code ?? input.response?.status ?? null;
  const entry = code != null ? IG_CODE_TABLE[code] : null;

  if (entry) {
    return _buildShape({
      category: input.error_category || entry.category,
      code,
      retryable: input.retryable != null ? input.retryable : entry.retryable,
      retryAfterSeconds: input.retry_after_seconds ?? entry.retryAfterSeconds,
      domain,
      source: 'ig_code_table',
    });
  }

  // Code not in table — use the error_category from the substrate
  // if present, otherwise default to unknown-transient
  const category = input.error_category || 'transient';
  return _buildShape({
    category,
    code,
    retryable: input.retryable != null ? input.retryable
      : (category === 'transient' || category === 'rate_limit'),
    retryAfterSeconds: input.retry_after_seconds ?? null,
    domain,
    source: 'substrate_structured',
  });
}

function _fromErrorString(message, domain) {
  if (RATE_LIMIT_MESSAGE_PATTERNS.some(p => p.test(message))) {
    return _buildShape({
      category: 'rate_limit', code: null, retryable: true,
      retryAfterSeconds: 60, domain, source: 'message_heuristic',
    });
  }
  if (AUTH_FAILURE_MESSAGE_PATTERNS.some(p => p.test(message))) {
    return _buildShape({
      category: 'auth_failure', code: null, retryable: false,
      retryAfterSeconds: null, domain, source: 'message_heuristic',
    });
  }
  if (TRANSIENT_MESSAGE_PATTERNS.some(p => p.test(message))) {
    return _buildShape({
      category: 'transient', code: null, retryable: true,
      retryAfterSeconds: 30, domain, source: 'message_heuristic',
    });
  }
  // Unrecognised — default to transient (retryable). Fail-open
  // so the classifier gets a chance to decide.
  return _buildShape({
    category: 'transient', code: null, retryable: true,
    retryAfterSeconds: 30, domain, source: 'message_default',
  });
}

function _unknownErrorShape(reason, domain) {
  return _buildShape({
    category: 'transient', code: null, retryable: true,
    retryAfterSeconds: 30, domain, source: `unknown:${reason}`,
  });
}

function _buildShape(parts) {
  const retryAfterSeconds = parts.retryAfterSeconds ?? null;
  return {
    category: parts.category,
    code: parts.code,
    retryable: parts.retryable,
    retryAfterSeconds,
    retryAfterMs: retryAfterSeconds != null
      ? retryAfterSeconds * 1000
      : null,
    domain: parts.domain,
    source: parts.source,
  };
}

module.exports = { parse, parseError, IG_CODE_TABLE };
