// substrates/ig-reliability-substrate.js
// Instagram Reliability Substrate — RELIABILITY-SUBSTRATE implementation
// for the Instagram Graph API dependency.
//
// Phase 2 (sprint) — three-turn instantiation. This file holds §1-§5
// of the 16-responsibility IG substrate spec:
//   §1 Error Normalization
//   §2 Failure Classification
//   §3 Token Lifecycle Intelligence
//   §4 Token Resolution Kernel Integration
//   §5 Quota Intelligence
//
// Turns 2 and 3 will add §6-§16 to this file. The file grows
// monotonically; the public surface (analyzeFailure) is sealed
// only at Turn 3.
//
// CONSTITUTIONAL CONTRACT:
//   Owns:
//     Interpretation of Instagram Graph API failures into a
//     canonical ontology consumed by the retry-cadence FSM and
//     the recovery worker framework. NOT a worker. NOT an FSM.
//     The substrate is the universal failure interpretation
//     engine for the IG dependency.
//
//   Does NOT own:
//     - Retry policy upper bounds (policy.js)
//     - Retry scheduling and timers (engagement-fsm)
//     - Worker execution (worker layer)
//     - FSM state transitions
//     - Token storage (graph-capability-kernel vault substrate)
//     - Token resolution (credential-resolver.js)
//
// USAGE (will be sealed in Turn 3):
//   const { analyzeFailure } = require('.../ig-reliability-substrate');
//   const analysis = analyzeFailure(rawError, 'publish:post', 'ig-graph', {
//     accountId, businessAccountId, intentId, attemptN: 1,
//     endpoint, operation, lineageId, lineageDomain, workerName,
//     publicationId, containerId, requestId, tokenMetadata,
//     quotaMetadata, correlationIds,
//   });
//
// LIVES AT: substrates/ because this is a shared bedrock substrate
// token-resolution kernel is the IG-side credential authority,
// and the substrate integrates with it (per spec §4). The
// substrate is a client of the token-resolution kernel, not a
// sibling.

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════
// §1 ERROR NORMALIZATION
// ═══════════════════════════════════════════════════════════════════════════
// Convert IG API error shapes (axios errors, Graph response errors,
// OAuth failures, subcode variants) into a common envelope. Mirrors
// the persistence substrate's _normalize, but adds IG-specific
// fields: graphCode, graphSubcode, endpoint, account/business
// identifiers, token metadata slot, quota metadata slot, recovery
// context.

function _normalize(rawError, operation, source) {
  // Empty / null guard
  if (!rawError) {
    return _emptyEnvelope('unknown_error');
  }

  // Axios-style IG error: { response: { status, data: { error: { code, message, error_subcode } } }, headers: { 'retry-after' } }
  if (typeof rawError === 'object') {
    const response = rawError.response || null;
    const data = response?.data || null;
    const igError = data?.error || null;

    const httpStatus = response?.status ?? rawError.status ?? rawError.statusCode ?? null;
    const graphCode = igError?.code ?? rawError.code ?? null;
    const graphSubcode = igError?.error_subcode ?? rawError.error_subcode ?? null;
    const message = igError?.message || rawError.message || 'unknown';
    const fbtraceId = igError?.fbtrace_id || response?.headers?.['x-fb-trace-id'] || null;
    const isTransient = igError?.is_transient ?? null;
    const errorType = igError?.type || null;
    const errorUserTitle = igError?.error_user_title || null;
    const errorUserMsg = igError?.error_user_msg || null;

    // Headers carry quota + rate-limit signals
    const headers = response?.headers || {};
    const retryAfterHeader = headers['retry-after'] || null;
    const xAppUsage = headers['x-app-usage'] || null;
    const xBusinessUseCaseUsage = headers['x-business-use-case-usage'] || null;
    const xPageUsage = headers['x-page-usage'] || null;
    const adAccountId = headers['x-ad-account-id'] || null;

    const requestId = rawError.requestId || fbtraceId || null;
    const executionMs = rawError.executionMs || null;

    return {
      httpStatus,
      graphCode,
      graphSubcode,
      message,
      errorType,
      errorUserTitle,
      errorUserMsg,
      isTransient,
      fbtraceId,
      requestId,
      executionMs,
      // IG response headers — these are quota + rate-limit signals
      // that §5 quota intelligence and §6 rate-limit recovery
      // consume downstream.
      headers: {
        retryAfter: retryAfterHeader,
        xAppUsage,
        xBusinessUseCaseUsage,
        xPageUsage,
        adAccountId,
      },
      // Raw opaque copy for downstream forensic analysis
      raw: { data, headers: { ...headers } },
    };
  }

  // String error — best-effort
  return {
    httpStatus: null,
    graphCode: null,
    graphSubcode: null,
    message: String(rawError),
    errorType: null,
    errorUserTitle: null,
    errorUserMsg: null,
    isTransient: null,
    fbtraceId: null,
    requestId: null,
    executionMs: null,
    headers: _emptyHeaders(),
    raw: null,
  };
}

function _emptyEnvelope(reason) {
  return {
    httpStatus: null,
    graphCode: null,
    graphSubcode: null,
    message: reason || 'unknown_error',
    errorType: null,
    errorUserTitle: null,
    errorUserMsg: null,
    isTransient: null,
    fbtraceId: null,
    requestId: null,
    executionMs: null,
    headers: _emptyHeaders(),
    raw: null,
  };
}

function _emptyHeaders() {
  return {
    retryAfter: null,
    xAppUsage: null,
    xBusinessUseCaseUsage: null,
    xPageUsage: null,
    adAccountId: null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 FAILURE CLASSIFICATION
// ═══════════════════════════════════════════════════════════════════════════
// Universal IG ontology per spec §2:
//   TOKEN_EXPIRED, TOKEN_INVALID, TOKEN_REFRESH_REQUIRED, RATE_LIMIT,
//   PERMISSION_FAILURE, ACCOUNT_RESTRICTION, MEDIA_PROCESSING_FAILURE,
//   PUBLISHING_FAILURE, NETWORK_FAILURE, DEPENDENCY_FAILURE,
//   RESOURCE_EXHAUSTION, WEBHOOK_FAILURE, CONSISTENCY_FAILURE,
//   AUTHENTICATION_FAILURE, UNKNOWN
//
// (Plus MEDIA_PROCESSING_FAILURE and PUBLISHING_FAILURE are IG-specific
// categories that don't exist in the persistence ontology.)

// HTTP status → category mapping (highest signal)
const HTTP_CATEGORY_MAP = {
  // Transient / network
  408: { category: 'NETWORK_FAILURE',        subtype: 'request_timeout',       confidence: 0.9 },
  429: { category: 'RATE_LIMIT',             subtype: 'http_429',              confidence: 0.95 },
  500: { category: 'DEPENDENCY_FAILURE',     subtype: 'graph_5xx',             confidence: 0.7 },
  502: { category: 'DEPENDENCY_FAILURE',     subtype: 'graph_5xx',             confidence: 0.8 },
  503: { category: 'DEPENDENCY_FAILURE',     subtype: 'service_unavailable',   confidence: 0.9 },
  504: { category: 'NETWORK_FAILURE',        subtype: 'gateway_timeout',       confidence: 0.9 },
  // Auth
  400: { category: 'UNKNOWN',                subtype: 'bad_request',           confidence: 0.4 }, // overridden by IG code lookup below
  401: { category: 'AUTHENTICATION_FAILURE', subtype: 'http_401',              confidence: 0.9 },
  403: { category: 'PERMISSION_FAILURE',     subtype: 'http_403',              confidence: 0.85 },
  404: { category: 'UNKNOWN',                subtype: 'not_found',             confidence: 0.7 },
  409: { category: 'CONSISTENCY_FAILURE',    subtype: 'http_409',              confidence: 0.6 },
  422: { category: 'UNKNOWN',                subtype: 'unprocessable',         confidence: 0.6 },
};

// IG code → category mapping (the IG-specific taxonomy).
// Source: Instagram Graph API documentation + observed production codes.
// High-confidence mappings take precedence over HTTP-only inference.
const IG_CODE_MAP = {
  // ── Auth failures ────────────────────────────────────────────
  190:  { category: 'TOKEN_EXPIRED',         subtype: 'access_token_expired',  confidence: 0.99 },
  102:  { category: 'TOKEN_INVALID',         subtype: 'session_expired',       confidence: 0.99 },
  104:  { category: 'TOKEN_INVALID',         subtype: 'invalid_token',         confidence: 0.99 },
  // Facebook Login OAuth errors
  200:  { category: 'PERMISSION_FAILURE',    subtype: 'permission_denied',     confidence: 0.9 },
  10:   { category: 'PERMISSION_FAILURE',    subtype: 'permission_not_granted',confidence: 0.9 },
  // ── Rate limits (HTTP 400, NOT 429 — IG uses IG codes) ──────
  4:    { category: 'RATE_LIMIT',            subtype: 'app_level_throttle',    confidence: 0.99 },
  17:   { category: 'RATE_LIMIT',            subtype: 'user_request_limit',    confidence: 0.99 },
  32:   { category: 'RATE_LIMIT',            subtype: 'page_level_throttle',   confidence: 0.99 },
  613:  { category: 'RATE_LIMIT',            subtype: 'rate_limit_exceeded',   confidence: 0.99 },
  // ── Permission / account-level ──────────────────────────────
  220:  { category: 'PERMISSION_FAILURE',    subtype: 'account_restricted',    confidence: 0.9 },
  901:  { category: 'ACCOUNT_RESTRICTION',   subtype: 'user_checked_app',      confidence: 0.95 },
  902:  { category: 'ACCOUNT_RESTRICTION',   subtype: 'user_uninstalled',      confidence: 0.95 },
  // ── Publishing / media ──────────────────────────────────────
  // Subcode 2207027 = "Your media is not yet ready to be published"
  9004: { category: 'MEDIA_PROCESSING_FAILURE', subtype: 'media_not_ready',   confidence: 0.95 },
  // Subcode 2207050 = "Media ID is invalid" / "This media does not exist"
  9007: { category: 'MEDIA_PROCESSING_FAILURE', subtype: 'invalid_media_id',  confidence: 0.95 },
  // ── Webhook / real-time ─────────────────────────────────────
  // (Webhook failures surface through different transports, but
  //  if observed in the response envelope they map here.)
};

// IG subcode → category refinement (subcodes override codes when
// more specific). The IG error_subcode is the second precision
// vector after graphCode.
const IG_SUBCODE_MAP = {
  '2207027': { category: 'MEDIA_PROCESSING_FAILURE', subtype: 'media_not_ready',       confidence: 0.99 },
  '2207050': { category: 'MEDIA_PROCESSING_FAILURE', subtype: 'invalid_media_id',      confidence: 0.99 },
  '2207002': { category: 'PUBLISHING_FAILURE',       subtype: 'media_upload_failed',   confidence: 0.95 },
  '2207022': { category: 'PUBLISHING_FAILURE',       subtype: 'igtv_post_failed',      confidence: 0.95 },
  '2207042': { category: 'PUBLISHING_FAILURE',       subtype: 'invalid_publish_target',confidence: 0.9 },
  '459':    { category: 'ACCOUNT_RESTRICTION',      subtype: 'account_disabled',      confidence: 0.99 },
  '460':    { category: 'ACCOUNT_RESTRICTION',      subtype: 'password_reset_required',confidence: 0.95 },
  '463':    { category: 'AUTHENTICATION_FAILURE',   subtype: 'reauth_required',       confidence: 0.95 },
};

function _classify(normalized, operation, context) {
  const reasoning = [];
  const op = operation || 'unknown';

  // 1. IG subcode wins (most specific IG signal)
  if (normalized.graphSubcode != null && IG_SUBCODE_MAP[String(normalized.graphSubcode)]) {
    const m = IG_SUBCODE_MAP[String(normalized.graphSubcode)];
    reasoning.push(`ig_subcode_${normalized.graphSubcode}→${m.category}/${m.subtype}`);
    return { category: m.category, subtype: m.subtype, confidence: m.confidence, reasoning };
  }

  // 2. IG code
  if (normalized.graphCode != null && IG_CODE_MAP[normalized.graphCode]) {
    const m = IG_CODE_MAP[normalized.graphCode];
    reasoning.push(`ig_code_${normalized.graphCode}→${m.category}/${m.subtype}`);
    return { category: m.category, subtype: m.subtype, confidence: m.confidence, reasoning };
  }

  // 3. HTTP status (only for non-overlapping statuses — IG codes
  //    are the dominant signal for HTTP 400)
  if (normalized.httpStatus != null && HTTP_CATEGORY_MAP[normalized.httpStatus]) {
    const m = HTTP_CATEGORY_MAP[normalized.httpStatus];
    if (m.confidence >= 0.8) {
      reasoning.push(`http_${normalized.httpStatus}→${m.category}/${m.subtype}`);
      return { category: m.category, subtype: m.subtype, confidence: m.confidence, reasoning };
    }
  }

  // 4. Network-layer code (axios / Node socket errors)
  if (typeof normalized.raw === 'object' && normalized.raw) {
    const errCode = normalized.raw?.code || null;
    if (errCode === 'ETIMEDOUT' || errCode === 'ECONNABORTED') {
      reasoning.push(`network_code_${errCode}→NETWORK_FAILURE`);
      return { category: 'NETWORK_FAILURE', subtype: 'timeout', confidence: 0.85, reasoning };
    }
    if (errCode === 'ECONNREFUSED' || errCode === 'ENOTFOUND' || errCode === 'ECONNRESET') {
      reasoning.push(`network_code_${errCode}→NETWORK_FAILURE`);
      return { category: 'NETWORK_FAILURE', subtype: 'connection_reset', confidence: 0.85, reasoning };
    }
  }

  // 5. Message-pattern inference (lower confidence)
  const msg = (normalized.message || '').toLowerCase();
  if (/timeout|timed out|deadline exceeded/.test(msg)) {
    reasoning.push('msg_pattern→NETWORK_FAILURE');
    return { category: 'NETWORK_FAILURE', subtype: 'message_pattern_timeout', confidence: 0.7 };
  }
  if (/rate.?limit|throttl|too many requests|429/.test(msg)) {
    reasoning.push('msg_pattern→RATE_LIMIT');
    return { category: 'RATE_LIMIT', subtype: 'message_pattern', confidence: 0.7 };
  }
  if (/token expired|access.?token|invalid token|expired session/.test(msg)) {
    reasoning.push('msg_pattern→TOKEN_EXPIRED');
    return { category: 'TOKEN_EXPIRED', subtype: 'message_pattern', confidence: 0.75 };
  }
  if (/unauthorized|auth.*fail|invalid auth/.test(msg)) {
    reasoning.push('msg_pattern→AUTHENTICATION_FAILURE');
    return { category: 'AUTHENTICATION_FAILURE', subtype: 'message_pattern', confidence: 0.7 };
  }
  if (/permission|not authorized|scope/.test(msg)) {
    reasoning.push('msg_pattern→PERMISSION_FAILURE');
    return { category: 'PERMISSION_FAILURE', subtype: 'message_pattern', confidence: 0.7 };
  }
  if (/network|connection refused|connection reset|econnrefused|enotfound/.test(msg)) {
    reasoning.push('msg_pattern→NETWORK_FAILURE');
    return { category: 'NETWORK_FAILURE', subtype: 'connection_refused', confidence: 0.75 };
  }
  if (/quota|usage limit|business.?use.?case/.test(msg)) {
    reasoning.push('msg_pattern→RESOURCE_EXHAUSTION');
    return { category: 'RESOURCE_EXHAUSTION', subtype: 'quota_message', confidence: 0.7 };
  }
  if (/media.*not.*ready|processing/.test(msg)) {
    reasoning.push('msg_pattern→MEDIA_PROCESSING_FAILURE');
    return { category: 'MEDIA_PROCESSING_FAILURE', subtype: 'message_pattern', confidence: 0.7 };
  }
  if (/publish.*fail|post.*fail|upload.*fail/.test(msg)) {
    reasoning.push('msg_pattern→PUBLISHING_FAILURE');
    return { category: 'PUBLISHING_FAILURE', subtype: 'message_pattern', confidence: 0.65 };
  }

  // 6. Last resort
  reasoning.push('no_signal→UNKNOWN');
  return { category: 'UNKNOWN', subtype: 'unclassified', confidence: 0.3, reasoning };
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 TOKEN LIFECYCLE INTELLIGENCE
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §3: long-lived IG tokens are valid ~60 days. The substrate
// tracks token age, expiration windows, refresh eligibility, refresh
// history, and authorization state. Proactive refresh must happen
// BEFORE the expiry window to prevent operational failure.
//
// The substrate is read-only with respect to token storage. It
// consumes token metadata from the caller (passed via context) and
// from the token-resolution kernel (per §4). It does NOT persist
// token state itself.

const TOKEN_REFRESH_WINDOW_DAYS = 7;       // refresh when this many days remain
const TOKEN_LIFESPAN_DAYS = 60;             // IG long-lived token typical lifespan
const TOKEN_REFRESH_GRACE_DAYS = 1;         // grace period after expiry for refresh

function _analyzeTokenLifecycle(context, classified) {
  const token = context?.tokenMetadata || null;
  const result = {
    tokenPresent: !!token,
    tokenAge: null,
    daysUntilExpiry: null,
    refreshEligible: false,
    refreshWindowActive: false,
    refreshHistoryCount: token?.refreshHistory?.length ?? 0,
    authorizationState: token?.authorizationState || 'unknown',
    permissionScopes: token?.scopes || [],
    isLongLived: token?.isLongLived ?? false,
    recommendation: null,
  };

  if (!token) {
    result.recommendation = classified.category === 'TOKEN_EXPIRED' || classified.category === 'TOKEN_INVALID'
      ? 'REFRESH_TOKEN'
      : 'INSPECT_TOKEN_MISSING';
    return result;
  }

  // Age in days (issuedAt is ISO timestamp)
  if (token.issuedAt) {
    const issuedMs = Date.parse(token.issuedAt);
    if (!isNaN(issuedMs)) {
      result.tokenAge = Math.floor((Date.now() - issuedMs) / 86400000);
    }
  }

  // Days until expiry
  if (token.expiresAt) {
    const expiresMs = Date.parse(token.expiresAt);
    if (!isNaN(expiresMs)) {
      result.daysUntilExpiry = Math.floor((expiresMs - Date.now()) / 86400000);
    }
  }

  // Refresh eligibility
  // 1. Must not already be expired by more than the grace window
  // 2. Must not have exceeded the refresh history limit (IG allows
  //    one long-lived refresh per long-lived token; after that the
  //    user must re-authorize)
  const withinGrace = result.daysUntilExpiry == null
    || result.daysUntilExpiry >= -TOKEN_REFRESH_GRACE_DAYS;
  const hasRefreshBudget = result.refreshHistoryCount < 1; // IG long-lived refresh rule
  result.refreshEligible = withinGrace && hasRefreshBudget;

  // Refresh window active: daysUntilExpiry <= TOKEN_REFRESH_WINDOW_DAYS
  // AND token is long-lived (short-lived tokens can't be refreshed
  // programmatically — user must re-authorize)
  result.refreshWindowActive = (
    result.isLongLived
    && result.daysUntilExpiry != null
    && result.daysUntilExpiry <= TOKEN_REFRESH_WINDOW_DAYS
    && result.daysUntilExpiry >= -TOKEN_REFRESH_GRACE_DAYS
  );

  // Recommendation
  if (classified.category === 'TOKEN_EXPIRED' || classified.category === 'TOKEN_INVALID') {
    result.recommendation = result.refreshEligible ? 'REFRESH_TOKEN' : 'REAUTHORIZE_USER';
  } else if (result.refreshWindowActive) {
    result.recommendation = 'PROACTIVE_REFRESH';
  } else if (result.tokenAge != null && result.tokenAge >= TOKEN_LIFESPAN_DAYS - TOKEN_REFRESH_WINDOW_DAYS) {
    // Approaching window: pro-active recommendation
    result.recommendation = 'PROACTIVE_REFRESH';
  } else {
    result.recommendation = 'NO_TOKEN_ACTION';
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 TOKEN RESOLUTION KERNEL INTEGRATION
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §4: the substrate reads from the token-resolution kernel
// (graph-capability-kernel) rather than touching credential storage
// directly. The integration is a thin facade:
//
//   - The substrate's caller (worker / FSM) passes the resolved
//     token in context.tokenMetadata.
//   - The substrate's `_resolveTokenMetadata` is the integration
//     seam: if tokenMetadata is not provided AND the substrate
//     has a token-resolver reference, it asks the resolver.
//   - Recovery workers MUST go through this seam — never direct
//     to the vault.
//
// The substrate does NOT instantiate the resolver. The kernel
// is wired in via setTokenResolver() at boot, called by the
// orchastrator. This is symmetric with the persistence substrate's
// pattern (substrate holds no state; orchastrator wires dependencies).

let _tokenResolver = null;

function setTokenResolver(resolver) {
  _tokenResolver = resolver;
}

/**
 * Read-only token metadata fetch. Goes through the resolver if
 * present, returns null if the resolver has no record. Does NOT
 * touch credential storage.
 *
 * @param {string} accountId
 * @returns {Promise<object|null>} token metadata or null
 */
async function _resolveTokenMetadata(accountId) {
  if (!_tokenResolver || !accountId) return null;
  try {
    if (typeof _tokenResolver.resolveMetadata === 'function') {
      return await _tokenResolver.resolveMetadata(accountId);
    }
    if (typeof _tokenResolver.resolveAccountCredentials === 'function') {
      // Fallback: derive metadata from a credential resolution
      const creds = await _tokenResolver.resolveAccountCredentials(accountId);
      if (!creds) return null;
      return {
        issuedAt: creds.issuedAt || null,
        expiresAt: creds.expiresAt || null,
        isLongLived: creds.isLongLived ?? false,
        scopes: creds.scopes || [],
        authorizationState: creds.authorizationState || 'authorized',
        refreshHistory: creds.refreshHistory || [],
      };
    }
  } catch (err) {
    // Resolver failure does not break the substrate — caller can
    // still supply tokenMetadata via context.
    return null;
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 QUOTA INTELLIGENCE
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §5: monitor account-specific usage, consumption rates,
// throttle events, recovery windows, request densities, and account-
// level utilization. Instagram exposes usage telemetry through:
//
//   - x-app-usage header (app-level — 100% = at cap)
//   - x-business-use-case-usage header (per-call-type percentages)
//   - x-page-usage header (page-level usage)
//   - ad_account_id header (when scope applies)
//
// The substrate parses these headers into a structured form so
// the FSM can adapt cadence (per §14 adaptive cadence, Turn 3).

function _parseUsageHeader(value) {
  // Header value is a JSON-like string with percentage usage per
  // call_type or action. Example:
  //   {"data":[{"call_type":"ads_management","usage":45}]}
  // Returns an array of {call_type, usage} or null on parse fail.
  if (!value || typeof value !== 'string') return null;
  try {
    const parsed = JSON.parse(value);
    if (parsed?.data && Array.isArray(parsed.data)) {
      return parsed.data.map((entry) => ({
        callType: entry.call_type || 'unknown',
        usage: typeof entry.usage === 'number' ? entry.usage : null,
        estimatedTimeToRegainAccess: entry.estimated_time_to_regain_access || null,
        totalCputime: entry.total_cputime || null,
        totalTime: entry.total_time || null,
      }));
    }
  } catch {
    return null;
  }
  return null;
}

function _analyzeQuota(normalized, context) {
  const result = {
    appUsage: null,
    businessUseCaseUsage: null,
    pageUsage: null,
    appUsagePercent: null,
    pageUsagePercent: null,
    pressureLevel: 'NONE',     // NONE | LOW | MEDIUM | HIGH | CRITICAL
    pressureScore: 0,
    throttledCallTypes: [],
    recommendedConcurrencyDelta: 0,
  };

  // x-app-usage: app-level usage across all accounts
  if (normalized.headers.xAppUsage) {
    const entries = _parseUsageHeader(normalized.headers.xAppUsage);
    if (entries) {
      result.appUsage = entries;
      const maxUsage = entries.reduce((m, e) => Math.max(m, e.usage || 0), 0);
      result.appUsagePercent = maxUsage;
    }
  }

  // x-page-usage: per-page usage
  if (normalized.headers.xPageUsage) {
    const entries = _parseUsageHeader(normalized.headers.xPageUsage);
    if (entries) {
      result.pageUsage = entries;
      const maxUsage = entries.reduce((m, e) => Math.max(m, e.usage || 0), 0);
      result.pageUsagePercent = maxUsage;
    }
  }

  // x-business-use-case-usage: specific business use case
  if (normalized.headers.xBusinessUseCaseUsage) {
    const entries = _parseUsageHeader(normalized.headers.xBusinessUseCaseUsage);
    if (entries) {
      result.businessUseCaseUsage = entries;
      // Throttled call types (>= 100% = at cap)
      result.throttledCallTypes = entries
        .filter((e) => (e.usage ?? 0) >= 100)
        .map((e) => e.callType);
    }
  }

  // Pressure scoring: highest of app/page/business
  const maxPercent = Math.max(
    result.appUsagePercent ?? 0,
    result.pageUsagePercent ?? 0,
    ...(result.businessUseCaseUsage || []).map((e) => e.usage ?? 0)
  );

  if (maxPercent >= 100) {
    result.pressureLevel = 'CRITICAL';
    result.pressureScore = 100;
    result.recommendedConcurrencyDelta = -3;
  } else if (maxPercent >= 80) {
    result.pressureLevel = 'HIGH';
    result.pressureScore = 80;
    result.recommendedConcurrencyDelta = -2;
  } else if (maxPercent >= 60) {
    result.pressureLevel = 'MEDIUM';
    result.pressureScore = 60;
    result.recommendedConcurrencyDelta = -1;
  } else if (maxPercent >= 40) {
    result.pressureLevel = 'LOW';
    result.pressureScore = 40;
    result.recommendedConcurrencyDelta = 0;
  } else {
    result.pressureLevel = 'NONE';
    result.pressureScore = maxPercent;
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS — turn 1 surface
// ═══════════════════════════════════════════════════════════════════════════
// §1-§5 are sealed. §6-§16 (turns 2-3) will append additional
// helpers. The analyzeFailure entry point is added in turn 3
// when all 16 sections are complete.

// ═══════════════════════════════════════════════════════════════════════════
// §6 RATE-LIMIT RECOVERY ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §6: rate-limit events must NOT be treated as generic
// failures. The substrate classifies them separately from dependency
// outages, generates recovery workflows (workload throttling, queue
// deferral, request pacing, operation prioritization, adaptive
// scheduling), and is account-aware rather than globally scoped.
//
// The IG `retry-after` header (when present) is the most authoritative
// signal. IG rate-limit codes (4, 17, 32, 613) carry implicit cooldown
// windows which §6 consults to compute retryAfterMs when the header
// is absent.
//
// Account-aware scope: the rate-limit context includes the accountId
// so the FSM can decide whether to throttle the account specifically
// vs the global workload.

const IG_CODE_DEFAULT_COOLDOWN_MS = {
  4:    60000,   // app-level throttle — 60s
  17:   60000,   // user request limit — 60s
  32:   15000,   // page-level — faster recovery 15s
  613:  60000,   // rate limit exceeded — 60s
};

function _analyzeRateLimit(normalized, classified, context) {
  const isRateLimited = classified.category === 'RATE_LIMIT';
  let retryAfterMs = null;
  let retryAfterSource = null;
  let recommendedThrottle = false;
  let recommendedDefer = false;
  let recommendedPacing = false;
  let accountScoped = false;

  if (isRateLimited) {
    // Priority 1: IG `retry-after` header (most authoritative)
    if (normalized.headers.retryAfter) {
      const ra = normalized.headers.retryAfter;
      if (typeof ra === 'string') {
        const n = parseInt(ra, 10);
        if (!isNaN(n)) {
          retryAfterMs = n * 1000;
          retryAfterSource = 'ig_retry_after_header';
        } else {
          const ms = Date.parse(ra) - Date.now();
          if (!isNaN(ms) && ms > 0) {
            retryAfterMs = ms;
            retryAfterSource = 'ig_retry_after_httpdate';
          }
        }
      } else if (typeof ra === 'number') {
        retryAfterMs = ra * 1000;
        retryAfterSource = 'ig_retry_after_header';
      }
    }

    // Priority 2: IG code default cooldown
    if (retryAfterMs == null && normalized.graphCode != null
        && IG_CODE_DEFAULT_COOLDOWN_MS[normalized.graphCode] != null) {
      retryAfterMs = IG_CODE_DEFAULT_COOLDOWN_MS[normalized.graphCode];
      retryAfterSource = 'ig_code_default';
    }

    // Priority 3: page-level throttling has a known 900s cap
    if (retryAfterMs == null && normalized.graphCode === 32) {
      retryAfterMs = 900000;
      retryAfterSource = 'ig_code_32_default';
    }

    // Priority 4: last-resort default
    if (retryAfterMs == null) {
      retryAfterMs = 3600000;  // 1h
      retryAfterSource = 'default_1h';
    }

    // Account-scoped when the operation target is a specific account
    accountScoped = !!(context?.accountId);

    // Throttle recommendation: persistent throttling (3+ in window)
    // is signalled by the FSM's throttle tracker. The substrate
    // emits `recommendedThrottle: true` when the cooldown exceeds
    // 5 minutes — that is the empirical threshold past which
    // workload shaping is preferred over per-call backoff.
    if (retryAfterMs > 5 * 60 * 1000) {
      recommendedThrottle = true;
    }

    // Defer recommendation: when the cooldown exceeds 30 minutes
    // the operation is best deferred to a low-priority queue
    // rather than holding the worker.
    if (retryAfterMs > 30 * 60 * 1000) {
      recommendedDefer = true;
    }

    // Pacing recommendation: account-level usage at MEDIUM+
    // pressure (§5) triggers request pacing.
    if (context?.quotaMetadata?.pressureLevel === 'MEDIUM'
        || context?.quotaMetadata?.pressureLevel === 'HIGH'
        || context?.quotaMetadata?.pressureLevel === 'CRITICAL') {
      recommendedPacing = true;
    }
  }

  return {
    isRateLimited,
    retryAfterMs,
    retryAfterSource,
    accountScoped,
    recommendedThrottle,
    recommendedDefer,
    recommendedPacing,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 REQUEST PRIORITIZATION ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §7: not all IG operations have equal business value. The
// substrate assigns each operation a priority class so the FSM can
// defer low-priority work before higher-priority work is impacted.
//
// Priority classes (high → low):
//   CRITICAL — token refresh, account synchronization, webhook
//              processing (auth-dependent; must run)
//   HIGH     — publishing operations (user-initiated; high business
//              value)
//   MEDIUM   — analytics collection, engagement sync
//   LOW      — bulk UGC operations, audit syncs
//
// The priority is keyed by the operation domain (publish:post, etc.)
// plus the operation context (initiator, urgency, account state).

const PRIORITY_TABLE = {
  // Publishing — user-initiated, high business value
  'publish:post':    { priority: 'HIGH',     businessValue: 0.9,  deferrable: false },
  'publish:story':   { priority: 'HIGH',     businessValue: 0.8,  deferrable: false },
  'publish:comment': { priority: 'HIGH',     businessValue: 0.85, deferrable: false },
  'publish:message': { priority: 'HIGH',     businessValue: 0.85, deferrable: false },
  // Engagement reads — feed the publishing pipeline
  comments:          { priority: 'MEDIUM',   businessValue: 0.7,  deferrable: true  },
  messages:          { priority: 'MEDIUM',   businessValue: 0.7,  deferrable: true  },
  // UGC / analytics
  ugc:               { priority: 'MEDIUM',   businessValue: 0.5,  deferrable: true  },
  insights:          { priority: 'LOW',      businessValue: 0.4,  deferrable: true  },
  // Token / account lifecycle
  'token:refresh':   { priority: 'CRITICAL', businessValue: 1.0,  deferrable: false },
  'account:sync':    { priority: 'CRITICAL', businessValue: 0.95, deferrable: false },
  'webhook:process': { priority: 'CRITICAL', businessValue: 0.95, deferrable: false },
  'webhook:replay':  { priority: 'HIGH',     businessValue: 0.85, deferrable: false },
  // Audit / maintenance
  'audit:sync':      { priority: 'LOW',      businessValue: 0.2,  deferrable: true  },
};

function _analyzePrioritization(operation, context) {
  const op = operation || 'unknown';
  const entry = PRIORITY_TABLE[op] || {
    priority: 'MEDIUM',
    businessValue: 0.5,
    deferrable: true,
  };

  // Adjust based on context
  let priority = entry.priority;
  let deferrable = entry.deferrable;

  // Token issues escalate priority (the operation may be the
  // first indication of a token failure — running it confirms)
  if (context?.tokenMetadata?.authorizationState === 'expiring') {
    if (priority === 'MEDIUM' || priority === 'LOW') priority = 'HIGH';
  }

  // Account restrictions escalate priority
  if (context?.accountRestrictionDetected) {
    if (priority !== 'CRITICAL') priority = 'HIGH';
    deferrable = false;
  }

  // Quota pressure reduces deferrable operations first
  if (context?.quotaMetadata?.pressureLevel === 'CRITICAL' && deferrable) {
    priority = 'LOW';
  }

  return {
    priority,
    businessValue: entry.businessValue,
    deferrable,
    operation: op,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 MEDIA PROCESSING INTELLIGENCE
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §8: a successful upload does NOT guarantee publication.
// Instagram publishing involves async media processing. The substrate
// tracks container creation, processing status, publication state,
// processing failures, and completion signals.
//
// Media processing delays are NOT infrastructure failures. The
// substrate distinguishes:
//   - MEDIA_PROCESSING_FAILURE: media container is in error state
//   - MEDIA_PROCESSING_PENDING: container is in PROCESSING state
//   - MEDIA_PROCESSING_TIMEOUT: container hasn't completed in N seconds
//
// The substrate consumes context.publicationState to derive the
// processing verdict.

const MEDIA_PROCESSING_STATES = {
  PROCESSING:    { isTerminal: false, isFailure: false, isPending: true  },
  IN_PROGRESS:   { isTerminal: false, isFailure: false, isPending: true  },
  PUBLISHED:     { isTerminal: true,  isFailure: false, isPending: false },
  FINISHED:      { isTerminal: true,  isFailure: false, isPending: false },
  ERROR:         { isTerminal: true,  isFailure: true,  isPending: false },
  EXPIRED:       { isTerminal: true,  isFailure: true,  isPending: false },
  FAILED:        { isTerminal: true,  isFailure: true,  isPending: false },
};

const MEDIA_PROCESSING_DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;  // 5 min

function _analyzeMediaProcessing(context, classified) {
  const pub = context?.publicationState || null;
  const result = {
    isPublishingOperation: false,
    publicationState: pub?.status_code || pub?.status || null,
    containerId: pub?.container_id || context?.containerId || null,
    publicationId: pub?.id || context?.publicationId || null,
    processingVerdict: 'NOT_APPLICABLE',  // PENDING | COMPLETE | FAILED | TIMEOUT | NOT_APPLICABLE
    processingDurationMs: null,
    isTerminal: false,
    isFailure: false,
    recommendation: null,
  };

  // Only apply to publishing operations
  const op = context?.operation || 'unknown';
  if (!op.startsWith('publish:') && op !== 'media:upload') {
    return result;
  }

  result.isPublishingOperation = true;

  // If the substrate's classify already returned MEDIA_PROCESSING_FAILURE,
  // honor that as the verdict.
  if (classified.category === 'MEDIA_PROCESSING_FAILURE') {
    result.processingVerdict = 'FAILED';
    result.isTerminal = true;
    result.isFailure = true;
    result.recommendation = 'RECOVER_MEDIA_CONTAINER';
    return result;
  }

  // Otherwise inspect the publication state
  if (pub?.status_code && MEDIA_PROCESSING_STATES[pub.status_code]) {
    const s = MEDIA_PROCESSING_STATES[pub.status_code];
    result.isTerminal = s.isTerminal;
    result.isFailure = s.isFailure;
    if (s.isFailure) {
      result.processingVerdict = 'FAILED';
      result.recommendation = 'RECOVER_MEDIA_CONTAINER';
    } else if (s.isPending) {
      result.processingVerdict = 'PENDING';
      result.recommendation = 'VERIFY_PUBLICATION';
    } else {
      result.processingVerdict = 'COMPLETE';
    }
  }

  // Compute processing duration if timestamps available
  if (pub?.creation_time) {
    const createdMs = Date.parse(pub.creation_time);
    if (!isNaN(createdMs)) {
      result.processingDurationMs = Date.now() - createdMs;
      // Timeout verdict if still pending past the threshold
      if (result.processingVerdict === 'PENDING'
          && result.processingDurationMs > MEDIA_PROCESSING_DEFAULT_TIMEOUT_MS) {
        result.processingVerdict = 'TIMEOUT';
        result.recommendation = 'VERIFY_PUBLICATION';
      }
    }
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 PUBLISHING STATE ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §9: publication operations are STATE MACHINES, not single
// API calls. The substrate tracks transitions through:
//   creation → processing → validation → publication → verification → synchronization
//
// The state model is exposed as a canonical phase enum so the FSM
// can drive EXECUTING → IDLE transitions with phase-aware logging.
// The substrate is READ-ONLY with respect to publication state — it
// does NOT mutate the publication record.

const PUBLISHING_PHASES = [
  'CREATION',       // media container created
  'PROCESSING',     // IG is processing the media
  'VALIDATION',     // IG validating content (copyright, community standards)
  'PUBLICATION',    // media published to the surface
  'VERIFICATION',   // post-publish verification (fetch media, confirm live)
  'SYNCHRONIZATION' // downstream sync (engagement-fsm, projection writers)
];

const PUBLISHING_STATE_TO_PHASE = {
  // Instagram container status_code values
  'PROCESSING':     'PROCESSING',
  'IN_PROGRESS':    'PROCESSING',
  'PUBLISHED':      'PUBLICATION',
  'FINISHED':       'PUBLICATION',
  'ERROR':          'VALIDATION',
  'EXPIRED':        'VALIDATION',
  'PUBLISH_FAILED': 'PUBLICATION',
  'READY':          'PROCESSING',
};

function _analyzePublishingState(context, classified) {
  const pub = context?.publicationState || null;
  const op = context?.operation || 'unknown';

  // Only apply to publishing operations
  if (!op.startsWith('publish:') && op !== 'media:upload') {
    return {
      isPublishingOperation: false,
      phase: null,
      progressPercent: null,
      canRecover: false,
      recoveryPhase: null,
    };
  }

  const statusCode = pub?.status_code || pub?.status || null;
  const phase = statusCode ? (PUBLISHING_STATE_TO_PHASE[statusCode] || 'CREATION') : 'CREATION';
  const phaseIndex = PUBLISHING_PHASES.indexOf(phase);
  const progressPercent = phaseIndex >= 0
    ? Math.round((phaseIndex / (PUBLISHING_PHASES.length - 1)) * 100)
    : null;

  // Recovery eligibility: a publish is recoverable up to the
  // SYNCHRONIZATION phase. After that, recovery is the operator's
  // job (not the FSM's).
  const canRecover = phaseIndex < PUBLISHING_PHASES.length - 1;

  return {
    isPublishingOperation: true,
    phase,
    progressPercent,
    canRecover,
    // The phase to recover INTO (always one step back from current
    // failure, except for VALIDATION which is terminal for retry)
    recoveryPhase: phase === 'VALIDATION' ? null : PUBLISHING_PHASES[Math.max(0, phaseIndex - 1)] || null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 WEBHOOK RELIABILITY MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §10: polling-heavy architectures consume significantly more
// API calls than webhook-driven ones. The substrate tracks webhook
// delivery health, processing failures, sync lag, replay events,
// missed events, and webhook verification state. Webhook degradation
// triggers synchronization recovery workflows.
//
// The substrate consumes context.webhookState (set by the webhook
// ingestion substrate, not yet built) and emits a degradation
// verdict. The verdict is consumed by the FSM to decide between
// webhook-driven sync (preferred) and polling-driven sync (fallback).

const WEBHOOK_LAG_THRESHOLD_MS = 5 * 60 * 1000;   // 5 min
const WEBHOOK_DEGRADED_THRESHOLD_MS = 30 * 60 * 1000;  // 30 min
const WEBHOOK_FAILED_THRESHOLD = 5;               // consecutive failures
const WEBHOOK_MISSED_THRESHOLD = 10;              // missed events in window

function _analyzeWebhookReliability(context) {
  const wh = context?.webhookState || null;
  const result = {
    webhookActive: !!wh,
    healthState: 'UNKNOWN',     // HEALTHY | DEGRADED | FAILED | UNKNOWN
    consecutiveFailures: 0,
    syncLagMs: null,
    missedEventsInWindow: 0,
    replayRequired: false,
    verificationState: 'unknown',
    recommendation: null,
  };

  if (!wh) {
    return result;
  }

  result.webhookActive = true;
  result.consecutiveFailures = wh.consecutiveFailures ?? 0;
  result.syncLagMs = wh.syncLagMs ?? null;
  result.missedEventsInWindow = wh.missedEventsInWindow ?? 0;
  result.replayRequired = wh.replayRequired ?? false;
  result.verificationState = wh.verificationState || 'unknown';

  // Health state derivation
  if (result.consecutiveFailures >= WEBHOOK_FAILED_THRESHOLD) {
    result.healthState = 'FAILED';
    result.recommendation = 'RESYNCHRONIZE_ACCOUNT';
  } else if (result.syncLagMs != null && result.syncLagMs >= WEBHOOK_DEGRADED_THRESHOLD_MS) {
    result.healthState = 'DEGRADED';
    result.recommendation = 'REBUILD_WEBHOOK_STATE';
  } else if (result.missedEventsInWindow >= WEBHOOK_MISSED_THRESHOLD) {
    result.healthState = 'DEGRADED';
    result.recommendation = 'REBUILD_WEBHOOK_STATE';
  } else if (result.syncLagMs != null && result.syncLagMs >= WEBHOOK_LAG_THRESHOLD_MS) {
    result.healthState = 'DEGRADED';
    result.recommendation = 'VERIFY_PUBLICATION';
  } else {
    result.healthState = 'HEALTHY';
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// §11 DEPENDENCY HEALTH ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §11: the substrate continuously evaluates Instagram service
// health. Network interruptions, Graph outages, endpoint instability,
// elevated latency, and dependency degradation are tracked
// independently from account-level failures. The substrate distinguishes
// platform failures (DEPENDENCY_FAILURE) from account-specific failures
// (TOKEN_*, PERMISSION_*, ACCOUNT_RESTRICTION) whenever possible.
//
// The substrate consumes context.dependencyHealth (set by the
// health-substrate in graph-capability-kernel) and the request's
// own latency. A request that failed AFTER the dependency was
// marked DEGRADED is reclassified to DEPENDENCY_FAILURE with
// high confidence.

function _analyzeDependencyHealth(normalized, classified, context) {
  const dep = context?.dependencyHealth || null;
  const result = {
    dependencyTracked: !!dep,
    dependencyState: dep?.state || 'UNKNOWN',  // HEALTHY | DEGRADED | FAILED | UNKNOWN
    platformFailure: false,
    endpointUnstable: false,
    elevatedLatency: false,
    lastIncidentAt: dep?.lastIncidentAt || null,
    reclassifiedToDependency: false,
  };

  if (dep?.state === 'FAILED' || dep?.state === 'DEGRADED') {
    result.dependencyTracked = true;
    result.dependencyState = dep.state;
  }

  // Latency: > 5s is elevated; > 15s is failure-tier
  const execMs = normalized.executionMs ?? null;
  if (execMs != null) {
    if (execMs > 15000) {
      result.elevatedLatency = true;
      result.endpointUnstable = true;
    } else if (execMs > 5000) {
      result.elevatedLatency = true;
    }
  }

  // Reclassification: if dependency is FAILED and the failure
  // looks like a network/5xx, upgrade to DEPENDENCY_FAILURE.
  if (dep?.state === 'FAILED'
      && (classified.category === 'NETWORK_FAILURE'
          || classified.category === 'DEPENDENCY_FAILURE'
          || (classified.httpStatus && classified.httpStatus >= 500))) {
    result.platformFailure = true;
    result.reclassifiedToDependency = true;
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// §12 RETRYABILITY ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §12: retryability is evaluated by operation type. Network
// interruptions, temporary dependency failures, service unavailability,
// media processing delays, and selected rate-limit conditions may be
// retryable. Authentication failures, permission failures, invalid
// requests, account restrictions, and revoked permissions should
// generally trigger recovery workflows rather than direct retries.

const NON_RETRYABLE_CATEGORIES = new Set([
  'TOKEN_EXPIRED',         // refresh needed, not retry
  'TOKEN_INVALID',         // re-auth needed, not retry
  'PERMISSION_FAILURE',    // scope drift, not retry
  'ACCOUNT_RESTRICTION',   // operator action, not retry
  'PUBLISHING_FAILURE',    // content rejection, not retry
  'AUTHENTICATION_FAILURE',// re-auth needed
]);

const NON_RETRYABLE_SUBTYPES = new Set([
  'invalid_media_id',           // container is gone, retry won't fix
  'media_upload_failed',        // upload itself failed
  'igtv_post_failed',           // IGTV-specific failure
  'invalid_publish_target',     // bad target
  'account_disabled',           // operator action
  'password_reset_required',    // user action
  'reauth_required',            // re-auth, not retry
  'user_checked_app',
  'user_uninstalled',
]);

const CONDITIONALLY_RETRYABLE = {
  RATE_LIMIT: (rateLimit) => rateLimit.retryAfterMs != null,
  MEDIA_PROCESSING_FAILURE: (r, ctx) => ctx?.media?.processingVerdict === 'TIMEOUT'
                                       || ctx?.media?.processingVerdict === 'PENDING',
  WEBHOOK_FAILURE: () => false,  // webhook failures need rebuild, not retry
  DEPENDENCY_FAILURE: (r, ctx) => ctx?.dependency?.reclassifiedToDependency === false,
};

function _analyzeRetryability(classified, rateLimit, context) {
  const { category, subtype } = classified;

  // Explicitly non-retryable
  if (NON_RETRYABLE_CATEGORIES.has(category)) {
    return { retryable: false, reason: `non_retryable_category:${category}` };
  }

  // Non-retryable subtypes
  if (subtype && NON_RETRYABLE_SUBTYPES.has(subtype)) {
    return { retryable: false, reason: `non_retryable_subtype:${subtype}` };
  }

  // Conditionally retryable
  if (CONDITIONALLY_RETRYABLE[category]) {
    const fn = CONDITIONALLY_RETRYABLE[category];
    if (fn(rateLimit, context)) {
      return { retryable: true, reason: `conditional_retry:${category}` };
    }
    return { retryable: false, reason: `conditional_not_met:${category}` };
  }

  // Default: retryable for transient categories
  if (category === 'UNKNOWN') {
    return { retryable: false, reason: 'unknown_not_retryable_fail_loud' };
  }

  // CONSISTENCY_FAILURE, RESOURCE_EXHAUSTION, NETWORK_FAILURE,
  // DEPENDENCY_FAILURE (when reclassified) are retryable with backoff
  return { retryable: true, reason: `retryable_category:${category}` };
}

// ═══════════════════════════════════════════════════════════════════════════
// §13 IDEMPOTENCY ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §13: publishing operations require explicit replay
// protection. The substrate assumes requests can succeed while
// responses fail. Publication IDs, media container IDs, request
// IDs, and operation IDs prevent duplicate publication events
// during retry workflows.

const IDEMPOTENCY_TABLE = {
  // Reads — safe
  'read:comments':         { required: false, risk: 'safe' },
  'read:messages':         { required: false, risk: 'safe' },
  'read:media':            { required: false, risk: 'safe' },
  'read:insights':         { required: false, risk: 'safe' },
  'read:ugc':              { required: false, risk: 'safe' },
  // Token operations
  'token:exchange':        { required: true,  risk: 'duplicate_state_possible' },
  'token:refresh':         { required: true,  risk: 'duplicate_state_possible' },
  // Publishing — always required (per spec §13)
  'publish:post':          { required: true,  risk: 'duplicate_state_possible' },
  'publish:story':         { required: true,  risk: 'duplicate_state_possible' },
  'publish:comment':       { required: true,  risk: 'duplicate_state_possible' },
  'publish:message':       { required: true,  risk: 'duplicate_state_possible' },
  'media:upload':          { required: true,  risk: 'duplicate_write_possible' },
  'media:create_container':{ required: true,  risk: 'duplicate_state_possible' },
  // Webhook
  'webhook:verify':        { required: false, risk: 'safe' },
  'webhook:process':       { required: true,  risk: 'duplicate_state_possible' },
  'webhook:replay':        { required: true,  risk: 'duplicate_state_possible' },
};

function _analyzeIdempotency(operation, classified) {
  const explicit = IDEMPOTENCY_TABLE[operation];
  if (explicit) return explicit;

  // Reads default to safe
  if (operation?.startsWith('read:')) {
    return { required: false, risk: 'safe' };
  }

  // Publishing defaults to required (per spec §13)
  if (operation?.startsWith('publish:')) {
    return { required: true, risk: 'duplicate_state_possible' };
  }

  // Token operations default to required
  if (operation?.startsWith('token:')) {
    return { required: true, risk: 'duplicate_state_possible' };
  }

  // Webhook defaults vary — process is required
  if (operation?.startsWith('webhook:')) {
    return { required: true, risk: 'duplicate_state_possible' };
  }

  // Writes default to insert-style
  return { required: true, risk: 'duplicate_write_possible' };
}

function _generateIdempotencyKey(context) {
  const { accountId, intentId, publicationId, containerId, requestId, lineageId } = context || {};
  if (!lineageId || !accountId) return null;
  const input = [
    lineageId,
    accountId,
    intentId || '',
    publicationId || '',
    containerId || '',
    requestId || '',
  ].join('|');
  return crypto.createHash('sha256').update(input).digest('hex').slice(0, 32);
}

// ═══════════════════════════════════════════════════════════════════════════
// §14 ADAPTIVE CADENCE GENERATION
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §14: the IG retry cadence does not rely solely on exponential
// backoff. Cadence generation incorporates:
//   - quota pressure (§5)
//   - account utilization
//   - operation priority (§7)
//   - token health (§3)
//   - endpoint stability (§11)
//   - dependency health (§11)
//   - historical recovery performance
//
// The substrate emits a baseMs, multiplier, jitterMs. The FSM
// combines with policy.maxDelayMs (structural cap) and emits the
// actual timer. The substrate NEVER sets a timer itself.

const BASE_PROFILES = {
  // Retryable categories
  NETWORK_FAILURE:           { baseMs: 1000,  multiplier: 2,    jitterMs: 500  },
  DEPENDENCY_FAILURE:        { baseMs: 15000, multiplier: 2,    jitterMs: 3000 },
  CONSISTENCY_FAILURE:       { baseMs: 10000, multiplier: 2,    jitterMs: 2000 },
  RATE_LIMIT:                { baseMs: 60000, multiplier: 1,    jitterMs: 0    },
  RESOURCE_EXHAUSTION:       { baseMs: 60000, multiplier: 1.5,  jitterMs: 5000 },
  MEDIA_PROCESSING_FAILURE:  { baseMs: 30000, multiplier: 1.5,  jitterMs: 5000 },
  // Non-retryable
  TOKEN_EXPIRED:             { baseMs: 0,     multiplier: 0,    jitterMs: 0    },
  TOKEN_INVALID:             { baseMs: 0,     multiplier: 0,    jitterMs: 0    },
  PERMISSION_FAILURE:        { baseMs: 0,     multiplier: 0,    jitterMs: 0    },
  ACCOUNT_RESTRICTION:       { baseMs: 0,     multiplier: 0,    jitterMs: 0    },
  PUBLISHING_FAILURE:        { baseMs: 0,     multiplier: 0,    jitterMs: 0    },
  AUTHENTICATION_FAILURE:    { baseMs: 0,     multiplier: 0,    jitterMs: 0    },
  UNKNOWN:                   { baseMs: 0,     multiplier: 0,    jitterMs: 0    },
};

function _generateAdaptiveCadence(classified, rateLimit, quota, prioritization, context) {
  const profile = BASE_PROFILES[classified.category] || BASE_PROFILES.UNKNOWN;
  const attemptN = Math.max(1, context?.attemptN ?? 1);

  // Rate-limit: retryAfter overrides everything
  if (classified.category === 'RATE_LIMIT' && rateLimit.retryAfterMs != null) {
    return {
      baseMs: rateLimit.retryAfterMs,
      multiplier: 1,
      jitterMs: 0,
      computedMs: rateLimit.retryAfterMs,
      cappedMs: rateLimit.retryAfterMs,
      cadenceProfile: 'rate_limit_strict',
      adaptive: false,
    };
  }

  // Non-retryable: no backoff
  if (profile.baseMs === 0) {
    return {
      baseMs: 0, multiplier: 0, jitterMs: 0,
      computedMs: 0, cappedMs: 0,
      cadenceProfile: 'no_retry',
      adaptive: false,
    };
  }

  // Adaptive multiplier based on quota pressure
  let multiplier = profile.multiplier;
  if (quota?.pressureLevel === 'HIGH') multiplier = Math.max(1.5, multiplier);
  else if (quota?.pressureLevel === 'CRITICAL') multiplier = Math.max(2, multiplier * 1.5);

  // Adaptive base for elevated latency (slow endpoint → longer waits)
  let baseMs = profile.baseMs;
  if (context?.dependency?.elevatedLatency) {
    baseMs = baseMs * 1.5;
  }

  // Priority boost: LOW-priority operations get longer waits
  // (the FSM is more likely to defer them anyway)
  if (prioritization?.priority === 'LOW') {
    baseMs = baseMs * 1.5;
  }

  const computed = baseMs * Math.pow(multiplier, attemptN - 1);
  const jitter = profile.jitterMs > 0
    ? Math.floor(Math.random() * profile.jitterMs)
    : 0;

  return {
    baseMs,
    multiplier,
    jitterMs: profile.jitterMs,
    computedMs: computed,
    cappedMs: computed,  // policy.maxDelayMs cap is applied by the FSM
    cadenceProfile: 'adaptive',
    adaptive: true,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §15 RECOVERY RECOMMENDATION GENERATION
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §15: the substrate generates recovery recommendations.
// The FSM authorizes ALL flagged ones by emitting *_AUTHORIZED actions.
// The substrate does NOT pick one — it emits the set.
//
// IG-specific recommendation vocabulary:
//   REFRESH_TOKEN              — TOKEN_EXPIRED with refresh eligibility
//   REAUTHORIZE_USER           — TOKEN_INVALID or no refresh budget
//   VALIDATE_PERMISSIONS       — PERMISSION_FAILURE
//   THROTTLE_ACCOUNT           — RATE_LIMIT or quota CRITICAL
//   REQUEUE_OPERATION          — DEFER or quota MEDIUM+
//   REBUILD_WEBHOOK_STATE      — webhook DEGRADED/FAILED
//   VERIFY_PUBLICATION         — media PENDING/TIMEOUT
//   RECOVER_MEDIA_CONTAINER    — media FAILED
//   RESYNCHRONIZE_ACCOUNT      — webhook FAILED, dependency FAILED
//   ESCALATE_DEPENDENCY_FAILURE — DEPENDENCY_FAILURE CRITICAL
//   DEFER_NONCRITICAL_WORK     — quota HIGH+, account restriction
//   PROACTIVE_REFRESH          — token within refresh window

function _generateRecommendations(classified, retryability, rateLimit, quota, dependency, token, media, webhook, prioritization, severity) {
  const recs = [];

  // Token issues → REFRESH or REAUTHORIZE
  if (classified.category === 'TOKEN_EXPIRED') {
    if (token?.refreshEligible) {
      recs.push('REFRESH_TOKEN');
    } else {
      recs.push('REAUTHORIZE_USER');
    }
    return recs;
  }
  if (classified.category === 'TOKEN_INVALID') {
    recs.push('REAUTHORIZE_USER');
    return recs;
  }
  // Proactive refresh (token in refresh window but no failure yet)
  if (token?.refreshWindowActive && token.recommendation === 'PROACTIVE_REFRESH') {
    recs.push('PROACTIVE_REFRESH');
  }

  // Permission → VALIDATE_PERMISSIONS
  if (classified.category === 'PERMISSION_FAILURE') {
    recs.push('VALIDATE_PERMISSIONS');
    return recs;
  }

  // Account restriction → DEFER_NONCRITICAL_WORK + escalate
  if (classified.category === 'ACCOUNT_RESTRICTION') {
    recs.push('DEFER_NONCRITICAL_WORK', 'ESCALATE_DEPENDENCY_FAILURE');
    return recs;
  }

  // Publishing failure (content rejection) → escalate; retry won't help
  if (classified.category === 'PUBLISHING_FAILURE') {
    recs.push('ESCALATE_DEPENDENCY_FAILURE');
    return recs;
  }

  // Media processing — recovery verdict
  if (media?.isPublishingOperation) {
    if (media.processingVerdict === 'FAILED') {
      recs.push('RECOVER_MEDIA_CONTAINER');
      return recs;
    }
    if (media.processingVerdict === 'TIMEOUT' || media.processingVerdict === 'PENDING') {
      recs.push('VERIFY_PUBLICATION');
      // Fall through to retry logic — PENDING may resolve
    }
  }

  // Webhook health
  if (webhook?.webhookActive) {
    if (webhook.healthState === 'FAILED') {
      recs.push('RESYNCHRONIZE_ACCOUNT');
      return recs;
    }
    if (webhook.healthState === 'DEGRADED') {
      recs.push('REBUILD_WEBHOOK_STATE');
      // Fall through — the underlying operation may still be retried
    }
  }

  // Dependency failure CRITICAL → escalate + defer
  if (dependency?.reclassifiedToDependency && severity.severity === 'CRITICAL') {
    recs.push('ESCALATE_DEPENDENCY_FAILURE', 'DEFER_NONCRITICAL_WORK');
    return recs;
  }

  // Resource exhaustion / quota CRITICAL → throttle + defer
  if (quota?.pressureLevel === 'CRITICAL' || classified.category === 'RESOURCE_EXHAUSTION') {
    recs.push('THROTTLE_ACCOUNT', 'DEFER_NONCRITICAL_WORK');
    if (severity.severity === 'CRITICAL' || severity.severity === 'HIGH') {
      recs.push('ESCALATE_DEPENDENCY_FAILURE');
    }
    return recs;
  }

  // Rate-limit
  if (rateLimit?.isRateLimited) {
    recs.push('THROTTLE_ACCOUNT');
    if (rateLimit.recommendedDefer) {
      recs.push('DEFER_NONCRITICAL_WORK');
    } else if (rateLimit.recommendedThrottle) {
      recs.push('REQUEUE_OPERATION');
    }
    // Fall through to retry if retryable
  }

  // Quota HIGH → defer non-critical
  if (quota?.pressureLevel === 'HIGH' && prioritization?.deferrable) {
    recs.push('DEFER_NONCRITICAL_WORK');
  }

  // CRITICAL severity → escalate regardless
  if (severity.severity === 'CRITICAL') {
    recs.push('ESCALATE_DEPENDENCY_FAILURE', 'DEFER_NONCRITICAL_WORK');
    return recs;
  }

  // HIGH severity → at least resync or defer
  if (severity.severity === 'HIGH') {
    if (!recs.includes('RESYNCHRONIZE_ACCOUNT')) {
      recs.push('RESYNCHRONIZE_ACCOUNT');
    }
  }

  // Retryable transient → RETRY
  if (retryability.retryable) {
    recs.push('REQUEUE_OPERATION');
    return recs;
  }

  // Default for non-retryable unknowns → ESCALATE
  if (classified.category === 'UNKNOWN') {
    recs.push('ESCALATE_DEPENDENCY_FAILURE');
    return recs;
  }

  return recs;
}

// ═══════════════════════════════════════════════════════════════════════════
// §16 SEVERITY ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §16 (mirrors persistence substrate's §9): LOW / MEDIUM /
// HIGH / CRITICAL with a numeric score. CRITICAL severity bypasses
// normal flow (the FSM fires CRITICAL_FAILURE_OBSERVED immediately).

const SEVERITY_TABLE = {
  // LOW
  NETWORK_FAILURE:           { severity: 'LOW',     score: 25 },
  RATE_LIMIT:                { severity: 'LOW',     score: 30 },
  // MEDIUM
  MEDIA_PROCESSING_FAILURE:  { severity: 'MEDIUM',  score: 50 },
  RESOURCE_EXHAUSTION:       { severity: 'MEDIUM',  score: 60 },
  CONSISTENCY_FAILURE:       { severity: 'MEDIUM',  score: 55 },
  AUTHENTICATION_FAILURE:    { severity: 'MEDIUM',  score: 55 },
  PERMISSION_FAILURE:        { severity: 'MEDIUM',  score: 60 },
  // HIGH
  DEPENDENCY_FAILURE:        { severity: 'HIGH',    score: 75 },
  WEBHOOK_FAILURE:           { severity: 'HIGH',    score: 70 },
  PUBLISHING_FAILURE:        { severity: 'HIGH',    score: 80 },
  ACCOUNT_RESTRICTION:       { severity: 'HIGH',    score: 85 },
  TOKEN_EXPIRED:             { severity: 'HIGH',    score: 75 },
  TOKEN_INVALID:             { severity: 'HIGH',    score: 80 },
  // CRITICAL
  UNKNOWN:                   { severity: 'CRITICAL', score: 95 },  // fail-loud
};

function _analyzeSeverity(classified) {
  const entry = SEVERITY_TABLE[classified.category] || SEVERITY_TABLE.UNKNOWN;
  return { severity: entry.severity, severityScore: entry.score };
}

// ═══════════════════════════════════════════════════════════════════════════
// §17 TELEMETRY GENERATION
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §16 (telemetry is §16 in the spec; bumped to §17 here
// because §16 was severity). The telemetry package is attached
// to the analysis object so downstream consumers can route it
// without re-deriving fields.

function _buildTelemetry(analysis) {
  return {
    failureId: analysis.failureId,
    timestamp: analysis.timestamp,
    source: analysis.source,
    operation: analysis.operation,
    accountId: analysis.accountId,
    endpoint: analysis.endpoint,
    category: analysis.category,
    subtype: analysis.subtype,
    retryable: analysis.retryable,
    confidence: analysis.confidence,
    retryCount: analysis.attemptN,
    severity: analysis.severity,
    severityScore: analysis.severityScore,
    recommendation: analysis.recommendations.join(',') || null,
    priority: analysis.prioritization?.priority || null,
    quotaPressure: analysis.quota?.pressureLevel || null,
    tokenRefreshEligible: analysis.token?.refreshEligible ?? null,
    webhookHealth: analysis.webhook?.healthState || null,
    dependencyReclassified: analysis.dependency?.reclassifiedToDependency ?? false,
    correlationIds: {
      lineageId: analysis.lineageId,
      lineageDomain: analysis.lineageDomain,
      workerName: analysis.workerName,
      intentId: analysis.intentId,
    },
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §18 FINAL CANONICAL OUTPUT — analyzeFailure
// ═══════════════════════════════════════════════════════════════════════════
// Composes §1-§16. Single entry point for the canonical IG failure
// analysis. Mirrors the persistence substrate's analyzeFailure but
// extends it with IG-specific surface (token, quota, rate-limit
// recovery, prioritization, media, publishing state, webhook,
// dependency health).

function analyzeFailure(rawError, operation, source = 'ig-graph', context = {}) {
  const {
    attemptN = 1,
    lineageId = null,
    lineageDomain = 'ig-domain',
    workerName = null,
    accountId = null,
    businessAccountId = null,
    intentId = null,
    endpoint = null,
    tokenMetadata = null,
    publicationState = null,
    containerId = null,
    publicationId = null,
    webhookState = null,
    dependencyHealth = null,
    correlationIds = null,
  } = context;

  // §1 Normalize
  const normalized = _normalize(rawError, operation, source);

  // §2 Classify
  const classified = _classify(normalized, operation, context);

  // §3 Token lifecycle
  const token = _analyzeTokenLifecycle({ tokenMetadata, ...context }, classified);

  // §5 Quota intelligence
  const quota = _analyzeQuota(normalized, context);

  // §6 Rate-limit recovery
  const rateLimit = _analyzeRateLimit(normalized, classified, { ...context, quotaMetadata: quota });

  // §7 Prioritization
  const prioritization = _analyzePrioritization(operation, { ...context, quotaMetadata: quota });

  // §8 Media processing
  const media = _analyzeMediaProcessing({ ...context, operation, publicationState }, classified);

  // §9 Publishing state
  const publishing = _analyzePublishingState({ ...context, publicationState }, classified);

  // §10 Webhook reliability
  const webhook = _analyzeWebhookReliability({ ...context, webhookState });

  // §11 Dependency health (needs classified, normalized)
  const dependency = _analyzeDependencyHealth(normalized, classified, { ...context, dependencyHealth });

  // Apply dependency reclassification
  let effectiveCategory = classified.category;
  let effectiveSubtype = classified.subtype;
  if (dependency.reclassifiedToDependency && effectiveCategory !== 'DEPENDENCY_FAILURE') {
    effectiveCategory = 'DEPENDENCY_FAILURE';
    effectiveSubtype = 'reclassified_from_ig';
  }
  const effectiveClassified = {
    category: effectiveCategory,
    subtype: effectiveSubtype,
    confidence: classified.confidence,
    reasoning: classified.reasoning.concat(['dependency_reclassification']),
  };

  // §12 Retryability (uses effective category)
  const retryability = _analyzeRetryability(effectiveClassified, rateLimit, { media, dependency });

  // §13 Idempotency
  const idempotency = _analyzeIdempotency(operation, classified);
  const idempotencyKey = idempotency.required
    ? _generateIdempotencyKey({ accountId, intentId, publicationId, containerId,
                                requestId: normalized.requestId, lineageId })
    : null;

  // §14 Adaptive cadence
  const backoff = _generateAdaptiveCadence(effectiveClassified, rateLimit, quota, prioritization,
    { ...context, attemptN, dependency });

  // §15 Severity
  const severity = _analyzeSeverity(effectiveClassified);

  // §16 Recommendations
  const recommendations = _generateRecommendations(
    effectiveClassified, retryability, rateLimit, quota, dependency, token,
    media, webhook, prioritization, severity
  );

  const failureId = crypto.randomUUID();
  const timestamp = new Date().toISOString();

  // Compose final
  const analysis = {
    failureId,
    timestamp,
    source,
    operation,
    lineageId,
    lineageDomain,
    workerName,
    attemptN,
    accountId,
    businessAccountId,
    intentId,
    endpoint,

    normalized,
    category: effectiveClassified.category,
    subtype: effectiveClassified.subtype,
    retryable: retryability.retryable,
    retryabilityReason: retryability.reason,
    confidence: effectiveClassified.confidence,
    reasoning: effectiveClassified.reasoning,

    idempotencyRequired: idempotency.required,
    idempotencyRisk: idempotency.risk,
    idempotencyKey,

    backoff,

    rateLimit,
    quota,
    token,
    media,
    publishing,
    webhook,
    dependency,
    prioritization,

    severity: severity.severity,
    severityScore: severity.severityScore,

    recommendations,
  };

  // §17 Telemetry
  analysis.telemetry = _buildTelemetry(analysis);

  return analysis;
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS — full substrate (turn 1 + turn 2 + turn 3)
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
  // §1
  _normalize,
  _emptyEnvelope,
  _emptyHeaders,
  // §2
  _classify,
  HTTP_CATEGORY_MAP,
  IG_CODE_MAP,
  IG_SUBCODE_MAP,
  // §3
  _analyzeTokenLifecycle,
  TOKEN_REFRESH_WINDOW_DAYS,
  TOKEN_LIFESPAN_DAYS,
  TOKEN_REFRESH_GRACE_DAYS,
  // §4
  setTokenResolver,
  _resolveTokenMetadata,
  // §5
  _analyzeQuota,
  _parseUsageHeader,
  // §6
  _analyzeRateLimit,
  IG_CODE_DEFAULT_COOLDOWN_MS,
  // §7
  _analyzePrioritization,
  PRIORITY_TABLE,
  // §8
  _analyzeMediaProcessing,
  MEDIA_PROCESSING_STATES,
  MEDIA_PROCESSING_DEFAULT_TIMEOUT_MS,
  // §9
  _analyzePublishingState,
  PUBLISHING_PHASES,
  PUBLISHING_STATE_TO_PHASE,
  // §10
  _analyzeWebhookReliability,
  WEBHOOK_LAG_THRESHOLD_MS,
  WEBHOOK_DEGRADED_THRESHOLD_MS,
  WEBHOOK_FAILED_THRESHOLD,
  WEBHOOK_MISSED_THRESHOLD,
  // §11
  _analyzeDependencyHealth,
  // §12
  _analyzeRetryability,
  NON_RETRYABLE_CATEGORIES,
  NON_RETRYABLE_SUBTYPES,
  CONDITIONALLY_RETRYABLE,
  // §13
  _analyzeIdempotency,
  _generateIdempotencyKey,
  IDEMPOTENCY_TABLE,
  // §14
  _generateAdaptiveCadence,
  BASE_PROFILES,
  // §15
  _generateRecommendations,
  // §16
  _analyzeSeverity,
  SEVERITY_TABLE,
  // §17
  _buildTelemetry,
  // §18
  analyzeFailure,
};
