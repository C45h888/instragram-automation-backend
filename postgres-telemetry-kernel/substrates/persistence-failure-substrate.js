// postgres-telemetry-kernel/substrates/persistence-failure-substrate.js
// Persistence Failure Substrate — RELIABILITY-SUBSTRATE implementation
// for the persistence domain.
//
// This module implements the 12-responsibility reliability-substrate spec
// (see `.hermes/specs/reliability-substrate.md`). The substrate is the
// universal failure interpretation engine. It does NOT own state
// transitions (FSM), authority decisions (the FSM acts as authority
// vector inside the system), or execution (worker layer). It owns
// interpretation: normalize, classify, score, recommend.
//
// CONSTITUTIONAL CONTRACT:
//   Owns:
//     §1  Error Normalization
//     §2  Failure Classification
//     §3  Retryability Analysis
//     §4  Idempotency Risk Analysis
//     §5  Backoff Policy Generation
//     §6  Rate-Limit Intelligence
//     §7  Resource Exhaustion Analysis
//     §8  Timeout Intelligence
//     §9  Severity Analysis
//     §10 Recovery Recommendation Generation
//     §11 Telemetry Generation
//     §12 Final Canonical Output Composition
//
//   Does NOT own:
//     - Retry policy upper bounds (policy.js owns maxRetries, maxDelayMs)
//     - Retry scheduling and timers (retry-cadence FSM owns this)
//     - Worker execution (worker layer)
//     - FSM state transitions
//     - Authority decisions (the FSM evaluates recommendations and
//       authorizes workers to be dispatched)
//
// USAGE:
//   const { analyzeFailure } = require('.../persistence-failure-substrate');
//   const analysis = analyzeFailure(rawError, 'write', 'supabase', {
//     attemptN: 1,
//     lineageId: '...',
//     lineageDomain: 'persist-telemetry',
//     workerName: 'comments-writer',
//     primaryKeyField: 'instagram_comment_id',
//     primaryKeyValue: 'c1',
//   });
//   // analysis carries category, subtype, severity, recommendations,
//   // backoff, idempotency risk, telemetry, and the full normalized
//   // error envelope.
//
// BACKWARDS COMPAT:
//   `reportFailure(rawError, operation)` is a legacy wrapper that calls
//   analyzeFailure and returns the slim shape. Existing call sites in
//   the 10 writers continue to work until they migrate to the full
//   analysis in step 2.
//
// RETURN SHAPE — analyzeFailure:
//   {
//     failureId, timestamp, source, operation, lineageId, lineageDomain,
//     workerName, attemptN,
//     normalized: { httpStatus, pgCode, postgrestCode, message,
//                   details, requestId, executionMs },
//     category, subtype, retryable, confidence, reasoning,
//     idempotencyRequired, idempotencyRisk, idempotencyKey,
//     backoff: { baseMs, maxMs, multiplier, jitterMs, computedMs, cappedMs },
//     rateLimit: { isRateLimited, retryAfterMs, recommendedThrottle },
//     resourceExhaustion: { detected, signal, recommendedConcurrencyDelta },
//     timeout: { kind, architecturalPressure },
//     severity, severityScore,
//     recommendations: string[],
//     telemetry: { failureId, timestamp, source, operation, category,
//                  subtype, retryable, confidence, retryCount, severity,
//                  severityScore, recommendation, correlationIds },
//   }

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════
// §1 ERROR NORMALIZATION
// ═══════════════════════════════════════════════════════════════════════════
// Convert vendor error shapes (Supabase, PostgREST, GoTrue, Storage, etc.)
// into a common envelope. The substrate never assumes the caller has
// pre-normalized anything.

function _normalize(rawError, operation, source) {
  // Empty / null guard
  if (!rawError) {
    return {
      httpStatus: null,
      pgCode: null,
      postgrestCode: null,
      message: 'unknown_error',
      details: null,
      requestId: null,
      executionMs: null,
    };
  }

  // Supabase / PostgREST error shape: { message, code, hint, details }
  // where code may be a PG code (e.g. '23505') or PostgREST code (e.g. 'PGRST116')
  if (typeof rawError === 'object') {
    const code = rawError.code || null;
    const isPgCode = code && /^\d{5}$/.test(code);
    const isPostgrestCode = code && /^PGRST\d+$/.test(code);
    const httpStatus = rawError.status || rawError.statusCode || null;
    const message = rawError.message || rawError.error_description || 'unknown';
    const details = rawError.details || rawError.hint || null;
    const requestId = rawError.requestId || null;
    const executionMs = rawError.executionMs || null;

    return {
      httpStatus,
      pgCode: isPgCode ? code : null,
      postgrestCode: isPostgrestCode ? code : null,
      message,
      details,
      requestId,
      executionMs,
    };
  }

  // String error — best-effort
  return {
    httpStatus: null,
    pgCode: null,
    postgrestCode: null,
    message: String(rawError),
    details: null,
    requestId: null,
    executionMs: null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 FAILURE CLASSIFICATION
// ═══════════════════════════════════════════════════════════════════════════
// Universal ontology per spec §2:
//   NETWORK, RATE_LIMIT, AUTHENTICATION, PERMISSION, CONFLICT,
//   RESOURCE_EXHAUSTION, CONNECTION_FAILURE, TIMEOUT, CONSISTENCY_FAILURE,
//   SCHEMA_FAILURE, DEPENDENCY_FAILURE, STORAGE_FAILURE, DATABASE_FAILURE,
//   UNKNOWN

const HTTP_CATEGORY_MAP = {
  // Transient → NETWORK / CONNECTION_FAILURE
  408: { category: 'TIMEOUT', subtype: 'request_timeout', confidence: 0.9 },
  429: { category: 'RATE_LIMIT', subtype: 'http_429', confidence: 0.95 },
  500: { category: 'CONNECTION_FAILURE', subtype: 'server_5xx', confidence: 0.7 },
  502: { category: 'CONNECTION_FAILURE', subtype: 'server_5xx', confidence: 0.8 },
  503: { category: 'CONNECTION_FAILURE', subtype: 'server_unavailable', confidence: 0.9 },
  504: { category: 'TIMEOUT', subtype: 'gateway_timeout', confidence: 0.9 },
  // Auth/permission
  400: { category: 'SCHEMA_FAILURE', subtype: 'bad_request', confidence: 0.6 },
  401: { category: 'AUTHENTICATION', subtype: 'jwt_expired', confidence: 0.9 },
  403: { category: 'PERMISSION', subtype: 'rls_denied', confidence: 0.9 },
  404: { category: 'SCHEMA_FAILURE', subtype: 'not_found', confidence: 0.8 },
  409: { category: 'CONFLICT', subtype: 'http_409', confidence: 0.85 },
  410: { category: 'SCHEMA_FAILURE', subtype: 'gone', confidence: 0.9 },
  422: { category: 'SCHEMA_FAILURE', subtype: 'unprocessable', confidence: 0.9 },
};

// PG error code → category mapping (spec §3 — explicit)
const PG_CODE_MAP = {
  // Connection family 08*
  '08000': { category: 'CONNECTION_FAILURE', subtype: 'connection_exception' },
  '08003': { category: 'CONNECTION_FAILURE', subtype: 'connection_does_not_exist' },
  '08006': { category: 'CONNECTION_FAILURE', subtype: 'connection_failure' },
  '08001': { category: 'CONNECTION_FAILURE', subtype: 'sqlclient_unable_to_establish_sqlconnection' },
  '08004': { category: 'CONNECTION_FAILURE', subtype: 'sqlserver_rejected_establishment_of_sqlconnection' },
  '08007': { category: 'CONNECTION_FAILURE', subtype: 'transaction_resolution_unknown' },
  // Auth family 28*
  '28000': { category: 'AUTHENTICATION', subtype: 'invalid_authorization_specification' },
  '28P01': { category: 'AUTHENTICATION', subtype: 'invalid_password' },
  // Transaction rollback 40*
  '40000': { category: 'CONFLICT', subtype: 'transaction_rollback' },
  '40001': { category: 'CONFLICT', subtype: 'serialization_failure' },
  '40002': { category: 'CONFLICT', subtype: 'integrity_constraint_violation' },
  '40003': { category: 'CONFLICT', subtype: 'statement_completion_unknown' },
  // Resource exhaustion 53*
  '53000': { category: 'RESOURCE_EXHAUSTION', subtype: 'insufficient_resources' },
  '53100': { category: 'RESOURCE_EXHAUSTION', subtype: 'disk_full' },
  '53200': { category: 'RESOURCE_EXHAUSTION', subtype: 'out_of_memory' },
  '53300': { category: 'RESOURCE_EXHAUSTION', subtype: 'too_many_connections' },
  '53400': { category: 'RESOURCE_EXHAUSTION', subtype: 'configuration_limit_exceeded' },
  // Integrity constraint violations (PG 23*)
  '23000': { category: 'CONFLICT', subtype: 'integrity_constraint_violation' },
  '23502': { category: 'SCHEMA_FAILURE', subtype: 'not_null_violation' },
  '23503': { category: 'CONFLICT', subtype: 'foreign_key_violation' },
  '23505': { category: 'CONFLICT', subtype: 'unique_constraint_violation' },
  '23514': { category: 'SCHEMA_FAILURE', subtype: 'check_violation' },
  // Data exceptions 22*
  '22000': { category: 'SCHEMA_FAILURE', subtype: 'data_exception' },
  '22023': { category: 'SCHEMA_FAILURE', subtype: 'invalid_parameter_value' },
  '22001': { category: 'SCHEMA_FAILURE', subtype: 'string_data_right_truncation' },
  '22P02': { category: 'SCHEMA_FAILURE', subtype: 'invalid_text_representation' },
  // Query canceled 57*
  '57014': { category: 'TIMEOUT', subtype: 'query_canceled' },
  '57P01': { category: 'CONNECTION_FAILURE', subtype: 'admin_shutdown' },
  '57P02': { category: 'CONNECTION_FAILURE', subtype: 'crash_shutdown' },
  '57P03': { category: 'CONNECTION_FAILURE', subtype: 'cannot_connect_now' },
  // Operator intervention
  '55P03': { category: 'PERMISSION', subtype: 'lock_not_available' },
};

// PostgREST code mapping (spec §3)
const POSTGREST_CODE_MAP = {
  PGRST000: { category: 'DATABASE_FAILURE', subtype: 'connection_lost', confidence: 0.95 },
  PGRST001: { category: 'DATABASE_FAILURE', subtype: 'connection_lost', confidence: 0.95 },
  PGRST002: { category: 'DATABASE_FAILURE', subtype: 'connection_lost', confidence: 0.95 },
  PGRST116: { category: 'SCHEMA_FAILURE', subtype: 'row_not_found', confidence: 0.95 },
  PGRST301: { category: 'CONFLICT', subtype: 'duplicate_uri', confidence: 0.9 },
};

function _classify(normalized, operation) {
  const reasoning = [];

  // 1. HTTP status wins (highest signal)
  if (normalized.httpStatus && HTTP_CATEGORY_MAP[normalized.httpStatus]) {
    const m = HTTP_CATEGORY_MAP[normalized.httpStatus];
    reasoning.push(`http_${normalized.httpStatus}→${m.category}/${m.subtype}`);
    return { category: m.category, subtype: m.subtype, confidence: m.confidence, reasoning };
  }

  // 2. PG error code
  if (normalized.pgCode && PG_CODE_MAP[normalized.pgCode]) {
    const m = PG_CODE_MAP[normalized.pgCode];
    reasoning.push(`pg_${normalized.pgCode}→${m.category}/${m.subtype}`);
    return { category: m.category, subtype: m.subtype, confidence: 0.95, reasoning };
  }

  // 3. PG family (3-char prefix) — for codes we don't have a specific mapping for
  if (normalized.pgCode) {
    const family = normalized.pgCode.slice(0, 2);
    if (family === '08') {
      reasoning.push(`pg_family_08→CONNECTION_FAILURE`);
      return { category: 'CONNECTION_FAILURE', subtype: 'pg_family_08', confidence: 0.85 };
    }
    if (family === '53') {
      reasoning.push(`pg_family_53→RESOURCE_EXHAUSTION`);
      return { category: 'RESOURCE_EXHAUSTION', subtype: 'pg_family_53', confidence: 0.85 };
    }
    if (family === '28') {
      reasoning.push(`pg_family_28→AUTHENTICATION`);
      return { category: 'AUTHENTICATION', subtype: 'pg_family_28', confidence: 0.85 };
    }
    if (family === '40') {
      reasoning.push(`pg_family_40→CONFLICT`);
      return { category: 'CONFLICT', subtype: 'pg_family_40', confidence: 0.85 };
    }
  }

  // 4. PostgREST code
  if (normalized.postgrestCode && POSTGREST_CODE_MAP[normalized.postgrestCode]) {
    const m = POSTGREST_CODE_MAP[normalized.postgrestCode];
    reasoning.push(`postgrest_${normalized.postgrestCode}→${m.category}/${m.subtype}`);
    return { category: m.category, subtype: m.subtype, confidence: m.confidence, reasoning };
  }

  // 5. Message-pattern inference (lower confidence)
  const msg = (normalized.message || '').toLowerCase();

  if (/timeout|timed out|deadline exceeded/.test(msg)) {
    reasoning.push(`msg_pattern→TIMEOUT`);
    return { category: 'TIMEOUT', subtype: 'message_pattern', confidence: 0.7 };
  }
  if (/too many connections|connection pool|supavisor/.test(msg)) {
    reasoning.push(`msg_pattern→RESOURCE_EXHAUSTION`);
    return { category: 'RESOURCE_EXHAUSTION', subtype: 'message_pattern', confidence: 0.75 };
  }
  if (/jwt expired|invalid jwt|invalid token|token expired|unauthorized/.test(msg)) {
    reasoning.push(`msg_pattern→AUTHENTICATION`);
    return { category: 'AUTHENTICATION', subtype: 'token_expired', confidence: 0.8 };
  }
  if (/rls|row level security|policy/.test(msg)) {
    reasoning.push(`msg_pattern→PERMISSION`);
    return { category: 'PERMISSION', subtype: 'rls_denied', confidence: 0.75 };
  }
  if (/duplicate|already exists|unique/.test(msg)) {
    reasoning.push(`msg_pattern→CONFLICT`);
    return { category: 'CONFLICT', subtype: 'duplicate', confidence: 0.7 };
  }
  if (/network|connection refused|connection reset|econnrefused|enotfound/.test(msg)) {
    reasoning.push(`msg_pattern→NETWORK`);
    return { category: 'NETWORK', subtype: 'connection_refused', confidence: 0.75 };
  }
  if (/rate.?limit|429|throttl/.test(msg)) {
    reasoning.push(`msg_pattern→RATE_LIMIT`);
    return { category: 'RATE_LIMIT', subtype: 'message_pattern', confidence: 0.7 };
  }
  if (/schema|migration|column.*does not exist|relation.*does not exist/.test(msg)) {
    reasoning.push(`msg_pattern→SCHEMA_FAILURE`);
    return { category: 'SCHEMA_FAILURE', subtype: 'message_pattern', confidence: 0.7 };
  }
  if (/storage|bucket|object/.test(msg)) {
    reasoning.push(`msg_pattern→STORAGE_FAILURE`);
    return { category: 'STORAGE_FAILURE', subtype: 'message_pattern', confidence: 0.65 };
  }
  if (/supabase_unavailable|client not available/.test(msg)) {
    reasoning.push(`msg_pattern→DATABASE_FAILURE`);
    return { category: 'DATABASE_FAILURE', subtype: 'client_unavailable', confidence: 0.85 };
  }

  // 6. Last resort
  reasoning.push('no_signal→UNKNOWN');
  return { category: 'UNKNOWN', subtype: 'unclassified', confidence: 0.3, reasoning };
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 RETRYABILITY ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §3: retryable / conditionally-retryable / non-retryable.
// Returns both the boolean flag and a confidence score.

const NON_RETRYABLE_CATEGORIES = new Set([
  'AUTHENTICATION',   // refresh needed, not retry
  'PERMISSION',       // RLS drift, not retry
  'SCHEMA_FAILURE',   // migration needed, not retry
  'STORAGE_FAILURE',  // needs investigation
]);

// Some subtypes within retryable categories are permanently non-retryable.
// unique_constraint_violation means the data exists — retry won't help.
// foreign_key_violation means the parent row is missing — retry won't fix.
const NON_RETRYABLE_SUBTYPES = new Set([
  'unique_constraint_violation',
  'foreign_key_violation',
]);

const CONDITIONALLY_RETRYABLE = {
  RATE_LIMIT: (r) => r.retryAfterMs != null,    // retry only if Retry-After set
  TIMEOUT: (r) => !r.timeout.architecturalPressure,  // skip if architectural
  RESOURCE_EXHAUSTION: () => true,                // always retry (with backoff)
  CONSISTENCY_FAILURE: () => true,
  DEPENDENCY_FAILURE: () => true,
};

function _analyzeRetryability(classified, rateLimit, timeout) {
  const { category, confidence } = classified;

  // Explicitly non-retryable
  if (NON_RETRYABLE_CATEGORIES.has(category)) {
    return { retryable: false, reason: `non_retryable_category:${category}` };
  }

  // Non-retryable subtypes within retryable categories (e.g. CONFLICT/unique_constraint_violation)
  if (classified.subtype && NON_RETRYABLE_SUBTYPES.has(classified.subtype)) {
    return { retryable: false, reason: `non_retryable_subtype:${classified.subtype}` };
  }

  // Conditionally retryable
  if (CONDITIONALLY_RETRYABLE[category]) {
    if (category === 'RATE_LIMIT' && !rateLimit.retryAfterMs) {
      return { retryable: false, reason: 'rate_limit_no_retry_after' };
    }
    if (category === 'TIMEOUT' && timeout.architecturalPressure) {
      return { retryable: false, reason: 'architectural_pressure' };
    }
    return { retryable: true, reason: `conditional_retry:${category}` };
  }

  // Default: retryable for transient categories, non-retryable for UNKNOWN
  if (category === 'UNKNOWN') {
    return { retryable: false, reason: 'unknown_not_retryable_fail_loud' };
  }

  // All other categories (NETWORK, CONFLICT, CONNECTION_FAILURE, DATABASE_FAILURE)
  // are retryable with appropriate backoff
  return { retryable: true, reason: `retryable_category:${category}` };
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 IDEMPOTENCY RISK ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §4: every operation classified by replay safety.
//   read           → safe
//   update         → duplicate_state_possible (idempotent on same WHERE)
//   upsert (key)   → duplicate_state_possible
//   insert (no key)→ duplicate_write_possible
//   rpc            → unsafe (caller must verify)
//   delete         → safe
//   storage.upload → unsafe
//   storage.download → safe

const IDEMPOTENCY_TABLE = {
  read:          { required: false, risk: 'safe' },
  write:         { required: true,  risk: 'duplicate_write_possible' },  // generic write defaults to insert-style
  upsert:        { required: true,  risk: 'duplicate_state_possible' },
  insert:        { required: true,  risk: 'duplicate_write_possible' },
  update:        { required: true,  risk: 'duplicate_state_possible' },
  delete:        { required: false, risk: 'safe' },
  rpc:           { required: true,  risk: 'unsafe' },
  'storage.upload':   { required: true,  risk: 'unsafe' },
  'storage.download': { required: false, risk: 'safe' },
  'storage.delete':   { required: false, risk: 'safe' },
  'auth.refresh':     { required: false, risk: 'safe' },
};

function _analyzeIdempotency(operation, classified) {
  // Look up by explicit operation type, then fall back to the generic 'write'/'read'
  const explicit = IDEMPOTENCY_TABLE[operation];
  if (explicit) return explicit;

  // Reads never have idempotency risk
  if (operation === 'read' || operation === 'select') {
    return { required: false, risk: 'safe' };
  }

  // Writes default to insert-style (highest risk)
  return { required: true, risk: 'duplicate_write_possible' };
}

function _generateIdempotencyKey(lineageId, table, primaryKeyField, primaryKeyValue) {
  if (!lineageId || !table || !primaryKeyField || primaryKeyValue == null) {
    return null;
  }
  const input = `${lineageId}|${table}|${primaryKeyField}|${primaryKeyValue}`;
  return crypto.createHash('sha256').update(input).digest('hex').slice(0, 32);
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 BACKOFF POLICY GENERATION
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §5: substrate owns the schedule, FSM applies it.
// Per Q4: substrate generates per-attempt delay, policy.js caps it.
// The substrate returns baseMs, multiplier, jitterMs. The FSM combines
// with policy.maxDelayMs (structural cap) and emits the actual timer.

const BACKOFF_PROFILES = {
  NETWORK:            { baseMs: 1000, multiplier: 2,  jitterMs: 500 },
  CONNECTION_FAILURE: { baseMs: 5000, multiplier: 2,  jitterMs: 1000 },
  RATE_LIMIT:         { baseMs: 30000, multiplier: 1, jitterMs: 0 },  // rate-limit uses retryAfter
  TIMEOUT:            { baseMs: 5000, multiplier: 2,  jitterMs: 1000 },
  RESOURCE_EXHAUSTION:{ baseMs: 60000, multiplier: 1.5, jitterMs: 5000 },
  CONSISTENCY_FAILURE:{ baseMs: 10000, multiplier: 2, jitterMs: 2000 },
  DEPENDENCY_FAILURE: { baseMs: 15000, multiplier: 2, jitterMs: 3000 },
  DATABASE_FAILURE:   { baseMs: 30000, multiplier: 2, jitterMs: 5000 },
  CONFLICT:           { baseMs: 5000, multiplier: 2,  jitterMs: 1000 },
  STORAGE_FAILURE:    { baseMs: 10000, multiplier: 2, jitterMs: 2000 },
  PERMISSION:         { baseMs: 0, multiplier: 0, jitterMs: 0 },   // no backoff for permission
  AUTHENTICATION:     { baseMs: 0, multiplier: 0, jitterMs: 0 },   // no backoff for auth
  SCHEMA_FAILURE:     { baseMs: 0, multiplier: 0, jitterMs: 0 },   // no backoff for schema
  UNKNOWN:            { baseMs: 0, multiplier: 0, jitterMs: 0 },   // fail loud
};

function _generateBackoff(classified, rateLimit, attemptN) {
  const profile = BACKOFF_PROFILES[classified.category] || BACKOFF_PROFILES.UNKNOWN;
  const n = Math.max(1, attemptN);

  // Rate-limit: retryAfter overrides everything
  if (classified.category === 'RATE_LIMIT' && rateLimit.retryAfterMs != null) {
    return {
      baseMs: rateLimit.retryAfterMs,
      multiplier: 1,
      jitterMs: 0,
      computedMs: rateLimit.retryAfterMs,
      cappedMs: rateLimit.retryAfterMs,
    };
  }

  // Non-retryable categories: no backoff
  if (profile.baseMs === 0) {
    return { baseMs: 0, multiplier: 0, jitterMs: 0, computedMs: 0, cappedMs: 0 };
  }

  // Exponential: base * (multiplier ^ (n-1))
  const computed = profile.baseMs * Math.pow(profile.multiplier, n - 1);
  const jitter = profile.jitterMs > 0
    ? Math.floor(Math.random() * profile.jitterMs)
    : 0;
  return {
    baseMs: profile.baseMs,
    multiplier: profile.multiplier,
    jitterMs: profile.jitterMs,
    computedMs: computed,
    cappedMs: computed,  // policy.maxDelayMs cap is applied by the FSM, not here
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 RATE-LIMIT INTELLIGENCE
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §6: inspect responses for rate-limit signals.
//   - HTTP 429 → rate-limited
//   - Retry-After header (seconds or HTTP-date) → retryAfterMs
//   - Persistent rate-limit (3+ in window) → recommendedThrottle

function _analyzeRateLimit(normalized, classified) {
  const isRateLimited = classified.category === 'RATE_LIMIT';
  let retryAfterMs = null;
  let recommendedThrottle = false;

  if (isRateLimited) {
    // Try to read Retry-After from the normalized details (vendor header
    // already extracted in §1)
    if (normalized.details && typeof normalized.details === 'object') {
      const ra = normalized.details.retryAfter
        || normalized.details['retry-after']
        || normalized.details.RetryAfter;
      if (ra != null) {
        if (typeof ra === 'number') {
          retryAfterMs = ra * 1000;  // seconds → ms
        } else if (typeof ra === 'string') {
          // HTTP-date
          const ms = Date.parse(ra) - Date.now();
          if (!isNaN(ms) && ms > 0) retryAfterMs = ms;
          else {
            const n = parseInt(ra, 10);
            if (!isNaN(n)) retryAfterMs = n * 1000;
          }
        }
      }
    }
    // Default if rate-limited but no retryAfter set: 30s
    if (retryAfterMs == null) retryAfterMs = 30000;
  }

  return {
    isRateLimited,
    retryAfterMs,
    recommendedThrottle,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 RESOURCE EXHAUSTION ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §7: connection pool, Supavisor, concurrency, slowdown.

function _analyzeResourceExhaustion(normalized, classified) {
  let detected = false;
  let signal = null;
  let recommendedConcurrencyDelta = 0;

  if (classified.category === 'RESOURCE_EXHAUSTION') {
    detected = true;
    signal = classified.subtype;

    // PG 53300 (too_many_connections) or Supavisor pressure
    if (normalized.pgCode === '53300' || /supavisor|too many connections|connection pool/i.test(normalized.message || '')) {
      signal = 'connection_pool';
      recommendedConcurrencyDelta = -2;
    }
    // PG 53100 (disk full)
    else if (normalized.pgCode === '53100') {
      signal = 'disk_full';
      recommendedConcurrencyDelta = -3;
    }
    // PG 53200 (out of memory)
    else if (normalized.pgCode === '53200') {
      signal = 'out_of_memory';
      recommendedConcurrencyDelta = -3;
    }
    // PG 53400 (configuration_limit_exceeded)
    else if (normalized.pgCode === '53400') {
      signal = 'configuration_limit';
      recommendedConcurrencyDelta = -1;
    }
    // Slowdown pattern
    else if (/slow|overload|exhaust/i.test(normalized.message || '')) {
      signal = 'slowdown';
      recommendedConcurrencyDelta = -1;
    }
  }

  return {
    detected,
    signal,
    recommendedConcurrencyDelta,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 TIMEOUT INTELLIGENCE
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §8: differentiate query, lock, transaction, api, idle timeouts.

function _analyzeTimeout(normalized, classified) {
  if (classified.category !== 'TIMEOUT') {
    return { kind: null, architecturalPressure: false };
  }

  let kind = classified.subtype;

  // PG 57014 is query_canceled (most often timeout-related)
  if (normalized.pgCode === '57014') {
    if (/lock/i.test(normalized.message || '')) kind = 'lock';
    else if (/transaction/i.test(normalized.message || '')) kind = 'transaction';
    else kind = 'query';
  }
  // HTTP 408 is request timeout (api)
  if (normalized.httpStatus === 408) kind = 'api';
  // HTTP 504 is gateway timeout
  if (normalized.httpStatus === 504) kind = 'api';

  // Architectural pressure indicators:
  //   - long executionMs
  //   - "lock timeout" with high contention
  //   - repeated timeouts (handled at the FSM level via attemptN)
  const executionMs = normalized.executionMs || 0;
  const architecturalPressure = (
    executionMs > 30000 ||  // slow query threshold
    kind === 'lock' ||
    kind === 'transaction'
  );

  return { kind, architecturalPressure };
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 SEVERITY ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §9: LOW / MEDIUM / HIGH / CRITICAL with a numeric score.

const SEVERITY_TABLE = {
  // LOW
  NETWORK:                  { severity: 'LOW',     score: 25 },
  RATE_LIMIT:               { severity: 'LOW',     score: 25 },
  CONNECTION_FAILURE:       { severity: 'LOW',     score: 30 },
  // MEDIUM
  TIMEOUT:                  { severity: 'MEDIUM',  score: 50 },
  RESOURCE_EXHAUSTION:      { severity: 'MEDIUM',  score: 60 },
  CONFLICT:                 { severity: 'MEDIUM',  score: 55 },
  SCHEMA_FAILURE:           { severity: 'MEDIUM',  score: 65 },
  AUTHENTICATION:           { severity: 'MEDIUM',  score: 55 },
  PERMISSION:               { severity: 'MEDIUM',  score: 55 },
  // HIGH
  CONSISTENCY_FAILURE:      { severity: 'HIGH',    score: 75 },
  STORAGE_FAILURE:          { severity: 'HIGH',    score: 75 },
  DEPENDENCY_FAILURE:       { severity: 'HIGH',    score: 80 },
  // CRITICAL
  DATABASE_FAILURE:         { severity: 'CRITICAL', score: 95 },
  UNKNOWN:                  { severity: 'HIGH',    score: 70 },  // fail-loud
};

function _analyzeSeverity(classified) {
  const entry = SEVERITY_TABLE[classified.category] || SEVERITY_TABLE.UNKNOWN;
  return { severity: entry.severity, severityScore: entry.score };
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 RECOVERY RECOMMENDATION GENERATION
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §10: substrate generates recommendations; FSM authorizes.

function _generateRecommendations(classified, retryability, rateLimit, resourceExhaustion, timeout, severity) {
  const recs = [];

  // Auth → REFRESH
  if (classified.category === 'AUTHENTICATION') {
    recs.push('REFRESH_AUTHENTICATION');
    return recs;
  }

  // Permission → RECONCILE
  if (classified.category === 'PERMISSION') {
    recs.push('RECONCILE_STATE');
    return recs;
  }

  // Schema → REPAIR + ESCALATE
  if (classified.category === 'SCHEMA_FAILURE') {
    recs.push('REPAIR_SCHEMA', 'ESCALATE_TO_OPERATOR');
    return recs;
  }

  // Storage → REBUILD
  if (classified.category === 'STORAGE_FAILURE') {
    recs.push('REBUILD_CACHE');
    return recs;
  }

  // Database failure (PostgREST dead) → ESCALATE + DEFER
  if (classified.category === 'DATABASE_FAILURE') {
    recs.push('ESCALATE_TO_OPERATOR', 'DEFER_EXECUTION');
    return recs;
  }

  // Resource exhaustion → THROTTLE + RECONCILE
  if (resourceExhaustion.detected) {
    recs.push('THROTTLE_WORKLOAD', 'RECONCILE_STATE');
    if (severity.severity === 'CRITICAL' || severity.severity === 'HIGH') {
      recs.push('ESCALATE_TO_OPERATOR');
    }
    return recs;
  }

  // Rate-limit with retry-after → RETRY + THROTTLE
  if (rateLimit.isRateLimited && rateLimit.retryAfterMs != null) {
    recs.push('RETRY_OPERATION', 'THROTTLE_WORKLOAD');
    if (rateLimit.recommendedThrottle) {
      recs.push('DEFER_EXECUTION');
    }
    return recs;
  }

  // Conflict (unique constraint) → RECONCILE
  if (classified.category === 'CONFLICT' && classified.subtype === 'unique_constraint_violation') {
    recs.push('RECONCILE_STATE');
    return recs;
  }

  // Other conflicts → RETRY (transient)
  if (classified.category === 'CONFLICT') {
    if (retryability.retryable) recs.push('RETRY_OPERATION');
    recs.push('RECONCILE_STATE');
    return recs;
  }

  // Timeout with architectural pressure → ESCALATE + RECONCILE
  if (timeout.architecturalPressure) {
    recs.push('ESCALATE_TO_OPERATOR', 'RECONCILE_STATE');
    return recs;
  }

  // Consistency failure → RECONCILE + ESCALATE
  if (classified.category === 'CONSISTENCY_FAILURE') {
    recs.push('RECONCILE_STATE', 'ESCALATE_TO_OPERATOR');
    return recs;
  }

  // Dependency failure → DEFER + ESCALATE
  if (classified.category === 'DEPENDENCY_FAILURE') {
    recs.push('DEFER_EXECUTION', 'ESCALATE_TO_OPERATOR');
    return recs;
  }

  // CRITICAL severity → ESCALATE regardless
  if (severity.severity === 'CRITICAL') {
    recs.push('ESCALATE_TO_OPERATOR', 'DEFER_EXECUTION');
    return recs;
  }

  // HIGH severity → at least RECONCILE
  if (severity.severity === 'HIGH') {
    recs.push('RECONCILE_STATE');
    return recs;
  }

  // Retryable transient → RETRY
  if (retryability.retryable) {
    recs.push('RETRY_OPERATION');
    return recs;
  }

  // Default for non-retryable unknowns → ESCALATE
  if (classified.category === 'UNKNOWN') {
    recs.push('ESCALATE_TO_OPERATOR');
    return recs;
  }

  // Fallback: no recommendation
  return recs;
}

// ═══════════════════════════════════════════════════════════════════════════
// §11 TELEMETRY GENERATION
// ═══════════════════════════════════════════════════════════════════════════
// Per spec §11: every analysis carries a telemetry package.

function _buildTelemetry(analysis) {
  return {
    failureId: analysis.failureId,
    timestamp: analysis.timestamp,
    source: analysis.source,
    operation: analysis.operation,
    category: analysis.category,
    subtype: analysis.subtype,
    retryable: analysis.retryable,
    confidence: analysis.confidence,
    retryCount: analysis.attemptN,
    severity: analysis.severity,
    severityScore: analysis.severityScore,
    recommendation: analysis.recommendations.join(',') || null,
    correlationIds: {
      lineageId: analysis.lineageId,
      lineageDomain: analysis.lineageDomain,
      workerName: analysis.workerName,
    },
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// §12 FINAL CANONICAL OUTPUT — analyzeFailure
// ═══════════════════════════════════════════════════════════════════════════
// Composes §1-§11. Single entry point for the canonical failure analysis.

function analyzeFailure(rawError, operation, source = 'supabase', context = {}) {
  const {
    attemptN = 1,
    lineageId = null,
    lineageDomain = 'persist-telemetry',
    workerName = null,
    primaryKeyField = null,
    primaryKeyValue = null,
  } = context;

  // §1 Normalize
  const normalized = _normalize(rawError, operation, source);

  // §2 Classify
  const classified = _classify(normalized, operation);

  // §6 Rate-limit (needed for §3 and §5)
  const rateLimit = _analyzeRateLimit(normalized, classified);

  // §7 Resource exhaustion
  const resourceExhaustion = _analyzeResourceExhaustion(normalized, classified);

  // §8 Timeout
  const timeout = _analyzeTimeout(normalized, classified);

  // §3 Retryability
  const retryability = _analyzeRetryability(classified, rateLimit, timeout);

  // §4 Idempotency
  const idempotency = _analyzeIdempotency(operation, classified);
  const idempotencyKey = idempotency.required
    ? _generateIdempotencyKey(lineageId, 'tbd', primaryKeyField, primaryKeyValue)
    : null;

  // §5 Backoff
  const backoff = _generateBackoff(classified, rateLimit, attemptN);

  // §9 Severity
  const severity = _analyzeSeverity(classified);

  // §10 Recommendations
  const recommendations = _generateRecommendations(
    classified, retryability, rateLimit, resourceExhaustion, timeout, severity
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

    normalized,
    category: classified.category,
    subtype: classified.subtype,
    retryable: retryability.retryable,
    retryabilityReason: retryability.reason,
    confidence: classified.confidence,
    reasoning: classified.reasoning,

    idempotencyRequired: idempotency.required,
    idempotencyRisk: idempotency.risk,
    idempotencyKey,

    backoff,

    rateLimit,
    resourceExhaustion,
    timeout,
    severity: severity.severity,
    severityScore: severity.severityScore,

    recommendations,
  };

  // §11 Telemetry
  analysis.telemetry = _buildTelemetry(analysis);

  return analysis;
}

// ═══════════════════════════════════════════════════════════════════════════
// BACKWARDS COMPAT — legacy reportFailure wrapper
// ═══════════════════════════════════════════════════════════════════════════
// Returns the slim shape the existing 10 writers use. Will be removed
// in step 2 when writers migrate to analyzeFailure.

function reportFailure(rawError, operation) {
  const a = analyzeFailure(rawError, operation, 'supabase', { attemptN: 1 });
  return {
    category: a.category,
    subtype: a.subtype,
    retryable: a.retryable,
    retryAfterMs: a.rateLimit.retryAfterMs,
    httpStatus: a.normalized.httpStatus,
    raw: a.normalized,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
  analyzeFailure,
  reportFailure,  // legacy
  // Exposed for testing and finer-grained reuse
  _normalize,
  _classify,
  _analyzeRetryability,
  _analyzeIdempotency,
  _generateIdempotencyKey,
  _generateBackoff,
  _analyzeRateLimit,
  _analyzeResourceExhaustion,
  _analyzeTimeout,
  _analyzeSeverity,
  _generateRecommendations,
  _buildTelemetry,
};
