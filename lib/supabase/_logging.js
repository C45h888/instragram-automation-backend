// backend.api/lib/supabase/_logging.js
/**
 * Structured logging to Supabase: audit_log and api_usage tables.
 * Extracted from config/supabase.js logAudit() + logApiRequest().
 */

const { getSupabaseAdmin } = require('./_client');

// ── LOG LEVEL ─────────────────────────────────────────────────────────────

const _LOG_LEVELS = { trace: 0, debug: 1, standard: 2, minimal: 3 };
const _CURRENT_LEVEL = _LOG_LEVELS[process.env.LOG_LEVEL] ?? _LOG_LEVELS.standard;

function shouldLog(level) {
  return (_LOG_LEVELS[level] ?? _LOG_LEVELS.standard) >= _CURRENT_LEVEL;
}

// ── logAudit ─────────────────────────────────────────────────────────────────

/**
 * Write a row to audit_log.
 * Supports two call signatures for backward compat:
 *   logAudit({ event_type, action, resource_type, ... })   ← object form (preferred)
 *   logAudit(eventType, userId, eventData, req)           ← positional form (legacy)
 */
async function logAudit(eventTypeOrObj, userId, eventData, req) {
  try {
    let eventType_v, userId_v, eventData_v, req_v;

    if (eventTypeOrObj !== null && typeof eventTypeOrObj === 'object' && !Array.isArray(eventTypeOrObj)) {
      // Object form (used by agent-proxy.js)
      eventType_v = eventTypeOrObj.event_type;
      userId_v    = eventTypeOrObj.user_id || null;
      eventData_v = {
        action:        eventTypeOrObj.action || 'unknown',
        resource_type: eventTypeOrObj.resource_type,
        resource_id:   eventTypeOrObj.resource_id,
        details:       eventTypeOrObj.details || {},
        success:       eventTypeOrObj.success !== false,
      };
      req_v = null;
    } else {
      // Positional form (used by server.js, supabaseHelpers, instagram-tokens.js)
      eventType_v = eventTypeOrObj;
      userId_v     = userId;
      eventData_v  = eventData || {};
      req_v        = req;
    }

    const admin = getSupabaseAdmin();
    if (!admin) {
      console.warn('⚠️  Cannot log audit - database not connected');
      return;
    }

    await admin.from('audit_log').insert({
      user_id:       userId_v,
      event_type:    eventType_v,
      action:        eventData_v.action || 'unknown',
      resource_type: eventData_v.resource_type,
      resource_id:   eventData_v.resource_id,
      details:       eventData_v.details,
      ip_address:    req_v?.ip || req_v?.connection?.remoteAddress || null,
      user_agent:    req_v?.headers?.['user-agent'] || 'unknown',
      success:       eventData_v.success !== false,
      created_at:    new Date().toISOString(),
    });
  } catch (error) {
    console.error('Audit log error:', error);
  }
}

// ── logApiRequest ────────────────────────────────────────────────────────────

/**
 * Write a row to api_usage via the log_api_request RPC.
 * Supports two call signatures:
 *   logApiRequest({ endpoint, method, latency, success, ... })    ← object form (preferred)
 *   logApiRequest(userId, endpoint, method, responseTime, statusCode, success, businessAccountId)  ← positional
 *
 * Retry: up to 3 attempts with exponential back-off for unique-constraint violations (23505).
 * Non-retryable errors fail fast.
 */
async function logApiRequest(userIdOrObj, endpoint, method, responseTime, statusCode, success, businessAccountId) {
  try {
    let userId_v, endpoint_v, method_v, responseTime_v, statusCode_v, success_v, businessAccountId_v;
    let errorMessage_v = null;
    let domain_v = null;

    if (userIdOrObj !== null && typeof userIdOrObj === 'object' && !Array.isArray(userIdOrObj)) {
      // Object form (used by agent-proxy.js)
      userId_v            = userIdOrObj.user_id || null;
      endpoint_v          = userIdOrObj.endpoint;
      method_v            = userIdOrObj.method;
      responseTime_v      = userIdOrObj.latency || userIdOrObj.response_time || 0;
      statusCode_v        = userIdOrObj.status_code || (userIdOrObj.success ? 200 : 500);
      success_v           = userIdOrObj.success !== undefined ? userIdOrObj.success : true;
      businessAccountId_v = userIdOrObj.business_account_id || null;
      errorMessage_v      = userIdOrObj.error || null;
      domain_v            = userIdOrObj.domain || null;
    } else {
      // Positional form (used by server.js middleware)
      userId_v            = userIdOrObj;
      endpoint_v          = endpoint;
      method_v            = method;
      responseTime_v      = responseTime;
      statusCode_v        = statusCode;
      success_v           = success;
      businessAccountId_v = businessAccountId;
    }

    const admin = getSupabaseAdmin();
    if (!admin) {
      console.warn('⚠️  Cannot log API request - database not connected');
      return;
    }

    const _now        = new Date();
    const _hourBucket = new Date(_now);
    _hourBucket.setMinutes(0, 0, 0);

    const MAX_RETRIES   = 3;
    const BASE_DELAY_MS = 100;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      const { error } = await admin.rpc('log_api_request', {
        p_user_id:             userId_v,
        p_business_account_id: businessAccountId_v,
        p_endpoint:            endpoint_v,
        p_method:              method_v,
        p_response_time_ms:    responseTime_v,
        p_status_code:         statusCode_v,
        p_success:             success_v,
        p_error_message:      errorMessage_v,
        p_domain:              domain_v,
        p_hour_bucket:         _hourBucket.toISOString(),
      });

      if (!error) return;

      const isConstraintViolation = error?.code === '23505';
      if (!isConstraintViolation) {
        console.error(`[logApiRequest] Non-retryable error (${error?.code}): ${error?.message}`);
        return;
      }

      if (attempt === MAX_RETRIES) {
        console.error(
          `[logApiRequest] All ${MAX_RETRIES} retries exhausted for ` +
          `${endpoint_v} ${method_v} (hour_bucket=${_hourBucket.toISOString()}). ` +
          `Last error: ${error.message}`
        );
        return;
      }

      const delay = BASE_DELAY_MS * Math.pow(2, attempt - 1);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  } catch (error) {
    console.error(`[logApiRequest] Unexpected exception: ${error.message}`);
  }
}

module.exports = { logAudit, logApiRequest, shouldLog };
