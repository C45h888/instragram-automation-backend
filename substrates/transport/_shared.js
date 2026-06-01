// substrates/transport/_shared.js
// Shared transport utilities — credential resolution, error handling, limit clamping.
//
// Extracted from the former substrates/transport/instagram.js (616-line god module).
// All substrate-specific transport files import from here to avoid boilerplate duplication.

const axios = require('axios');
const { resolveAccountCredentials, categorizeIgError, GRAPH_API_BASE } = require('../../helpers/agent-helpers');
const { logWithDomain } = require('../telemetry');
const { parseUsageHeader } = require('../quota');

/**
 * Resolve credentials, preferring pre-resolved over DB lookup.
 */
async function resolveCreds(accountId, credentials) {
  if (credentials) return credentials;
  return resolveAccountCredentials(accountId);
}

/**
 * Clamp a limit value between defaults.
 */
function clampLimit(value, defaultVal, maxVal) {
  return Math.min(parseInt(value) || defaultVal, maxVal);
}

/**
 * Build standard error response from axios error.
 */
function buildErrorResponse(error) {
  const errorMessage = error.response?.data?.error?.message || error.message;
  const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
  return {
    success: false,
    error: errorMessage,
    code: error.response?.data?.error?.code || null,
    retryable,
    error_category,
    retry_after_seconds,
  };
}

/**
 * Extract usage percentage from response headers.
 */
function extractUsage(headers) {
  return parseUsageHeader(headers?.['x-business-use-case-usage']);
}

/**
 * Log telemetry for a transport call. Fire-and-forget.
 */
async function logTelemetry(domain, endpoint, accountId, userId, success, latencyMs, details = {}) {
  try {
    await logWithDomain(domain, {
      endpoint, method: 'GET',
      business_account_id: accountId,
      user_id: userId || null,
      success,
      latency: latencyMs,
      status_code: details.status_code || null,
      error: details.error || null,
      details: details.meta || undefined,
    });
  } catch (_) {
    // telemetry is fire-and-forget
  }
}

module.exports = {
  axios,
  GRAPH_API_BASE,
  resolveCreds,
  clampLimit,
  buildErrorResponse,
  extractUsage,
  logTelemetry,
};
