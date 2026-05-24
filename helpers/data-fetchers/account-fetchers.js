// helpers/data-fetchers/account-fetchers.js
// Domain: account — account-level insights.
// Transport layer lives in substrates/transport/instagram.js (fetchAccountInsights).
// This file is a thin wrapper: transport → normalize → persistence call.
//
// All api_usage rows written with domain='account' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'account' AND success = false ORDER BY created_at DESC

const {
  resolveAccountCredentials,
  categorizeIgError,
  logWithDomain,
} = require('./base');
const { fetchAccountInsights } = require('../../substrates/transport/instagram');

// ============================================
// ACCOUNT INSIGHTS
// ============================================

/**
 * Fetches account-level insights.
 * Thin wrapper: calls transport (fetchAccountInsights), returns normalized result.
 * No DB write — caller handles persistence if needed.
 *
 * @param {string} businessAccountId - UUID
 * @param {Object} [options] - {since, until, period}
 * @returns {Promise<{success: boolean, data: Object, error?: string}>}
 */
async function fetchAndStoreAccountInsights(businessAccountId, options = {}) {
  const startTime = Date.now();

  try {
    // Pass options to transport; fetchAccountInsights resolves its own credentials
    const transportResult = await fetchAccountInsights(businessAccountId, options);

    if (!transportResult.success) {
      return { success: false, data: {}, error: transportResult.error };
    }

    const accountInsights = {
      success: true,
      data: {
        time_series: transportResult.v1Data || [],
        totals: transportResult.v2Data || [],
      },
      period: transportResult.period,
      hasWebsite: transportResult.hasWebsite,
    };

    const latency = Date.now() - startTime;
    await logWithDomain('account', {
      endpoint: '/account-insights',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: transportResult.igUserId,
      success: true,
      latency,
    });

    return { success: true, data: accountInsights };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('account', {
      endpoint: '/account-insights',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, data: {}, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

module.exports = {
  fetchAndStoreAccountInsights,
};
