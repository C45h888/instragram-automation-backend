// backend.api/helpers/data-fetchers/account-fetchers.js
// Domain: account — account-level insights.
// Fetches from Instagram Graph API via the instagram-tokens service.
// No req/res dependencies — callable from routes and proactive-sync cron.
//
// All api_usage rows written with domain='account' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'account' AND success = false ORDER BY created_at DESC

const {
  axios,
  getSupabaseAdmin,
  resolveAccountCredentials,
  categorizeIgError,
  logWithDomain,
  GRAPH_API_BASE,
} = require('./base');

// Metric arrays for account-level insights — defined inline to keep this file self-contained
const V1_METRICS = ['reach'];
const V2_METRICS_BASE = ['accounts_engaged', 'profile_views'];
const V2_METRICS_WEBSITE = ['website_clicks'];

// ============================================
// ACCOUNT INSIGHTS
// ============================================

/**
 * Fetches account-level insights.
 * Thin wrapper around getAccountInsights() from instagram-tokens.js.
 *
 * @param {string} businessAccountId - UUID
 * @param {Object} [options] - {since, until, period}
 * @returns {Promise<{success: boolean, data: Object, error?: string}>}
 */
async function fetchAndStoreAccountInsights(businessAccountId, options = {}) {
  const startTime = Date.now();

  try {
    // DB-aware: check if account has a website URL configured before requesting website_clicks.
    // website_clicks is a v2 total_value metric that Meta only returns for accounts with websites.
    const supabase = getSupabaseAdmin();
    const { data: accountRow } = await supabase
      .from('instagram_business_accounts')
      .select('website')
      .eq('id', businessAccountId)
      .single();

    const hasWebsite = !!accountRow?.website;

    const { igUserId, pageToken } = await resolveAccountCredentials(businessAccountId);

    // ── Account insights: inlined from getAccountInsights (instagram-tokens) ──────
    const { period = '7d', until: untilParam } = options;
    const periodMatch = period.match(/^(\d+)d$/);
    if (!periodMatch) throw new Error(`Invalid period format: ${period}. Use format: '7d', '30d', '90d'`);

    const periodDays = parseInt(periodMatch[1]);
    if (periodDays < 1 || periodDays > 90) throw new Error(`Period must be between 1 and 90 days. Got: ${periodDays}`);

    const until = untilParam || Math.floor(Date.now() / 1000);
    const since = until - (periodDays * 24 * 60 * 60);
    const v2Metrics = [...V2_METRICS_BASE, ...(hasWebsite ? V2_METRICS_WEBSITE : [])];

    const v1Response = await axios.get(`${GRAPH_API_BASE}/${igUserId}/insights`, {
      params: { metric: V1_METRICS.join(','), period: 'day', since, until, access_token: pageToken },
      timeout: 15000
    });
    if (v1Response.data.error) throw new Error(`Instagram API Error (v1): ${v1Response.data.error.message}`);

    const v2Response = await axios.get(`${GRAPH_API_BASE}/${igUserId}/insights`, {
      params: { metric: v2Metrics.join(','), period: 'day', metric_type: 'total_value', since, until, access_token: pageToken },
      timeout: 15000
    });
    if (v2Response.data.error) throw new Error(`Instagram API Error (v2): ${v2Response.data.error.message}`);

    const accountInsights = {
      success: true,
      data: {
        time_series: v1Response.data.data || [],
        totals: v2Response.data.data || []
      },
      period: {
        since, until, days: periodDays,
        start_date: new Date(since * 1000).toISOString(),
        end_date: new Date(until * 1000).toISOString()
      },
      hasWebsite
    };
    // ── End inlined getAccountInsights ────────────────────────────────────────────

    const latency = Date.now() - startTime;

    await logWithDomain('account', {
      endpoint: '/account-insights',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: igUserId,
      success: true,
      latency
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
      retryable, error_category, retry_after_seconds
    };
  }
}

module.exports = {
  fetchAndStoreAccountInsights,
};
