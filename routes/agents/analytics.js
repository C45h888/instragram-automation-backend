// backend.api/routes/agents/analytics.js
// Analytics/Insights endpoints: /insights, /account-insights, /media-insights
//
// handleInsightsRequest lives here (not in agent-helpers) — it's a route-level
// concern and was previously there only due to a circular dependency workaround.
// That circular dep (agent-helpers ← data-fetchers → agent-helpers) is now gone.

const express = require('express');
const router = express.Router();
const { fetchAndStoreMediaInsights } = require('../../helpers/data-fetchers/media-fetchers');
const { fetchAndStoreAccountInsights } = require('../../helpers/data-fetchers/account-fetchers');
const { categorizeIgError } = require('../../helpers/agent-helpers');
const { logApiRequest } = require('../../config/supabase');

// ============================================
// SHARED HANDLER
// ============================================

async function handleInsightsRequest(req, res, startTime, metricTypeOverride) {
  const { business_account_id, since, until, metric_type } = req.query;

  try {
    if (!business_account_id) {
      return res.status(400).json({
        error: 'Missing required query parameter: business_account_id'
      });
    }

    const type = metricTypeOverride || metric_type || 'account';
    let insightsData = {};

    if (type === 'account') {
      const result = await fetchAndStoreAccountInsights(business_account_id, { since, until });
      if (!result.success) throw new Error(result.error);
      insightsData = result.data;

    } else if (type === 'media') {
      const result = await fetchAndStoreMediaInsights(business_account_id, since, until);
      if (!result.success) throw new Error(result.error);
      insightsData = { media_insights: result.mediaInsights };

    } else {
      return res.status(400).json({
        error: 'Invalid metric_type. Must be "account" or "media"'
      });
    }

    logApiRequest({
      endpoint: req.path,
      method: req.method,
      business_account_id,
      user_id: req.user?.id || null,
      response_time_ms: Date.now() - startTime,
      status_code: 200,
      success: true,
      domain: 'account',
    }).catch(() => {});

    res.json({ success: true, data: insightsData });

  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    console.error('❌ Insights fetch failed:', errorMessage);
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
    const statusCode = error.response?.status || 500;

    logApiRequest({
      endpoint: req.path,
      method: req.method,
      business_account_id,
      response_time_ms: Date.now() - startTime,
      status_code: statusCode,
      success: false,
      error_message: errorMessage,
      domain: 'account',
    }).catch(() => {});

    res.status(statusCode).json({
      error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable,
      error_category,
      retry_after_seconds
    });
  }
}

// ============================================
// ENDPOINT 5: GET /insights (Analytics Reports)
// ============================================

/**
 * Gets account or media insights for analytics reports.
 * Used by: Analytics reports scheduler (scheduler/analytics_reports.py)
 */
router.get('/insights', (req, res) => handleInsightsRequest(req, res, Date.now(), null));

// ============================================
// ENDPOINT 5A: GET /account-insights
// ============================================

/**
 * Account-level insights alias matching agent naming convention.
 * Agent calls: GET /account-insights?business_account_id=X&since=Y&until=Z
 * Used by: analytics_tools.py fetch_account_insights()
 */
router.get('/account-insights', (req, res) => handleInsightsRequest(req, res, Date.now(), 'account'));

// ============================================
// ENDPOINT 5B: GET /media-insights
// ============================================

/**
 * Media-level insights alias matching agent naming convention.
 * Agent calls: GET /media-insights?business_account_id=X&since=Y&until=Z
 * Used by: analytics_tools.py fetch_media_insights()
 */
router.get('/media-insights', (req, res) => handleInsightsRequest(req, res, Date.now(), 'media'));

module.exports = router;
