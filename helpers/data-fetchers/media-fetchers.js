// helpers/data-fetchers/media-fetchers.js
// Domain: media — business post feed, per-post insights.
// Transport layer lives in substrates/transport/instagram.js.
// This file is a thin wrapper: transport → normalize → persistence call.
// No req/res dependencies — callable from routes and proactive-sync cron.
//
// All api_usage rows written with domain='media' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'media' AND success = false ORDER BY created_at DESC

const {
  resolveAccountCredentials,
  categorizeIgError,
  logWithDomain,
} = require('./base');
const { fetchBusinessPosts } = require('../../substrates/transport/instagram');
const { storeBusinessPosts } = require('../../substrates/persistence');

// ============================================
// MEDIA INSIGHTS — THIN FETCH: FEED
// ============================================

/**
 * Fetches the business account's media feed list.
 * Delegates to transport.fetchMediaFeed — signature preserved for backward compat.
 * NO DB write — returns the raw media list + credentials for the next pipeline step.
 *
 * @param {string} businessAccountId - UUID
 * @param {string|number} [since] - ISO date string or unix timestamp
 * @param {string|number} [until] - ISO date string or unix timestamp
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<object>}
 */
function fetchMediaFeed(businessAccountId, since, until, credentials) {
  return require('../../substrates/transport/instagram').fetchMediaFeed(businessAccountId, since, until, credentials);
}

// ============================================
// MEDIA INSIGHTS — THIN FETCH: PER-MEDIA INSIGHTS
// ============================================

/**
 * Fetches per-media insights (reach, impressions, saved) for a list of media objects.
 * Delegates to transport.fetchMediaInsightsBatch — signature preserved for backward compat.
 * NO DB write. Batches 5 posts in parallel with 500ms delay between batches.
 *
 * @param {Array} mediaList - Media objects (must have .id and .media_type)
 * @param {string} pageToken - Page access token
 * @returns {Promise<Array>} mediaInsights — one entry per media item
 */
function fetchMediaInsightsBatch(mediaList, pageToken) {
  return require('../../substrates/transport/instagram').fetchMediaInsightsBatch(mediaList, pageToken);
}

// ============================================
// MEDIA INSIGHTS — BATCH WRITER
// ============================================

/**
 * Writes media insight records to instagram_media and syncs hashtags.
 * Delegates to substrates/persistence.storeMediaInsightsBatch.
 * Story saves fix: writes NULL (not 0) for STORY rows — NULL means "metric not applicable"
 * whereas 0 would imply the metric was tracked and found to be zero, which is incorrect.
 *
 * @param {string} businessAccountId - UUID
 * @param {Array} mediaInsights - Output of fetchMediaInsightsBatch()
 * @param {Array<string>} captions - Post captions for hashtag auto-sync
 * @returns {Promise<{count: number}>}
 */
function storeMediaInsightsBatch(businessAccountId, mediaInsights, captions) {
  return require('../../substrates/persistence').storeMediaInsightsBatch(businessAccountId, mediaInsights, captions);
}

// ============================================
// MEDIA INSIGHTS — SHIM (route + sync backward-compat)
// ============================================

/**
 * Fetches media insights and persists to instagram_media.
 * Shim wiring fetchMediaFeed → fetchMediaInsightsBatch → storeMediaInsightsBatch.
 * Both callers (sync/insights.js and routes/agents/analytics.js) keep using this unchanged.
 *
 * @param {string} businessAccountId - UUID
 * @param {string|number} [since] - ISO date string or unix timestamp
 * @param {string|number} [until] - ISO date string or unix timestamp
 * @returns {Promise<{success: boolean, mediaInsights: Array, count: number, error?: string}>}
 */
async function fetchAndStoreMediaInsights(businessAccountId, since, until) {
  const startTime = Date.now();

  try {
    const credentials = await resolveAccountCredentials(businessAccountId);
    const feedResult = await fetchMediaFeed(businessAccountId, since, until, credentials);

    if (!feedResult.success) {
      return {
        success: false, mediaInsights: [], count: 0,
        error: feedResult.error,
        code: feedResult.code,
        retryable: feedResult.retryable,
        error_category: feedResult.error_category,
        retry_after_seconds: feedResult.retry_after_seconds,
      };
    }

    const mediaInsights = await fetchMediaInsightsBatch(feedResult.mediaList, credentials.pageToken);
    const captions = feedResult.mediaList.map(m => m.caption).filter(Boolean);
    await storeMediaInsightsBatch(businessAccountId, mediaInsights, captions);

    const latency = Date.now() - startTime;
    await logWithDomain('media', {
      endpoint: '/media-insights',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: credentials.igUserId,
      success: true,
      latency,
    });

    return {
      success: true,
      mediaInsights,
      count: mediaInsights.length,
      _usagePct: feedResult._usagePct,
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('media', {
      endpoint: '/media-insights',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, mediaInsights: [], count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ============================================
// BUSINESS POSTS
// ============================================

/**
 * Fetches the business account's own media feed and upserts full post data to instagram_media.
 * This is the proactive sync that populates the table read by GET /media/:accountId.
 * Thin wrapper: transport.fetchBusinessPosts → persistence.storeBusinessPosts.
 *
 * @param {string} businessAccountId - UUID
 * @param {number} [limit=50] - Max posts to fetch
 * @returns {Promise<{success: boolean, count: number, error?: string}>}
 */
async function fetchAndStoreBusinessPosts(businessAccountId, limit = 50) {
  const startTime = Date.now();

  try {
    const transportResult = await fetchBusinessPosts(businessAccountId, limit);

    if (!transportResult.success) {
      return { success: false, count: 0, error: transportResult.error };
    }

    if (transportResult.posts.length > 0) {
      await storeBusinessPosts(businessAccountId, transportResult.posts);
    }

    return {
      success: true, count: transportResult.count,
      _usagePct: transportResult._usagePct,
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('media', {
      endpoint: '/sync/posts',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

module.exports = {
  fetchMediaFeed,
  fetchMediaInsightsBatch,
  storeMediaInsightsBatch,
  fetchAndStoreMediaInsights,
  fetchAndStoreBusinessPosts,
};
