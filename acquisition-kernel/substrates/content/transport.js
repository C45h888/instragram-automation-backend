// substrates/content/transport.js
// Content substrate transport: business-owned media posts.
//
// Owns: calling Instagram Graph API for business media endpoints.
// Does NOT own: DB writes, normalization, retry logic, orchestration.
//
// Decomposed from substrates/transport/instagram.js (former god module).
// Merged: fetchBusinessPosts + fetchMediaFeed → fetchPosts (single function).

const {
  axios,
  GRAPH_API_BASE,
  resolveCreds,
  clampLimit,
  buildErrorResponse,
  extractUsage,
  logTelemetry,
} = require('../../../substrates/transport/_shared');

// ═══════════════════════════════════════════════════════════════════════════════
// BUSINESS POSTS (merged with media feed)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetch the business account's own media posts.
 * Merged from fetchBusinessPosts + fetchMediaFeed — same Instagram endpoint.
 *
 * @param {string} accountId
 * @param {number} [limit=50]
 * @param {object} [credentials=null] - pre-resolved { igUserId, pageToken, userId }
 * @param {{ since?: number|string, until?: number|string }} [timeWindow] - optional time range
 * @returns {Promise<object>} { success, posts, count, igUserId, pageToken?, _usagePct, ...errorMeta }
 */
async function fetchPosts(accountId, limit = 50, credentials = null, timeWindow = null) {
  const startTime = Date.now();
  const fetchLimit = clampLimit(limit, 50, 100);

  try {
    const { igUserId, pageToken, userId } = await resolveCreds(accountId, credentials);

    const params = {
      fields: 'id,media_type,caption,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count',
      limit: fetchLimit,
      access_token: pageToken,
    };

    if (timeWindow) {
      if (timeWindow.since) params.since = typeof timeWindow.since === 'number'
        ? timeWindow.since : Math.floor(new Date(timeWindow.since).getTime() / 1000);
      if (timeWindow.until) params.until = typeof timeWindow.until === 'number'
        ? timeWindow.until : Math.floor(new Date(timeWindow.until).getTime() / 1000);
    }

    const res = await axios.get(`${GRAPH_API_BASE}/${igUserId}/media`, {
      params,
      timeout: 15000,
    });

    const posts = res.data.data || [];

    await logTelemetry('content', '/posts', accountId, userId, true, Date.now() - startTime);

    return {
      success: true, posts, count: posts.length,
      igUserId, pageToken,
      _usagePct: extractUsage(res.headers),
    };
  } catch (error) {
    await logTelemetry('content', '/posts', accountId, null, false, Date.now() - startTime, {
      status_code: error.response?.status || null,
      error: error.response?.data?.error?.message || error.message,
      meta: buildErrorResponse(error),
    });
    return { success: false, posts: [], count: 0, ...buildErrorResponse(error) };
  }
}

module.exports = { fetchPosts };
