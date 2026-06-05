// substrates/insights/transport.js
// Insights substrate transport: account-level insights and per-media insights.
//
// Owns: calling Instagram Graph API for analytics/insights endpoints.
// Does NOT own: DB writes, normalization, retry logic, orchestration.
//
// Decomposed from substrates/transport/instagram.js (former god module).
// FIXES: removed Supabase query (fetchAccountInsights), removed regex validation.

const {
  axios,
  GRAPH_API_BASE,
  resolveCreds,
  buildErrorResponse,
  extractUsage,
  logTelemetry,
} = require('../../../substrates/transport/_shared');

// ═══════════════════════════════════════════════════════════════════════════════
// ACCOUNT INSIGHTS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetch account-level insights from the Instagram Graph API.
 * Caller provides pre-computed { since, until } timestamps and _hasWebsite flag.
 *
 * @param {string} accountId
 * @param {{ since: number, until: number, hasWebsite?: boolean }} options
 * @param {object} [credentials=null] - pre-resolved { igUserId, pageToken }
 * @returns {Promise<object>} { success, v1Data, v2Data, period, hasWebsite, igUserId, _usagePct, ...errorMeta }
 */
async function fetchAccountInsights(accountId, options = {}, credentials = null) {
  const { since, until, hasWebsite = false } = options;

  const V1_METRICS = ['reach'];
  const V2_METRICS_BASE = ['accounts_engaged', 'profile_views'];
  const V2_METRICS_WEBSITE = ['website_clicks'];

  try {
    const { igUserId, pageToken } = await resolveCreds(accountId, credentials);

    const v2Metrics = [...V2_METRICS_BASE];
    if (hasWebsite) v2Metrics.push(...V2_METRICS_WEBSITE);

    const [v1Response, v2Response] = await Promise.all([
      axios.get(`${GRAPH_API_BASE}/${igUserId}/insights`, {
        params: { metric: V1_METRICS.join(','), period: 'day', since, until, access_token: pageToken },
        timeout: 15000,
      }),
      axios.get(`${GRAPH_API_BASE}/${igUserId}/insights`, {
        params: { metric: v2Metrics.join(','), period: 'day', metric_type: 'total_value', since, until, access_token: pageToken },
        timeout: 15000,
      }),
    ]);

    return {
      success: true,
      v1Data: v1Response.data.data || [],
      v2Data: v2Response.data.data || [],
      period: { since, until, start_date: new Date(since * 1000).toISOString(), end_date: new Date(until * 1000).toISOString() },
      hasWebsite,
      igUserId,
      _usagePct: extractUsage(v1Response.headers),
    };
  } catch (error) {
    return {
      success: false, v1Data: [], v2Data: [], period: {}, hasWebsite: false,
      igUserId: null, ...buildErrorResponse(error),
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA INSIGHTS BATCH
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetch per-media insights (reach, impressions, saved) for a list of media objects.
 * Batches 5 posts in parallel with 500ms delay between batches for rate-limit safety.
 *
 * @param {Array} mediaList - media objects with .id and .media_type
 * @param {string} pageToken
 * @returns {Promise<Array>} mediaInsights — one entry per media item
 */
async function fetchMediaInsightsBatch(mediaList, pageToken) {
  const BATCH_SIZE = 5;
  const BATCH_DELAY_MS = 500;

  const fetchOne = async (media) => {
    try {
      const isStory = media.media_type === 'STORY';
      const metricParam = isStory ? 'reach,impressions' : 'reach,impressions,saved';

      const res = await axios.get(`${GRAPH_API_BASE}/${media.id}/insights`, {
        params: { metric: metricParam, access_token: pageToken },
      });

      return {
        media_id: media.id,
        media_type: media.media_type,
        timestamp: media.timestamp,
        caption: media.caption || null,
        media_url: media.media_url || null,
        thumbnail_url: media.thumbnail_url || null,
        permalink: media.permalink || null,
        like_count: media.like_count || 0,
        comments_count: media.comments_count || 0,
        insights: res.data.data || [],
      };
    } catch (err) {
      // Individual fetch failures return partial data, don't fail the batch
      return {
        media_id: media.id,
        media_type: media.media_type,
        timestamp: media.timestamp,
        caption: media.caption || null,
        media_url: media.media_url || null,
        thumbnail_url: media.thumbnail_url || null,
        permalink: media.permalink || null,
        like_count: media.like_count || 0,
        comments_count: media.comments_count || 0,
        insights: [],
        error: err.message,
      };
    }
  };

  const results = [];
  for (let i = 0; i < mediaList.length; i += BATCH_SIZE) {
    const batch = mediaList.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(fetchOne));
    results.push(...batchResults);
    if (i + BATCH_SIZE < mediaList.length) {
      await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));
    }
  }
  return results;
}

module.exports = { fetchAccountInsights, fetchMediaInsightsBatch };
